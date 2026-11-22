//! A minimal Parquet writer: one row group, one uncompressed data page (v1)
//! per column, Thrift compact metadata. Enough for the two output tables.
//!
//! Columns are PLAIN unless the caller hands over a dictionary page and the
//! indices it encoded itself (`DictStr`, `OptDictStr`). The whole file goes
//! to the kernel in one vectored write.

use std::io::{IoSlice, Write};

pub enum Values<'a> {
    I64(&'a [i64]),
    U32(&'a [u32]),
    /// Pre-encoded PLAIN strings (u32 length followed by bytes per value).
    PlainStr { rows: usize, bytes: &'a [u8] },
    /// Parquet DATE: days since the epoch.
    Date(&'a [i32]),
    Str(&'a [&'a [u8]]),
    OptStr(&'a [Option<&'a [u8]>]),
    /// A pre-encoded optional string page: `levels` is the 4-byte length and
    /// the definition levels, `values` the PLAIN strings of the present rows.
    OptStrPage { rows: usize, levels: &'a [u8], values: &'a [u8] },
    /// A string column with a dictionary: `dict` holds `entries` PLAIN strings
    /// and `indices` is the data page body, the bit width byte followed by
    /// RLE / bit-packed runs of indices into the dictionary.
    DictStr { rows: usize, entries: usize, dict: &'a [u8], indices: &'a [u8] },
    /// The same with definition levels (`levels` as in `OptStrPage`); only
    /// the present rows have an index.
    OptDictStr { rows: usize, entries: usize, dict: &'a [u8], levels: &'a [u8], indices: &'a [u8] },
}

pub struct Column<'a> {
    pub name: &'static str,
    pub values: Values<'a>,
}

const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const PLAIN: i32 = 0;
const RLE: i32 = 3;
const RLE_DICTIONARY: i32 = 8;
const CONVERTED_UTF8: i32 = 0;
const CONVERTED_DATE: i32 = 6;
const CONVERTED_U32: i32 = 13;

impl Values<'_> {
    fn rows(&self) -> usize {
        match self {
            Values::I64(v) => v.len(),
            Values::U32(v) => v.len(),
            Values::Date(v) => v.len(),
            Values::Str(v) => v.len(),
            Values::OptStr(v) => v.len(),
            Values::PlainStr { rows, .. }
            | Values::OptStrPage { rows, .. }
            | Values::DictStr { rows, .. }
            | Values::OptDictStr { rows, .. } => *rows,
        }
    }

    fn physical(&self) -> i32 {
        match self {
            Values::I64(_) => INT64,
            Values::Date(_) | Values::U32(_) => INT32,
            _ => BYTE_ARRAY,
        }
    }

    fn optional(&self) -> bool {
        matches!(self, Values::OptStr(_) | Values::OptStrPage { .. } | Values::OptDictStr { .. })
    }

    /// Encodings the column chunk uses, for its metadata.
    fn encodings(&self) -> &'static [i32] {
        match self {
            Values::DictStr { .. } | Values::OptDictStr { .. } => &[PLAIN, RLE, RLE_DICTIONARY],
            _ => &[PLAIN, RLE],
        }
    }
}

/// Thrift compact protocol, just the parts the footer needs.
struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn header(&mut self, id: i16, kind: u8) {
        let delta = id - *self.last.last().unwrap();
        if (1..=15).contains(&delta) {
            self.buf.push(((delta as u8) << 4) | kind);
        } else {
            self.buf.push(kind);
            self.zigzag(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.header(id, 5);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.header(id, 6);
        self.zigzag(v);
    }
    fn binary(&mut self, id: i16, v: &[u8]) {
        self.header(id, 8);
        self.varint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn begin_struct(&mut self, id: i16) {
        self.header(id, 12);
        self.last.push(0);
    }
    /// A struct inside a list: no field header.
    fn begin_element(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, kind: u8, len: usize) {
        self.header(id, 9);
        if len < 15 {
            self.buf.push(((len as u8) << 4) | kind);
        } else {
            self.buf.push(0xF0 | kind);
            self.varint(len as u64);
        }
    }
}

struct ChunkMeta {
    /// Where the chunk starts: its dictionary page if it has one.
    offset: i64,
    /// Where its first data page starts.
    data_offset: i64,
    dictionary: bool,
    size: i64,
    count: i64,
}

/// A piece of the file, in order.
#[derive(Clone, Copy)]
enum Part<'a> {
    /// `heads[from..to]`: the magic bytes, page headers and the footer.
    Head(usize, usize),
    /// `owned[i]`: a body built here.
    Owned(usize),
    Body(&'a [u8]),
}

/// The file as a list of pieces: headers and bodies are only gathered by the
/// final vectored write, never copied together.
struct Layout<'a> {
    heads: Vec<u8>,
    owned: Vec<Vec<u8>>,
    parts: Vec<Part<'a>>,
}

impl<'a> Layout<'a> {
    fn new() -> Self {
        let mut layout = Layout { heads: Vec::new(), owned: Vec::new(), parts: Vec::new() };
        layout.head(|h| h.extend_from_slice(b"PAR1"));
        layout
    }

    fn head(&mut self, make: impl FnOnce(&mut Vec<u8>)) {
        let from = self.heads.len();
        make(&mut self.heads);
        self.parts.push(Part::Head(from, self.heads.len()));
    }

    fn own(&mut self, bytes: Vec<u8>) -> Part<'a> {
        self.owned.push(bytes);
        Part::Owned(self.owned.len() - 1)
    }

    /// A page of `size` body bytes made of `body`; returns its size in the file.
    fn page(&mut self, header: impl FnOnce(&mut Vec<u8>, usize), body: &[Part<'a>], size: usize) -> i64 {
        let from = self.heads.len();
        self.head(|h| header(h, size));
        self.parts.extend_from_slice(body);
        (self.heads.len() - from + size) as i64
    }
}

/// A DATA_PAGE header (v1) for `rows` values in `encoding`.
fn data_page_header(out: &mut Vec<u8>, size: usize, rows: usize, encoding: i32) {
    let mut h = Thrift::new();
    h.i32(1, 0); // DATA_PAGE
    h.i32(2, size as i32);
    h.i32(3, size as i32);
    h.begin_struct(5);
    h.i32(1, rows as i32);
    h.i32(2, encoding);
    h.i32(3, RLE);
    h.i32(4, RLE);
    h.end_struct();
    h.buf.push(0);
    out.extend_from_slice(&h.buf);
}

/// A DICTIONARY_PAGE header for `entries` PLAIN values.
fn dictionary_page_header(out: &mut Vec<u8>, size: usize, entries: usize) {
    let mut h = Thrift::new();
    h.i32(1, 2); // DICTIONARY_PAGE
    h.i32(2, size as i32);
    h.i32(3, size as i32);
    h.begin_struct(7);
    h.i32(1, entries as i32);
    h.i32(2, PLAIN);
    h.end_struct();
    h.buf.push(0);
    out.extend_from_slice(&h.buf);
}

/// Writes the columns, all of the same length, as one Parquet file.
pub fn write(path: &std::path::Path, columns: &[Column]) -> std::io::Result<()> {
    let rows = columns[0].values.rows();
    let mut layout = Layout::new();
    let mut offset = 4i64;
    let mut metas = Vec::new();
    for column in columns {
        let start = offset;
        let mut data_offset = offset;
        let mut dictionary = false;
        let plain = |h: &mut Vec<u8>, n: usize| data_page_header(h, n, rows, PLAIN);
        let size = match &column.values {
            Values::I64(v) => {
                let b = unsafe { plain_numbers(v) };
                layout.page(plain, &[Part::Body(b)], b.len())
            }
            Values::U32(v) => {
                let b = unsafe { plain_numbers(v) };
                layout.page(plain, &[Part::Body(b)], b.len())
            }
            Values::Date(v) => {
                let b = unsafe { plain_numbers(v) };
                layout.page(plain, &[Part::Body(b)], b.len())
            }
            Values::PlainStr { bytes, .. } => layout.page(plain, &[Part::Body(bytes)], bytes.len()),
            Values::OptStrPage { levels, values, .. } => {
                layout.page(plain, &[Part::Body(levels), Part::Body(values)], levels.len() + values.len())
            }
            Values::Str(v) => {
                let mut b = Vec::new();
                for s in v.iter() {
                    b.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    b.extend_from_slice(s);
                }
                let n = b.len();
                let body = layout.own(b);
                layout.page(plain, &[body], n)
            }
            Values::OptStr(v) => {
                // Definition levels: RLE/bit-packed hybrid, bit width 1, one
                // bit-packed run, after a 4-byte length.
                let groups = v.len().div_ceil(8);
                let mut levels = Thrift::new();
                levels.varint(((groups as u64) << 1) | 1);
                let mut bits = vec![0u8; groups];
                for (i, s) in v.iter().enumerate() {
                    if s.is_some() {
                        bits[i / 8] |= 1 << (i % 8);
                    }
                }
                levels.buf.extend_from_slice(&bits);
                let mut b = Vec::new();
                b.extend_from_slice(&(levels.buf.len() as u32).to_le_bytes());
                b.extend_from_slice(&levels.buf);
                for s in v.iter().flatten() {
                    b.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    b.extend_from_slice(s);
                }
                let n = b.len();
                let body = layout.own(b);
                layout.page(plain, &[body], n)
            }
            Values::DictStr { entries, dict, indices, .. } => {
                dictionary = true;
                let mut size = layout.page(|h, n| dictionary_page_header(h, n, *entries), &[Part::Body(dict)], dict.len());
                data_offset = offset + size;
                size += layout.page(|h, n| data_page_header(h, n, rows, RLE_DICTIONARY), &[Part::Body(indices)], indices.len());
                size
            }
            Values::OptDictStr { entries, dict, levels, indices, .. } => {
                dictionary = true;
                let mut size = layout.page(|h, n| dictionary_page_header(h, n, *entries), &[Part::Body(dict)], dict.len());
                data_offset = offset + size;
                let body = [Part::Body(levels), Part::Body(indices)];
                size += layout.page(|h, n| data_page_header(h, n, rows, RLE_DICTIONARY), &body, levels.len() + indices.len());
                size
            }
        };
        metas.push(ChunkMeta { offset: start, data_offset, dictionary, size, count: rows as i64 });
        offset += size;
    }

    let mut f = Thrift::new();
    f.i32(1, 1);
    f.list(2, 12, columns.len() + 1);
    f.begin_element();
    f.binary(4, b"schema");
    f.i32(5, columns.len() as i32);
    f.end_struct();
    for column in columns {
        let values = &column.values;
        f.begin_element();
        f.i32(1, values.physical());
        f.i32(3, if values.optional() { OPTIONAL } else { REQUIRED });
        f.binary(4, column.name.as_bytes());
        match values {
            Values::Str(_)
            | Values::OptStr(_)
            | Values::OptStrPage { .. }
            | Values::PlainStr { .. }
            | Values::DictStr { .. }
            | Values::OptDictStr { .. } => {
                f.i32(6, CONVERTED_UTF8);
                f.begin_struct(10);
                f.begin_struct(1); // StringType
                f.end_struct();
                f.end_struct();
            }
            Values::Date(_) => {
                f.i32(6, CONVERTED_DATE);
                f.begin_struct(10);
                f.begin_struct(6); // DateType
                f.end_struct();
                f.end_struct();
            }
            Values::U32(_) => {
                f.i32(6, CONVERTED_U32);
                f.begin_struct(10);
                f.begin_struct(10); // IntType
                f.header(1, 3); // BYTE
                f.buf.push(32);
                f.header(2, 2); // false
                f.end_struct();
                f.end_struct();
            }
            Values::I64(_) => {}
        }
        f.end_struct();
    }
    f.i64(3, rows as i64);
    f.list(4, 12, 1);
    f.begin_element();
    f.list(1, 12, columns.len());
    for (column, meta) in columns.iter().zip(&metas) {
        f.begin_element();
        f.i64(2, meta.offset);
        f.begin_struct(3);
        f.i32(1, column.values.physical());
        let encodings = column.values.encodings();
        f.list(2, 5, encodings.len());
        for &e in encodings {
            f.zigzag(e as i64);
        }
        f.list(3, 8, 1);
        f.varint(column.name.len() as u64);
        f.buf.extend_from_slice(column.name.as_bytes());
        f.i32(4, 0); // UNCOMPRESSED
        f.i64(5, meta.count);
        f.i64(6, meta.size);
        f.i64(7, meta.size);
        f.i64(9, meta.data_offset);
        if meta.dictionary {
            f.i64(11, meta.offset);
        }
        f.end_struct();
        f.end_struct();
    }
    f.i64(2, metas.iter().map(|m| m.size).sum());
    f.i64(3, rows as i64);
    f.end_struct();
    f.binary(6, b"shopping-sessions");
    f.buf.push(0);

    let footer_len = f.buf.len() as u32;
    f.buf.extend_from_slice(&footer_len.to_le_bytes());
    f.buf.extend_from_slice(b"PAR1");
    layout.head(|h| h.extend_from_slice(&f.buf));

    let Layout { heads, owned, parts } = &layout;
    let mut slices: Vec<IoSlice> = parts
        .iter()
        .map(|p| match *p {
            Part::Head(a, b) => IoSlice::new(&heads[a..b]),
            Part::Owned(i) => IoSlice::new(&owned[i]),
            Part::Body(b) => IoSlice::new(b),
        })
        .filter(|s| !s.is_empty())
        .collect();
    let mut file = std::fs::File::create(path)?;
    let mut rest = &mut slices[..];
    while !rest.is_empty() {
        let n = file.write_vectored(rest)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut rest, n);
    }
    Ok(())
}

/// The scorer target is little-endian. PLAIN integers are stored as their
/// native bytes. The caller must pass i32, u32 or i64, which have no padding.
#[cfg(target_endian = "little")]
unsafe fn plain_numbers<T: Copy>(values: &[T]) -> &[u8] {
    // Callers pass slices of i32, u32 or i64, with no padding.
    unsafe { std::slice::from_raw_parts(values.as_ptr().cast(), std::mem::size_of_val(values)) }
}

fn uleb(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// An RLE run of the RLE / bit-packed hybrid: `len` copies of `value`, which
/// takes `value_bytes` bytes.
pub fn rle_run(out: &mut Vec<u8>, len: usize, value: u32, value_bytes: usize) {
    uleb(out, (len as u64) << 1);
    out.extend_from_slice(&value.to_le_bytes()[..value_bytes]);
}

/// One bit-packed run of the RLE / bit-packed hybrid holding all of `values`
/// at `width` bits each (the last group of 8 is padded with zeros). The
/// caller writes the bit width byte before the runs of a dictionary page.
pub fn bit_packed_run(out: &mut Vec<u8>, values: &[u8], width: u32) {
    let groups = values.len().div_ceil(8);
    uleb(out, ((groups as u64) << 1) | 1);
    let mut acc = 0u64;
    let mut bits = 0u32;
    for &v in values {
        acc |= (v as u64) << bits;
        bits += width;
        if bits >= 32 {
            out.extend_from_slice(&(acc as u32).to_le_bytes());
            acc >>= 32;
            bits -= 32;
        }
    }
    // What is left, then zeros up to a whole number of groups.
    let mut need = groups * width as usize - values.len() * width as usize / 32 * 4;
    let here = need.min(8);
    out.extend_from_slice(&acc.to_le_bytes()[..here]);
    need -= here;
    out.resize(out.len() + need, 0);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_uleb(b: &[u8], at: &mut usize) -> u64 {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let byte = b[*at];
            *at += 1;
            v |= ((byte & 127) as u64) << shift;
            if byte < 128 {
                return v;
            }
            shift += 7;
        }
    }

    fn lcg(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    #[test]
    fn bit_packed_runs_hold_the_values() {
        let mut rng = 9u64;
        for width in 1..=8u32 {
            for len in [0usize, 1, 7, 8, 9, 15, 16, 17, 100, 1001] {
                let v: Vec<u8> = (0..len).map(|_| (lcg(&mut rng) % (1 << width)) as u8).collect();
                let mut out = Vec::new();
                bit_packed_run(&mut out, &v, width);
                let mut at = 0;
                let header = read_uleb(&out, &mut at);
                assert_eq!(header & 1, 1);
                let groups = (header >> 1) as usize;
                assert_eq!(groups, len.div_ceil(8));
                assert_eq!(out.len() - at, groups * width as usize, "width {width} len {len}");
                for (i, &want) in v.iter().enumerate() {
                    let mut got = 0u8;
                    for k in 0..width as usize {
                        let pos = i * width as usize + k;
                        got |= ((out[at + pos / 8] >> (pos % 8)) & 1) << k;
                    }
                    assert_eq!(got, want);
                }
            }
        }
    }
}
