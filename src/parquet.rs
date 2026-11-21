//! A minimal Parquet writer: one row group, one uncompressed PLAIN data page
//! (v1) per column, Thrift compact metadata. Enough for the two output tables.

use std::io::Write;

pub enum Values<'a> {
    I64(&'a [i64]),
    U32(&'a [u32]),
    /// Pre-encoded PLAIN strings (u32 length followed by bytes per value).
    PlainStr { rows: usize, bytes: &'a [u8] },
    /// Parquet DATE: days since the epoch.
    Date(&'a [i32]),
    Str(&'a [&'a [u8]]),
    OptStr(&'a [Option<&'a [u8]>]),
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
const CONVERTED_UTF8: i32 = 0;
const CONVERTED_DATE: i32 = 6;
const CONVERTED_U32: i32 = 13;

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
    offset: i64,
    size: i64,
    count: i64,
}

/// Writes the columns, all of the same length, as one Parquet file.
pub fn write(path: &std::path::Path, columns: &[Column]) -> std::io::Result<()> {
    let rows = match &columns[0].values {
        Values::I64(v) => v.len(),
        Values::U32(v) => v.len(),
        Values::PlainStr { rows, .. } => *rows,
        Values::Date(v) => v.len(),
        Values::Str(v) => v.len(),
        Values::OptStr(v) => v.len(),
    };
    let mut file = std::fs::File::create(path)?;
    file.write_all(b"PAR1")?;
    let mut offset = 4i64;
    let mut metas = Vec::new();
    let mut page = Vec::new();
    for column in columns {
        page.clear();
        let body = match &column.values {
            Values::I64(v) => unsafe { plain_numbers(v) },
            Values::U32(v) => unsafe { plain_numbers(v) },
            Values::Date(v) => unsafe { plain_numbers(v) },
            Values::PlainStr { bytes, .. } => *bytes,
            Values::Str(v) => {
                for s in v.iter() {
                    page.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    page.extend_from_slice(s);
                }
                &page
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
                page.extend_from_slice(&(levels.buf.len() as u32).to_le_bytes());
                page.extend_from_slice(&levels.buf);
                for s in v.iter().flatten() {
                    page.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    page.extend_from_slice(s);
                }
                &page
            }
        };
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, body.len() as i32);
        h.i32(3, body.len() as i32);
        h.begin_struct(5);
        h.i32(1, rows as i32);
        h.i32(2, PLAIN);
        h.i32(3, RLE);
        h.i32(4, RLE);
        h.end_struct();
        h.buf.push(0);
        file.write_all(&h.buf)?;
        file.write_all(body)?;
        let size = (h.buf.len() + body.len()) as i64;
        metas.push(ChunkMeta { offset, size, count: rows as i64 });
        offset += size;
    }

    let physical = |v: &Values| match v {
        Values::I64(_) => INT64,
        Values::Date(_) | Values::U32(_) => INT32,
        _ => BYTE_ARRAY,
    };
    let mut f = Thrift::new();
    f.i32(1, 1);
    f.list(2, 12, columns.len() + 1);
    f.begin_element();
    f.binary(4, b"schema");
    f.i32(5, columns.len() as i32);
    f.end_struct();
    for column in columns {
        f.begin_element();
        f.i32(1, physical(&column.values));
        f.i32(3, if matches!(column.values, Values::OptStr(_)) { OPTIONAL } else { REQUIRED });
        f.binary(4, column.name.as_bytes());
        match column.values {
            Values::Str(_) | Values::OptStr(_) | Values::PlainStr { .. } => {
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
        f.i32(1, physical(&column.values));
        f.list(2, 5, 2);
        f.zigzag(PLAIN as i64);
        f.zigzag(RLE as i64);
        f.list(3, 8, 1);
        f.varint(column.name.len() as u64);
        f.buf.extend_from_slice(column.name.as_bytes());
        f.i32(4, 0); // UNCOMPRESSED
        f.i64(5, meta.count);
        f.i64(6, meta.size);
        f.i64(7, meta.size);
        f.i64(9, meta.offset);
        f.end_struct();
        f.end_struct();
    }
    f.i64(2, metas.iter().map(|m| m.size).sum());
    f.i64(3, rows as i64);
    f.end_struct();
    f.binary(6, b"shopping-sessions");
    f.buf.push(0);

    f.buf.extend_from_slice(&(f.buf.len() as u32).to_le_bytes());
    f.buf.extend_from_slice(b"PAR1");
    file.write_all(&f.buf)
}

/// The scorer target is little-endian. PLAIN integers are stored as their
/// native bytes. The caller must pass i32, u32 or i64, which have no padding.
#[cfg(target_endian = "little")]
unsafe fn plain_numbers<T: Copy>(values: &[T]) -> &[u8] {
    // Callers pass slices of i32, u32 or i64, with no padding.
    unsafe { std::slice::from_raw_parts(values.as_ptr().cast(), std::mem::size_of_val(values)) }
}
