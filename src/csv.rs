//! Input files: memory-mapped, cut into chunks on line ownership, and split
//! into fields by the dialect in rules.md "CSV dialect".

use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::Path;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}
const MADV_DONTNEED: i32 = 4;
const PROT_READ: i32 = 1;
const MAP_PRIVATE: i32 = 2;

/// A mapping of an immutable input file. Borrowed slices cannot outlive it.
pub struct Mapping {
    ptr: *mut u8,
    len: usize,
}

impl Mapping {
    pub fn open(path: &Path) -> Self {
        let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let len = file.metadata().unwrap().len() as usize;
        if len == 0 {
            return Self { ptr: std::ptr::null_mut(), len };
        }
        let ptr = unsafe { mmap(std::ptr::null_mut(), len, PROT_READ, MAP_PRIVATE, file.as_raw_fd(), 0) };
        assert!(ptr as isize != -1, "mmap {}", path.display());
        Self { ptr, len }
    }

    pub fn bytes(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            // The mapping remains readable until self is dropped.
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        if self.len != 0 {
            unsafe { munmap(self.ptr, self.len); }
        }
    }
}

/// Drops the page-table entries of the whole pages inside `data[start..end]`
/// once the chunk is parsed. The unmapping at the end then has almost nothing
/// to tear down, and this part runs on all threads at once. `data` must be a
/// whole mapping; a page read again later is simply faulted in again.
pub fn release(data: &[u8], start: usize, end: usize) {
    let (start, end) = ((start + 4095) & !4095, end & !4095);
    if end > start {
        // Safety: a read-only private file mapping, so dropping its pages
        // only costs a later fault; the range lies inside the mapping.
        unsafe { madvise(data.as_ptr().add(start) as *mut u8, end - start, MADV_DONTNEED) };
    }
}

/// Every `*.csv` directly in `dir`, sorted.
pub fn list(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out: Vec<_> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    out.sort();
    out
}

/// Byte ranges of `data` of about `size` bytes. A line belongs to the range
/// holding its first byte; `lines` skips the header and so needs the offset.
pub fn chunks(len: usize, size: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut s = 0;
    while s < len {
        let e = (s + size).min(len);
        out.push((s, e));
        s = e;
    }
    out
}

/// Calls `f` with every line that starts in `data[start..end]`, without its
/// `\n`, skipping the file's first line (the header).
#[inline]
pub fn lines(data: &[u8], start: usize, end: usize, mut f: impl FnMut(&[u8])) {
    let mut at = if start == 0 {
        // Skip the header.
        match memchr(b'\n', data, 0) {
            Some(nl) => nl + 1,
            None => return,
        }
    } else {
        match memchr(b'\n', data, start - 1) {
            Some(nl) => nl + 1,
            None => return,
        }
    };
    while at < end {
        let stop = memchr(b'\n', data, at).unwrap_or(data.len());
        f(&data[at..stop]);
        at = stop + 1;
    }
}

#[inline]
fn memchr(needle: u8, data: &[u8], from: usize) -> Option<usize> {
    data[from..].iter().position(|&b| b == needle).map(|p| p + from)
}

/// Splits a line into exactly N fields, unquoting whole-field quotes. A field
/// is None when empty. Returns false for a malformed line.
#[inline]
pub fn fields<'a, const N: usize>(line: &'a [u8], out: &mut [Option<&'a [u8]>; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut n = 0;
    let mut start = 0;
    let mut quotes = false;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if n + 1 >= N {
                return false;
            }
            match field(&line[start..i], quotes) {
                Some(v) => out[n] = v,
                None => return false,
            }
            n += 1;
            start = i + 1;
            quotes = false;
        } else if b == b'"' {
            quotes = true;
        }
    }
    if n + 1 != N {
        return false;
    }
    match field(&line[start..], quotes) {
        Some(v) => out[n] = v,
        None => return false,
    }
    true
}

/// Some(value) for a well-formed field, None for a malformed one.
#[inline]
fn field(f: &[u8], quotes: bool) -> Option<Option<&[u8]>> {
    let v = if quotes {
        if f.len() < 2 || f[0] != b'"' || f[f.len() - 1] != b'"' || f[1..f.len() - 1].contains(&b'"') {
            return None;
        }
        &f[1..f.len() - 1]
    } else {
        f
    };
    Some(if v.is_empty() { None } else { Some(v) })
}

/// Comma, newline and quote positions in one 64-byte block.
#[inline]
fn block(data: &[u8], at: usize) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx2")]
    if at + 64 <= data.len() {
        use std::arch::x86_64::*;
        // Both loads are within the mapping and AVX2 is enabled.
        unsafe {
            let p = data.as_ptr().add(at).cast::<__m256i>();
            let (a, b) = (_mm256_loadu_si256(p), _mm256_loadu_si256(p.add(1)));
            let mask = |c: u8| {
                let c = _mm256_set1_epi8(c as i8);
                let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, c)) as u32 as u64;
                let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, c)) as u32 as u64;
                lo | (hi << 32)
            };
            return (mask(b','), mask(b'\n'), mask(b'"'));
        }
    }
    let (mut comma, mut newline, mut quote) = (0u64, 0u64, 0u64);
    for (i, &c) in data[at..(at + 64).min(data.len())].iter().enumerate() {
        comma |= ((c == b',') as u64) << i;
        newline |= ((c == b'\n') as u64) << i;
        quote |= ((c == b'"') as u64) << i;
    }
    (comma, newline, quote)
}

pub struct Scanner;

/// Quote flags in field order. A quoted field needs a quote at both ends;
/// each quote must occupy exactly one of those boundary positions.
#[cfg(target_feature = "bmi2")]
#[inline(always)]
fn field_quotes<const N: usize>(quotes: u128, start: u128, end: u128) -> Option<u32> {
    use std::arch::x86_64::_pext_u64;
    let extract = |bits: u128, mask: u128| unsafe {
        let first = _pext_u64(bits as u64, mask as u64);
        if N <= 4 {
            first
        } else {
            let second = _pext_u64((bits >> 64) as u64, (mask >> 64) as u64);
            first | (second << (mask as u64).count_ones())
        }
    };
    let opening = extract(quotes, start);
    let closing = extract(quotes << 1, end);
    let double_used = quotes & start & (end >> 1);
    if opening != closing || double_used != 0 || quotes.count_ones() != 2 * opening.count_ones() {
        None
    } else {
        Some(opening as u32)
    }
}

impl Scanner {
    /// Visits the well-formed rows whose first byte is in the chunk.
    #[inline]
    pub fn scan<'a, const N: usize>(
        &mut self,
        data: &'a [u8],
        start: usize,
        end: usize,
        mut f: impl FnMut(&[Option<&'a [u8]>; N]),
    ) {
        let Some(nl) = memchr(b'\n', data, start.saturating_sub(1)) else { return };
        let mut at = nl + 1;
        if at >= end {
            return;
        }
        let first = at & !63;
        let initial_shift = at - first;
        let mut carried_commas = 0u128;
        let mut carried_quotes = 0u32;
        let mut out = [None; N];
        for block_at in (first..data.len()).step_by(64) {
            let (commas, mut newlines, quotes) = block(data, block_at);
            let mut shift = if block_at == first { initial_shift } else { 0 };
            newlines &= !0u64 << shift;
            while newlines != 0 {
                let stop = newlines.trailing_zeros() as usize;
                let row_end = block_at + stop;
                let row_len = row_end - at;
                let ok = if row_len < 128 {
                    let lower = (1u64 << stop) - 1;
                    // shift can only be 64 after the last newline in the block.
                    let row_commas = ((commas & lower) >> shift) as u128;
                    let all_commas = carried_commas | (row_commas << block_at.saturating_sub(at));
                    let row_quotes = carried_quotes + ((quotes & lower) >> shift).count_ones();
                    // all_commas contains only positions in this row before its newline.
                    unsafe { fields_from_masks(&data[at..=row_end], all_commas, row_quotes, &mut out) }
                } else {
                    fields(&data[at..row_end], &mut out)
                };
                if ok {
                    f(&out);
                }
                shift = stop + 1;
                at = row_end + 1;
                if at >= end {
                    return;
                }
                carried_commas = 0;
                carried_quotes = 0;
                newlines &= newlines - 1;
            }
            let offset = block_at.saturating_sub(at);
            if shift < 64 && offset < 128 {
                carried_commas |= ((commas >> shift) as u128) << offset;
                carried_quotes += (quotes >> shift).count_ones();
            }
        }
        // A final line need not have a newline.
        if at < end && fields(&data[at..], &mut out) {
            f(&out);
        }
    }
}

#[inline]
// Safety: line ends in a newline and every set comma bit is before it.
unsafe fn fields_from_masks<'a, const N: usize>(
    line: &'a [u8],
    mut commas: u128,
    quotes: u32,
    out: &mut [Option<&'a [u8]>; N],
) -> bool {
    let length = line.len() - 1;
    if length == 0 || commas.count_ones() as usize != N - 1 {
        return false;
    }
    let mut start = 0;
    let mut paired_quotes = 0;
    for (i, dst) in out.iter_mut().enumerate() {
        let end = if i == N - 1 { length } else { commas.trailing_zeros() as usize };
        commas &= commas.wrapping_sub(1);
        let size = end - start;
        // Matching commas come from the bytes before this newline and are
        // visited in ascending order. Even an empty final field can read
        // its first byte here: line includes the trailing newline.
        let (quoted, value) = unsafe {
            let ptr = line.as_ptr();
            let left = *ptr.add(start);
            let right = *ptr.add(end.saturating_sub(1));
            let quoted = (size >= 2) & (left == b'"') & (right == b'"');
            let trim = quoted as usize;
            let bytes = std::slice::from_raw_parts(ptr.add(start + trim), size - 2 * trim);
            (quoted, bytes)
        };
        paired_quotes += 2 * quoted as u32;
        *dst = (!value.is_empty()).then_some(value);
        start = end + 1;
    }
    // Every pair accounts for two distinct quote bytes. Any other quote
    // is outside an allowed field boundary.
    paired_quotes == quotes
}

/// Classify two vectors for compact records and a third for wider records.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn row_masks<const N: usize>(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p.cast());
    let b = _mm256_loadu_si256(p.add(32).cast());
    let c = if N > 4 { _mm256_loadu_si256(p.add(64).cast()) } else { _mm256_setzero_si256() };
    let mask = |byte| {
        let byte = _mm256_set1_epi8(byte);
        let bits = |x| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, byte)) as u32 as u128;
        bits(a) | (bits(b) << 32) | (if N > 4 { bits(c) } else { 0 } << 64)
    };
    (mask(b',' as i8), mask(b'\n' as i8), mask(b'"' as i8))
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn row_masks<const N: usize>(p: *const u8) -> (u128, u128, u128) {
    let (mut c, mut n, mut q) = (0u128, 0u128, 0u128);
    for i in 0..if N > 4 { 96 } else { 64 } {
        let b = *p.add(i);
        c |= ((b == b',') as u128) << i;
        n |= ((b == b'\n') as u128) << i;
        q |= ((b == b'"') as u128) << i;
    }
    (c, n, q)
}

/// Visits the well-formed rows whose first byte is in `data[start..end]`,
/// header skipped, as N fields; an empty field is an empty slice (null).
/// Each row is classified from 64 or 96 bytes at its start. A longer row,
/// or one starting too close to the end of `data`, goes through the
/// scalar `fields`, which defines the dialect.
#[inline(always)]
pub fn rows<'a, const N: usize>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
    let Some(nl) = memchr(b'\n', data, start.saturating_sub(1)) else { return };
    let mut at = nl + 1;
    let mut out: [&'a [u8]; N] = [&[]; N];
    let width = if N > 4 { 96 } else { 64 };
    let fast_end = data.len().saturating_sub(width);
    while at < end {
        if at <= fast_end {
            // Safety: at is positive and at + width <= data.len().
            let (commas, newlines, quotes) = unsafe { row_masks::<N>(data.as_ptr().add(at)) };
            if newlines != 0 {
                let len = newlines.trailing_zeros() as usize;
                let below = (1u128 << len) - 1;
                let mut c = commas & below;
                if len != 0 && c.count_ones() as usize == N - 1 {
                    let row = &data[at..at + len + 1];
                    let mut s = 0usize;
                    #[cfg(target_feature = "bmi2")]
                    {
                        if let Some(q) = field_quotes::<N>(quotes & below, (c << 1) | 1, c | (1 << len)) {
                            for (i, dst) in out.iter_mut().enumerate() {
                                let e = if i == N - 1 { len } else { c.trailing_zeros() as usize };
                                c &= c.wrapping_sub(1);
                                let trim = ((q >> i) & 1) as usize;
                                // The quote mask allowed only distinct, paired endpoints.
                                *dst = unsafe { row.get_unchecked(s + trim..e - trim) };
                                s = e + 1;
                            }
                            f(&out);
                        }
                    }
                    #[cfg(not(target_feature = "bmi2"))]
                    {
                        let mut paired = 0u32;
                        for (i, dst) in out.iter_mut().enumerate() {
                            let e = if i == N - 1 { len } else { c.trailing_zeros() as usize };
                            c &= c.wrapping_sub(1);
                            let size = e - s;
                            // Safety: s <= e <= len < row.len(), so both reads and
                            // the slice are inside the row.
                            let (left, right) = unsafe { (*row.get_unchecked(s), *row.get_unchecked(e.saturating_sub(1))) };
                            let q = (size >= 2) & (left == b'"') & (right == b'"');
                            let t = q as usize;
                            *dst = unsafe { row.get_unchecked(s + t..e - t) };
                            paired += 2 * q as u32;
                            s = e + 1;
                        }
                        // Every quote must be one of the pairs around a whole field.
                        if paired == (quotes & below).count_ones() {
                            f(&out);
                        }
                    }
                }
                at += len + 1;
                continue;
            }
        }
        let stop = memchr(b'\n', data, at).unwrap_or(data.len());
        let mut opt = [None; N];
        if fields(&data[at..stop], &mut opt) {
            for (dst, v) in out.iter_mut().zip(opt) {
                *dst = v.unwrap_or(&[]);
            }
            f(&out);
        }
        at = stop + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_boundaries_match_scalar_for_empty_and_malformed_fields() {
        let values = [b"".as_slice(), b"a", b"\"\"", b"\"a\"", b"\"", b"\"a", b"a\"", b"a\"b", b"\"\"\""];
        for &first in &values {
            for &second in &values {
                for &third in &values {
                    let mut data = b"header\n".to_vec();
                    let row = [first, second, third].join(&b',');
                    data.extend_from_slice(&row);
                    data.push(b'\n');
                    data.resize(data.len() + 96, b'x');
                    let end = b"header\n".len() + row.len() + 1;
                    let mut expected = [None; 3];
                    let valid = fields(&row, &mut expected);
                    let mut seen = false;
                    rows::<3>(&data, 0, end, |actual| {
                        assert_eq!(*actual, expected.map(|f| f.unwrap_or(&[])));
                        seen = true;
                    });
                    assert_eq!(seen, valid);
                }
            }
        }
    }

    /// `rows` agrees with `lines` + `fields` on random input and chunkings.
    #[test]
    fn rows_match_reference() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        const SHORT: &[u8] = b"ab,,,\"\n\n\r x";
        const LONG: &[u8] = b"abcdefghijklmnopqrstuvwxyz,,\"\n";
        const ROWS: &[u8] = b"abcdefghij,,,,,,\"\n";
        for round in 0..6000 {
            let len = (next() % 900) as usize;
            let alphabet = [SHORT, LONG, ROWS][round % 3];
            let data: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            let size = 1 + (next() % 300) as usize;
            let (mut want, mut got) = (Vec::new(), Vec::new());
            let (mut want7, mut got7) = (Vec::new(), Vec::new());
            for (s, e) in chunks(data.len(), size) {
                lines(&data, s, e, |line| {
                    let mut f = [None; 3];
                    if fields::<3>(line, &mut f) {
                        want.push(f.map(|v| v.map_or(Vec::new(), <[u8]>::to_vec)));
                    }
                    let mut f = [None; 7];
                    if fields::<7>(line, &mut f) {
                        want7.push(f.map(|v| v.map_or(Vec::new(), <[u8]>::to_vec)));
                    }
                });
                rows::<3>(&data, s, e, |f| got.push(f.map(<[u8]>::to_vec)));
                rows::<7>(&data, s, e, |f| got7.push(f.map(<[u8]>::to_vec)));
            }
            assert_eq!(want, got, "round {round}: {:?}", String::from_utf8_lossy(&data));
            assert_eq!(want7, got7, "round {round}: {:?}", String::from_utf8_lossy(&data));
        }
    }

    fn split(line: &[u8]) -> Option<Vec<Option<String>>> {
        let mut out = [None; 3];
        fields::<3>(line, &mut out)
            .then(|| out.iter().map(|f| f.map(|v| String::from_utf8_lossy(v).into_owned())).collect())
    }

    #[test]
    fn dialect() {
        let s = |v: &str| Some(v.to_string());
        assert_eq!(split(b"a,\"b\",c"), Some(vec![s("a"), s("b"), s("c")]));
        assert_eq!(split(b"a,\"\",c"), Some(vec![s("a"), None, s("c")]));
        assert_eq!(split(b"a,,c\r"), Some(vec![s("a"), None, s("c\r")]));
        assert_eq!(split(b" a,\" b\",c "), Some(vec![s(" a"), s(" b"), s("c ")]));
        assert_eq!(split(b"a,b"), None);
        assert_eq!(split(b"a,b,c,d"), None);
        assert_eq!(split(b""), None);
        assert_eq!(split(b"a,b\",c"), None);
        assert_eq!(split(b"\"a,b,c"), None);
        assert_eq!(split(b"a,\"b\"\",c"), None);
        assert_eq!(split(b"a,\",c"), None);
    }

    /// The block scanner agrees with `lines` + `fields` on random input.
    #[test]
    fn scanner_matches_reference() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        const SHORT: &[u8] = b"ab,,,\"\n\n\r x";
        const LONG: &[u8] = b"abcdefghijklmnopqrstuvwxyz,,\"\n";
        let mut scanner = Scanner;
        for round in 0..3000 {
            let len = (next() % 600) as usize;
            let alphabet = if round % 2 == 0 { SHORT } else { LONG };
            let data: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            let size = 1 + (next() % 130) as usize;
            let (mut want, mut got) = (Vec::new(), Vec::new());
            for (s, e) in chunks(data.len(), size) {
                lines(&data, s, e, |line| {
                    let mut f = [None; 3];
                    if fields::<3>(line, &mut f) {
                        want.push(f.map(|v| v.map(<[u8]>::to_vec)));
                    }
                });
                scanner.scan::<3>(&data, s, e, |f| got.push(f.map(|v| v.map(<[u8]>::to_vec))));
            }
            assert_eq!(want, got, "round {round}: {:?}", String::from_utf8_lossy(&data));
        }
    }

    #[test]
    fn short_and_long_rows_across_block_boundaries() {
        let short = "x".repeat(58);
        let long = "y".repeat(130);
        let data = format!("a,b,c\n,\"{short}\",\n{long},\"\",z\n\",x,y\n,final,\"");
        let expected = [
            [None, Some(short.as_bytes()), None],
            [Some(long.as_bytes()), None, Some(&b"z"[..])],
        ];
        let mut scanner = Scanner;
        for size in [1, 63, 64, 65, 127, data.len()] {
            let mut got = Vec::new();
            for (a, b) in chunks(data.len(), size) {
                scanner.scan::<3>(data.as_bytes(), a, b, |row| got.push(*row));
            }
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn ownership() {
        let data = b"h1,h2\nl1\n\nl3\nl4";
        for size in 1..data.len() + 2 {
            let mut seen = Vec::new();
            for (s, e) in chunks(data.len(), size) {
                lines(data, s, e, |l| seen.push(l.to_vec()));
            }
            assert_eq!(seen, vec![b"l1".to_vec(), b"".to_vec(), b"l3".to_vec(), b"l4".to_vec()], "{size}");
        }
    }
}
