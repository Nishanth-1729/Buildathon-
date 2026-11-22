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

/// Comma, newline and quote positions in the 64 bytes at `p`.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p.cast());
    let b = _mm256_loadu_si256(p.add(32).cast());
    let mask = |byte: i8| {
        let byte = _mm256_set1_epi8(byte);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, byte)) as u32 as u64;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, byte)) as u32 as u64;
        lo | hi << 32
    };
    (mask(b',' as i8), mask(b'\n' as i8), mask(b'"' as i8))
}

/// `rows` for up to four fields: a row of at most 63 bytes is classified
/// from the 64 bytes at its start with 64-bit masks (see `quoted_fields` for
/// the quote check, which uses mask arithmetic instead of a test per field).
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
#[allow(unused_assignments)]
fn rows_small<'a, const N: usize>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
    let Some(nl) = memchr(b'\n', data, start.saturating_sub(1)) else { return };
    let mut at = nl + 1;
    let fast_end = data.len().saturating_sub(64);
    while at < end {
        let mut out: [&'a [u8]; N] = [&[]; N];
        let mut next = 0;
        let mut ok = false;
        if at <= fast_end {
            // Safety: at + 64 <= data.len().
            let base = unsafe { data.as_ptr().add(at) };
            let (commas, newlines, quotes) = unsafe { masks64(base) };
            if newlines != 0 {
                let len = newlines.trailing_zeros() as usize;
                let below = (1u64 << len) - 1;
                let c = commas & below;
                next = at + len + 1;
                if len != 0 && c.count_ones() as usize == N - 1 {
                    // Safety: BMI2 is enabled; see `cut`.
                    if let Some(fs) = unsafe { quoted_fields(c, len, quotes) } {
                        unsafe { cut::<N>(base, len, c, fs, &mut out) };
                        ok = true;
                    }
                }
            } else {
                (ok, next, out) = longer_row(data, at);
            }
        } else {
            (ok, next, out) = slow_row(data, at);
        }
        if ok {
            f(&out);
        }
        at = next;
    }
}

/// Newline positions in the 64 bytes at `p`.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
unsafe fn newlines64(p: *const u8) -> u64 {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p.cast());
    let b = _mm256_loadu_si256(p.add(32).cast());
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, nl)) as u32 as u64;
    let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, nl)) as u32 as u64;
    lo | hi << 32
}

/// The quote logic shared by the windows: for the fields of one window,
/// ending at `t` (the position of the terminator), with commas `c` below `t`,
/// `fs` is the bit per field that starts with a quote, or None when the
/// window is not well formed: a field with a quote at one end only, a field
/// that is a lone quote, or a quote anywhere else.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
unsafe fn quoted_fields(c: u64, t: usize, quotes: u64) -> Option<u64> {
    use std::arch::x86_64::_pext_u64;
    let q = quotes & ((1u64 << t) - 1);
    let starts = (c << 1) | 1;
    let ends = c | (1u64 << t);
    let (fs, fe) = (_pext_u64(q & starts, starts), _pext_u64((q << 1) & ends, ends));
    if fs == fe && q & starts & (ends >> 1) == 0 && q.count_ones() == 2 * fs.count_ones() {
        Some(fs)
    } else {
        None
    }
}

/// The first window of an event row: three fields in the 64 bytes at `base`,
/// the last ending at the third comma `t`; returns (t, commas below t, fs).
/// A newline before `t` is not looked for here: `rows7` takes the row only if
/// its first newline comes after `t`.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
unsafe fn window_to_comma(base: *const u8) -> Option<(usize, u64, u64)> {
    let (commas, _, quotes) = masks64(base);
    if commas.count_ones() < 3 {
        return None;
    }
    let rest = commas & commas.wrapping_sub(1);
    let rest = rest & rest.wrapping_sub(1);
    let t = rest.trailing_zeros() as usize;
    let c = commas & ((1u64 << t) - 1);
    Some((t, c, quoted_fields(c, t, quotes)?))
}

/// The second window: `K` fields in the 64 bytes at `base`, the last ending
/// at the newline `t` (< 64, known from a scan of the row); returns (commas
/// below t, fs).
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
unsafe fn window_to_newline<const K: usize>(base: *const u8, t: usize) -> Option<(u64, u64)> {
    let (commas, _, quotes) = masks64(base);
    let c = commas & ((1u64 << t) - 1);
    if c.count_ones() as usize != K - 1 {
        return None;
    }
    Some((c, quoted_fields(c, t, quotes)?))
}

/// The slices of `K` fields of one window: the field `i` runs from the
/// previous comma (or `base`) to the next comma (or `t` for the last), less a
/// quote at either end where `fs` says so.
///
/// Safety: `c` holds the K - 1 commas below `t`, `fs` is from `quoted_fields`
/// for the same window, so every field lies inside the row.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
unsafe fn cut<'a, const K: usize>(base: *const u8, t: usize, mut c: u64, fs: u64, out: &mut [&'a [u8]]) {
    let mut s = 0usize;
    for i in 0..K {
        let e = if i == K - 1 { t } else { c.trailing_zeros() as usize };
        c &= c.wrapping_sub(1);
        let quoted = ((fs >> i) & 1) as usize;
        out[i] = std::slice::from_raw_parts(base.add(s + quoted), e - s - 2 * quoted);
        s = e + 1;
    }
}

/// `rows::<7>`: an event row is cut into two windows of 64 bytes, the first
/// three fields and then the last four from where the third ended.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
#[allow(unused_assignments)]
fn rows7<'a>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; 7])) {
    let Some(nl) = memchr(b'\n', data, start.saturating_sub(1)) else { return };
    let mut at = nl + 1;
    // Both windows end inside data: the second starts at most 64 bytes in.
    let fast_end = data.len().saturating_sub(128);
    while at < end {
        let mut out: [&'a [u8]; 7] = [&[]; 7];
        let mut next = 0;
        let mut ok = false;
        if at <= fast_end {
            // Safety: at + 128 <= data.len(), and the second window starts
            // at most 64 bytes after the first.
            let base = unsafe { data.as_ptr().add(at) };
            // The next row starts after the first newline, found from loads
            // that depend on nothing computed for this row: the scan of the
            // next row need not wait for the two windows of this one.
            let (n_lo, n_hi) = unsafe { (newlines64(base), newlines64(base.add(64))) };
            let nl = if n_lo != 0 { n_lo.trailing_zeros() } else { 64 + n_hi.trailing_zeros() } as usize;
            next = at + nl + 1;
            if let Some((t1, c1, fs1)) = unsafe { window_to_comma(base) } {
                // The row ends at that newline: not before the third comma
                // (then it has too few fields), and less than 64 bytes after
                // it (a longer row takes the scalar path).
                let t2 = nl.wrapping_sub(t1 + 1);
                if t2 < 64 {
                    let base2 = unsafe { base.add(t1 + 1) };
                    if let Some((c2, fs2)) = unsafe { window_to_newline::<4>(base2, t2) } {
                        // Safety: see `cut`.
                        unsafe {
                            cut::<3>(base, t1, c1, fs1, &mut out[..3]);
                            cut::<4>(base2, t2, c2, fs2, &mut out[3..]);
                        }
                        ok = true;
                    }
                }
            }
        }
        if !ok {
            (ok, next, out) = longer_row(data, at);
        }
        // One call site, so the closure is inlined and the fields stay in registers.
        if ok {
            f(&out);
        }
        at = next;
    }
}

/// A row of up to 127 bytes, from 128 bytes at `at` (which must be inside
/// `data`); None when the row is longer, for the scalar path.
#[cfg(target_feature = "avx2")]
#[inline(always)]
fn medium_row<'a, const N: usize>(data: &'a [u8], at: usize) -> Option<(bool, usize, [&'a [u8]; N])> {
    use std::arch::x86_64::*;
    // Safety: the caller ensures at + 128 <= data.len().
    let (commas, newlines, quotes) = unsafe {
        let p = data.as_ptr().add(at);
        let v = [0, 32, 64, 96].map(|o| _mm256_loadu_si256(p.add(o).cast()));
        let mask = |byte| {
            let byte = _mm256_set1_epi8(byte);
            v.iter().enumerate().fold(0u128, |m, (i, &x)| {
                m | (_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, byte)) as u32 as u128) << (32 * i)
            })
        };
        (mask(b',' as i8), mask(b'\n' as i8), mask(b'"' as i8))
    };
    if newlines == 0 {
        return None;
    }
    let len = newlines.trailing_zeros() as usize;
    let below = (1u128 << len) - 1;
    let mut c = commas & below;
    let mut out: [&'a [u8]; N] = [&[]; N];
    if len == 0 || c.count_ones() as usize != N - 1 {
        return Some((false, at + len + 1, out));
    }
    let row = &data[at..at + len + 1];
    let mut s = 0usize;
    let mut paired = 0u32;
    for (i, dst) in out.iter_mut().enumerate() {
        let e = if i == N - 1 { len } else { c.trailing_zeros() as usize };
        c &= c.wrapping_sub(1);
        let size = e - s;
        let (left, right) = (row[s], row[e.saturating_sub(1)]);
        let q = (size >= 2) & (left == b'"') & (right == b'"');
        let t = q as usize;
        *dst = &row[s + t..e - t];
        paired += 2 * q as u32;
        s = e + 1;
    }
    // Every quote must be one of the pairs around a whole field.
    Some((paired == (quotes & below).count_ones(), at + len + 1, out))
}

/// A row of up to 255 bytes with `N` fields, from four 64-byte mask windows:
/// the middle tier between the one- and two-window paths (rows of at most 63
/// or about 120 bytes) and the byte-by-byte `slow_row`, for rows that carry
/// a long ID. Returns the start of the next row and the fields, or None when
/// the row is longer, or is not well formed in any way (the caller then asks
/// `slow_row`, which decides).
///
/// Safety: `at + 256 <= data.len()`.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(never)]
unsafe fn wide_row<'a, const N: usize>(data: &'a [u8], at: usize) -> Option<(usize, [&'a [u8]; N])> {
    let base = data.as_ptr().add(at);
    let (mut commas, mut newlines, mut quotes) = ([0u64; 4], [0u64; 4], [0u64; 4]);
    for w in 0..4 {
        (commas[w], newlines[w], quotes[w]) = masks64(base.add(64 * w));
    }
    let len = (0..4).find(|&w| newlines[w] != 0).map(|w| 64 * w + newlines[w].trailing_zeros() as usize)?;
    if len == 0 {
        return None;
    }
    // The bits of window `w` that lie below the newline.
    let below = |w: usize| -> u64 {
        let lo = 64 * w;
        if len <= lo {
            0
        } else if len - lo >= 64 {
            !0
        } else {
            (1u64 << (len - lo)) - 1
        }
    };
    let mut pos = [0usize; N];
    let mut k = 0;
    for w in 0..4 {
        let mut m = commas[w] & below(w);
        while m != 0 {
            if k == N - 1 {
                return None;
            }
            pos[k] = 64 * w + m.trailing_zeros() as usize;
            k += 1;
            m &= m - 1;
        }
    }
    if k != N - 1 {
        return None;
    }
    pos[N - 1] = len;
    let mut quote_bytes = 0u32;
    for w in 0..4 {
        quote_bytes += (quotes[w] & below(w)).count_ones();
    }
    let mut out: [&'a [u8]; N] = [&[]; N];
    let (mut start, mut paired) = (0usize, 0u32);
    for i in 0..N {
        let end = pos[i];
        let (mut a, mut b) = (start, end);
        if end - start >= 2 && *base.add(start) == b'"' && *base.add(end - 1) == b'"' {
            a += 1;
            b -= 1;
            paired += 2;
        }
        out[i] = std::slice::from_raw_parts(base.add(a), b - a);
        start = end + 1;
    }
    // Every quote must be one of the pairs around a whole field.
    if paired != quote_bytes {
        return None;
    }
    Some((at + len + 1, out))
}

/// A row the 64-byte windows could not take: 128 bytes at once when they
/// are inside `data`, else the scalar path.
#[cfg(target_feature = "avx2")]
#[inline(always)]
fn longer_row<'a, const N: usize>(data: &'a [u8], at: usize) -> (bool, usize, [&'a [u8]; N]) {
    if at + 128 <= data.len() {
        if let Some(r) = medium_row(data, at) {
            return r;
        }
    }
    #[cfg(target_feature = "bmi2")]
    if at + 256 <= data.len() {
        // Safety: at + 256 <= data.len().
        if let Some((next, out)) = unsafe { wide_row::<N>(data, at) } {
            return (true, next, out);
        }
    }
    slow_row(data, at)
}

/// The row at `at` by the scalar `fields`: (well formed, start of the next
/// row, fields). Returned by value so the caller's own array never needs an
/// address and stays in registers.
#[cold]
#[inline(never)]
fn slow_row<'a, const N: usize>(data: &'a [u8], at: usize) -> (bool, usize, [&'a [u8]; N]) {
    let stop = memchr(b'\n', data, at).unwrap_or(data.len());
    let mut opt = [None; N];
    let ok = fields(&data[at..stop], &mut opt);
    (ok, stop + 1, opt.map(|v| v.unwrap_or(&[])))
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
    #[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
    if N <= 4 {
        return rows_small::<N>(data, start, end, f);
    }
    #[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
    if N == 7 {
        // Safety: N == 7, so the array types are one and the same.
        return rows7(data, start, end, |r| f(unsafe { &*(r as *const [&'a [u8]; 7] as *const [&'a [u8]; N]) }));
    }
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
        for round in 0..30000 {
            let len = (next() % 900) as usize;
            let alphabet = [SHORT, LONG, ROWS][round % 3];
            let data: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            let size = 1 + (next() % 300) as usize;
            let (mut want, mut got) = (Vec::new(), Vec::new());
            let (mut want7, mut got7) = (Vec::new(), Vec::new());
            let (mut want2, mut got2, mut want4, mut got4) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
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
                    let mut f = [None; 2];
                    if fields::<2>(line, &mut f) {
                        want2.push(f.map(|v| v.map_or(Vec::new(), <[u8]>::to_vec)));
                    }
                    let mut f = [None; 4];
                    if fields::<4>(line, &mut f) {
                        want4.push(f.map(|v| v.map_or(Vec::new(), <[u8]>::to_vec)));
                    }
                });
                rows::<3>(&data, s, e, |f| got.push(f.map(<[u8]>::to_vec)));
                rows::<7>(&data, s, e, |f| got7.push(f.map(<[u8]>::to_vec)));
                rows::<2>(&data, s, e, |f| got2.push(f.map(<[u8]>::to_vec)));
                rows::<4>(&data, s, e, |f| got4.push(f.map(<[u8]>::to_vec)));
            }
            assert_eq!(want, got, "round {round}: {:?}", String::from_utf8_lossy(&data));
            assert_eq!(want7, got7, "round {round}: {:?}", String::from_utf8_lossy(&data));
            assert_eq!(want2, got2, "round {round}: {:?}", String::from_utf8_lossy(&data));
            assert_eq!(want4, got4, "round {round}: {:?}", String::from_utf8_lossy(&data));
        }
    }

    /// Rows built from random fields (empty, quoted, lone or stray quotes,
    /// long, missing or extra commas) read the same through `rows` as through
    /// `lines` + `fields`, for every field count the pipeline uses.
    #[test]
    fn rows_structured_match_reference() {
        let mut state = 0x1234_5678_9ABC_DEF1u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..4000 {
            let mut data = Vec::new();
            data.extend_from_slice(b"h1,h2,h3\n");
            let count = 1 + next() % 40;
            for _ in 0..count {
                let n = match next() % 20 {
                    0 => 1 + next() % 9,
                    1 => 7 + next() % 2,
                    _ => [7, 7, 4, 2, 3][(next() % 5) as usize],
                };
                for i in 0..n {
                    let long = next() % 12 == 0;
                    let len = if long { next() % 70 } else { [0, 1, 2, 5, 8, 9, 10, 13, 20, 20, 25, 6][(next() % 12) as usize] };
                    let mut field: Vec<u8> = (0..len).map(|_| b"abcz019 _-:T"[(next() % 12) as usize]).collect();
                    match next() % 10 {
                        0 | 1 => {
                            field.insert(0, b'"');
                            field.push(b'"');
                        }
                        2 if next() % 8 == 0 => field.insert(0, b'"'),
                        3 if next() % 8 == 0 => field.push(b'"'),
                        4 if next() % 8 == 0 => {
                            let at = (next() as usize) % (field.len() + 1);
                            field.insert(at, b'"');
                        }
                        _ => {}
                    }
                    data.extend_from_slice(&field);
                    if i + 1 < n {
                        data.push(b',');
                    }
                }
                if next() % 40 != 0 {
                    data.push(b'\n');
                }
                if next() % 60 == 0 {
                    data.push(b'\n');
                }
            }
            let size = [97, 128, 129, 1000, 4096, 1 << 20][(next() % 6) as usize].min(data.len().max(1));
            macro_rules! check {
                ($n:literal) => {{
                    let (mut want, mut got) = (Vec::new(), Vec::new());
                    for (s, e) in chunks(data.len(), size) {
                        lines(&data, s, e, |line| {
                            let mut f = [None; $n];
                            if fields::<$n>(line, &mut f) {
                                want.push(f.map(|v| v.map_or(Vec::new(), <[u8]>::to_vec)));
                            }
                        });
                        rows::<$n>(&data, s, e, |f| got.push(f.map(<[u8]>::to_vec)));
                    }
                    assert_eq!(want, got, "round {round} n {}: {:?}", $n, String::from_utf8_lossy(&data));
                }};
            }
            check!(2);
            check!(3);
            check!(4);
            check!(7);
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
