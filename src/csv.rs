//! Input files: memory-mapped, cut into chunks on line ownership, and split
//! into fields by the dialect in rules.md "CSV dialect".

use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::Path;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
}
const PROT_READ: i32 = 1;
const MAP_PRIVATE: i32 = 2;

/// A read-only mapping that lives until the process exits.
pub fn map(path: &Path) -> &'static [u8] {
    let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let len = file.metadata().unwrap().len() as usize;
    if len == 0 {
        return &[];
    }
    unsafe {
        let p = mmap(std::ptr::null_mut(), len, PROT_READ, MAP_PRIVATE, file.as_raw_fd(), 0);
        assert!(p as isize != -1, "mmap {}", path.display());
        std::slice::from_raw_parts(p, len)
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

/// Bitmasks of one 64-byte block at `at`: separators (`,` or `\n`) and quotes.
#[inline]
fn block(data: &[u8], at: usize) -> (u64, u64) {
    #[cfg(target_feature = "avx2")]
    if at + 64 <= data.len() {
        use std::arch::x86_64::*;
        unsafe {
            let p = data.as_ptr().add(at) as *const __m256i;
            let (a, b) = (_mm256_loadu_si256(p), _mm256_loadu_si256(p.add(1)));
            let mask = |c: u8| {
                let c = _mm256_set1_epi8(c as i8);
                let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, c)) as u32 as u64;
                let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, c)) as u32 as u64;
                lo | (hi << 32)
            };
            return (mask(b',') | mask(b'\n'), mask(b'"'));
        }
    }
    let (mut sep, mut quote) = (0u64, 0u64);
    for (i, &c) in data[at..(at + 64).min(data.len())].iter().enumerate() {
        sep |= ((c == b',' || c == b'\n') as u64) << i;
        quote |= ((c == b'"') as u64) << i;
    }
    (sep, quote)
}

/// Thread-local scratch for `scan`.
#[derive(Default)]
pub struct Scanner {
    quotes: Vec<u64>,
}

impl Scanner {
    /// Like `lines` + `fields`: calls `f` with the N fields of every
    /// well-formed line that starts in `data[start..end]`, header skipped.
    #[inline]
    pub fn scan<'a, const N: usize>(
        &mut self,
        data: &'a [u8],
        start: usize,
        end: usize,
        mut f: impl FnMut(&[Option<&'a [u8]>; N]),
    ) {
        let from = if start == 0 { 0 } else { start - 1 };
        let mut at = match memchr(b'\n', data, from) {
            Some(nl) => nl + 1,
            None => return,
        };
        if at >= end {
            return;
        }
        let base = at & !63;
        self.quotes.clear();
        let (sep0, quote0) = block(data, base);
        self.quotes.push(quote0);
        let mut bi = 0usize;
        let mut m = sep0 & (!0u64 << (at - base));
        let mut out: [Option<&'a [u8]>; N] = [None; N];
        let mut bounds = [0usize; N];
        let mut n = 0usize;
        let mut bad = false;
        loop {
            while m == 0 {
                bi += 1;
                let pos = base + bi * 64;
                if pos >= data.len() {
                    break;
                }
                let (s, q) = block(data, pos);
                self.quotes.push(q);
                m = s;
            }
            let (p, newline) = if m == 0 {
                (data.len(), true)
            } else {
                let p = base + bi * 64 + m.trailing_zeros() as usize;
                m &= m - 1;
                (p, data[p] == b'\n')
            };
            if !newline {
                if n + 1 < N {
                    bounds[n] = p;
                    n += 1;
                } else {
                    bad = true;
                }
                continue;
            }
            // End of the line [at, p).
            if !bad && n + 1 == N && p > at {
                bounds[n] = p;
                let mut s = at;
                let mut ok = true;
                for k in 0..N {
                    let e = bounds[k];
                    match self.field(data, base, s, e) {
                        Some(v) => out[k] = v,
                        None => {
                            ok = false;
                            break;
                        }
                    }
                    s = e + 1;
                }
                if ok {
                    f(&out);
                }
            }
            at = p + 1;
            if at >= end || p >= data.len() {
                return;
            }
            n = 0;
            bad = false;
        }
    }

    /// The field data[s..e], unquoted, using the quote masks.
    #[inline]
    fn field<'a>(&self, data: &'a [u8], base: usize, s: usize, e: usize) -> Option<Option<&'a [u8]>> {
        let quotes = self.count_quotes(base, s, e);
        let v = if quotes == 0 {
            &data[s..e]
        } else if quotes == 2 && e - s >= 2 && data[s] == b'"' && data[e - 1] == b'"' {
            &data[s + 1..e - 1]
        } else {
            return None;
        };
        Some(if v.is_empty() { None } else { Some(v) })
    }

    #[inline]
    fn count_quotes(&self, base: usize, s: usize, e: usize) -> u32 {
        if s == e {
            return 0;
        }
        let (first, last) = ((s - base) / 64, (e - 1 - base) / 64);
        let mut count = 0;
        for b in first..=last {
            let mut q = self.quotes[b];
            if b == first {
                q &= !0u64 << ((s - base) % 64);
            }
            if b == last {
                let hi = (e - 1 - base) % 64;
                if hi < 63 {
                    q &= (1u64 << (hi + 1)) - 1;
                }
            }
            count += q.count_ones();
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut scanner = Scanner::default();
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
