//! Timestamps (rules.md "Timestamps") and the small number parsers.

pub const DAY: i64 = 86_400_000;

#[inline]
fn digit(b: u8) -> Option<i64> {
    if b.is_ascii_digit() { Some((b - b'0') as i64) } else { None }
}

#[inline]
fn two(s: &[u8], at: usize) -> Option<i64> {
    Some(digit(s[at])? * 10 + digit(s[at + 1])?)
}

/// Days since 1970-01-01 (Howard Hinnant's days_from_civil).
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// A run of ASCII digits as a number; None if empty, not all digits, or over i64.
#[inline]
pub fn digits(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    if s.len() <= 18 {
        for &b in s {
            v = v * 10 + digit(b)?;
        }
        return Some(v);
    }
    for &b in s {
        let d = digit(b)?;
        v = v.checked_mul(10)?.checked_add(d)?;
    }
    Some(v)
}

/// First year of `MONTHS`.
const TABLE_YEAR: i64 = 1900;
const TABLE_YEARS: usize = 300;

/// Per month of 1900..2200: days since the epoch of its 1st << 8 | its length.
static MONTHS: [i32; TABLE_YEARS * 12] = {
    let mut t = [0i32; TABLE_YEARS * 12];
    let mut i = 0;
    while i < TABLE_YEARS * 12 {
        let year = TABLE_YEAR + (i / 12) as i64;
        let month = (i % 12) as i64 + 1;
        // days_from_civil and days_in_month, in const form.
        let y = if month <= 2 { year - 1 } else { year };
        let era = y.div_euclid(400);
        let yoe = y - era * 400;
        let doy = (153 * ((month + 9) % 12) + 2) / 5;
        let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let len = match month {
            2 => if leap { 29 } else { 28 },
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        t[i] = ((days as i32) << 8) | len;
        i += 1;
    }
    t
};

/// Epoch seconds of `YYYY-MM-DDTHH:MM:SS` in s[..19] (s.len() >= 20), or None
/// unless it is a valid calendar date and time.
#[cfg(target_feature = "sse4.1")]
#[inline(always)]
fn civil_seconds(s: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    assert!(s.len() >= 20);
    // Years 2000..2099 take this path; any other year, or a non-digit in the
    // year, goes to the scalar parser, which also decides what is valid.
    let y = u32::from_le_bytes([s[0], s[1], s[2], s[3]]);
    let d2 = ((y >> 16) & 0xff).wrapping_sub(b'0' as u32);
    let d3 = (y >> 24).wrapping_sub(b'0' as u32);
    if (y & 0xffff) != 0x3032 || (d2 > 9) | (d3 > 9) {
        return civil_seconds_slow(s);
    }
    let row = (100 + d2 * 10 + d3) as usize;
    // Safety: s has at least 20 bytes, so the 16-byte load of s[4..20] is
    // inside it; SSE4.1 (and so SSSE3) is enabled.
    let (lo, second) = unsafe {
        // -MM-DDTHH:MM:SS and the byte after the seconds.
        let v = _mm_loadu_si128(s.as_ptr().add(4).cast());
        let pattern = _mm_setr_epi8(45, 48, 48, 45, 48, 48, 84, 48, 48, 58, 48, 48, 58, 48, 48, 0);
        let limit = _mm_setr_epi8(0, 9, 9, 0, 9, 9, 0, 9, 9, 0, 9, 9, 0, 9, 9, -1);
        // Digits become 0..=9 and separators 0 when the bytes are right.
        let t = _mm_sub_epi8(v, pattern);
        let shape = _mm_cmpeq_epi8(_mm_min_epu8(t, limit), t);
        let packed = _mm_shuffle_epi8(t, _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1));
        // Lanes: month, day, hour, minute, second, 0, 0, 0.
        let pairs = _mm_maddubs_epi16(packed, _mm_set1_epi16(0x010a));
        // Month and day count from 0 here: a 0 wraps to 0xffff and fails.
        let x = _mm_sub_epi16(pairs, _mm_setr_epi16(1, 1, 0, 0, 0, 0, 0, 0));
        let max = _mm_setr_epi16(11, 30, 23, 59, 59, -1, -1, -1);
        let range = _mm_cmpeq_epi16(_mm_min_epu16(x, max), x);
        if _mm_movemask_epi8(_mm_and_si128(shape, range)) != 0xffff {
            return None;
        }
        (_mm_cvtsi128_si64(x) as u64, _mm_extract_epi16(x, 4) as i64)
    };
    let month0 = (lo & 0xffff) as usize;
    let day0 = (lo >> 16) & 0xffff;
    let hour = (lo >> 32) & 0xffff;
    let minute = lo >> 48;
    // Safety: month0 < 12 was checked above and row < 200.
    let m = unsafe { *MONTHS.get_unchecked(row * 12 + month0) };
    if day0 as i32 >= (m & 0xff) {
        return None;
    }
    Some(((m >> 8) as i64 + day0 as i64) * 86_400 + (hour * 3_600 + minute * 60) as i64 + second)
}

#[cfg(not(target_feature = "sse4.1"))]
#[inline(always)]
fn civil_seconds(s: &[u8]) -> Option<i64> {
    civil_seconds_slow(s)
}

/// The scalar form of `civil_seconds`.
fn civil_seconds_slow(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = two(s, 0)? * 100 + two(s, 2)?;
    let [month, day, hour, minute, second] = [two(s, 5)?, two(s, 8)?, two(s, 11)?, two(s, 14)?, two(s, 17)?];
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Epoch milliseconds of F1, F2 or F3, or None for a bad timestamp. An F3 value
/// too large for i64 is None too: it is outside every window anyway.
#[inline(always)]
pub fn parse(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 if s[19] == b'Z' => civil_seconds(s).map(|v| v * 1_000),
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let (h, m) = (two(s, 20)?, two(s, 23)?);
            if h > 23 || m > 59 {
                return None;
            }
            let offset = if s[19] == b'+' { h * 60 + m } else { -(h * 60 + m) };
            civil_seconds(s).map(|v| v * 1_000 - offset * 60_000)
        }
        #[cfg(target_feature = "sse4.1")]
        13 => digits_13(s),
        _ => digits(s),
    }
}

/// `parse` for a field that is almost always F1 (`ingest_ts`): tests F1
/// first, so the common case is one predictable branch.
#[inline(always)]
pub fn parse_f1_first(s: &[u8]) -> Option<i64> {
    if s.len() == 20 && s[19] == b'Z' {
        return civil_seconds(s).map(|v| v * 1_000);
    }
    parse_cold(s)
}

#[inline(never)]
fn parse_cold(s: &[u8]) -> Option<i64> {
    parse(s)
}

/// Exact day prefixes for the dates near this dataset's time window.
#[derive(Clone, Copy, Default)]
struct DatePrefix {
    head: u64,
    tail: u32,
    midnight: i64,
}

pub struct WindowParser {
    dates: [DatePrefix; 64],
}

impl WindowParser {
    pub fn new(base: i64, end: i64) -> Self {
        let mut dates = [DatePrefix::default(); 64];
        let first = base.div_euclid(DAY).saturating_sub(1).max(days_from_civil(0, 1, 1));
        let last = end.div_euclid(DAY).saturating_add(5).min(days_from_civil(9999, 12, 31));
        if first <= last {
            let mut year = 0;
            let mut upper = 10_000;
            while upper - year > 1 {
                let mid = (year + upper) / 2;
                if days_from_civil(mid, 1, 1) <= first {
                    year = mid;
                } else {
                    upper = mid;
                }
            }
            let mut month = 1;
            while month < 12 && days_from_civil(year, month + 1, 1) <= first {
                month += 1;
            }
            let mut day = first - days_from_civil(year, month, 1) + 1;
            for epoch_day in first..=last {
                let bytes = format!("{year:04}-{month:02}-{day:02}T").into_bytes();
                let index = date_slot(bytes[8], bytes[9]);
                // A repeated day of month can replace a slot. The exact
                // prefix check sends the displaced date to the general parser.
                dates[index] = DatePrefix {
                    head: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                    tail: u32::from_le_bytes([bytes[8], bytes[9], b'T', 0]),
                    midnight: epoch_day * DAY,
                };
                day += 1;
                if day > days_in_month(year, month) {
                    day = 1;
                    month += 1;
                    if month == 13 {
                        month = 1;
                        year += 1;
                    }
                }
            }
        }
        Self { dates }
    }

    #[inline(always)]
    fn cached_f1(&self, iso: &[u8; 20]) -> Option<i64> {
        #[cfg(target_feature = "bmi2")]
        {
            let date = self.dates[date_slot(iso[8], iso[9])];
            let head = u64::from_le_bytes(iso[..8].try_into().unwrap());
            let tail = u32::from_le_bytes(iso[8..12].try_into().unwrap()) & 0xff_ffff;
            if date.head == head && date.tail == tail && date.tail != 0 {
                return utc_millis(iso).map(|offset| date.midnight + offset);
            }
        }
        #[cfg(not(target_feature = "bmi2"))]
        let _ = iso;
        None
    }

    #[inline(always)]
    pub fn parse(&self, field: &[u8]) -> Option<i64> {
        if let Ok(iso) = <&[u8; 20]>::try_from(field) {
            if let Some(time) = self.cached_f1(iso) {
                return Some(time);
            }
        }
        parse(field)
    }

    #[inline(always)]
    pub fn parse_f1_first(&self, field: &[u8]) -> Option<i64> {
        if let Ok(iso) = <&[u8; 20]>::try_from(field) {
            if let Some(time) = self.cached_f1(iso) {
                return Some(time);
            }
        }
        parse_f1_first(field)
    }
}

#[inline(always)]
fn date_slot(a: u8, b: u8) -> usize {
    (((a & 3) << 4) | (b & 15)) as usize
}

#[cfg(target_feature = "bmi2")]
const fn clock_units(maximum: u32, scale: u32) -> [u32; 256] {
    let mut units = [u32::MAX; 256];
    let mut value = 0;
    while value <= maximum {
        units[((value % 10) * 16 + value / 10) as usize] = value * scale;
        value += 1;
    }
    units
}

#[cfg(target_feature = "bmi2")]
#[inline(always)]
fn utc_millis(iso: &[u8; 20]) -> Option<i64> {
    static HOUR: [u32; 256] = clock_units(23, 3_600_000);
    static MINUTE: [u32; 256] = clock_units(59, 60_000);
    static SECOND: [u32; 256] = clock_units(59, 1_000);
    let clock = u64::from_le_bytes(iso[11..19].try_into().unwrap());
    const NIBBLES: u64 = 0x0f0f_000f_0f00_0f0f;
    const TEMPLATE: u64 = u64::from_le_bytes(*b"00:00:00");
    if (clock & !NIBBLES != TEMPLATE) | (iso[19] != b'Z') {
        return None;
    }
    // The checked high nibbles are all ASCII '3'. Each lookup rejects
    // invalid low nibbles and validates the respective clock range.
    let packed = unsafe { std::arch::x86_64::_pext_u64(clock, NIBBLES) };
    let hour = HOUR[packed as u8 as usize];
    let minute = MINUTE[(packed >> 8) as u8 as usize];
    let second = SECOND[(packed >> 16) as u8 as usize];
    if (hour | minute | second) == u32::MAX {
        return None;
    }
    Some(i64::from(hour + minute + second))
}

#[cfg(test)]
#[test]
fn window_parser_matches_general_parser_across_calendar_boundaries() {
    let base = parse(b"2028-02-24T00:00:00Z").unwrap();
    let end = parse(b"2028-03-30T00:00:00Z").unwrap();
    let window = WindowParser::new(base, end);
    for field in [
        "2028-02-25T03:42:15Z", "2028-03-25T03:42:15Z",
        "2028-02-29T23:59:59Z", "2028-03-01T00:00:00Z",
        "2028-02-30T23:59:59Z", "2028-03-01T24:00:00Z",
        "2028-03-01T00:60:00Z", "2028-03-01T00:00:60Z",
        "2028-03-01T0?:00:00Z", "2028-03-01T00:00:00z",
        "2028-03-01T00;00:00Z", "2028-03-01T00:00:00+00:01",
        "2028-01-01T01:02:03Z", "1843430400000",
        "00000000000000000001", "０８",
    ] {
        let bytes = field.as_bytes();
        assert_eq!(window.parse(bytes), parse(bytes), "{field}");
        assert_eq!(window.parse_f1_first(bytes), parse_f1_first(bytes), "{field}");
    }
}

/// The previous parser, kept as the reference for the tests.
#[cfg(test)]
pub fn parse_reference(s: &[u8]) -> Option<i64> {
    let offset_minutes = match s.len() {
        20 if s[19] == b'Z' => 0,
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let (h, m) = (two(s, 20)?, two(s, 23)?);
            if h > 23 || m > 59 {
                return None;
            }
            if s[19] == b'+' { h * 60 + m } else { -(h * 60 + m) }
        }
        _ => {
            #[cfg(target_feature = "sse4.1")]
            if s.len() == 13 {
                return digits_13(s);
            }
            return digits(s);
        }
    };
    let year = two(s, 0)? * 100 + two(s, 2)?;
    let [month, day, hour, minute, second] = civil_fields(s)?;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    Some(seconds * 1_000 - offset_minutes * 60_000)
}

#[cfg(target_feature = "sse4.1")]
#[inline]
fn digits_13(s: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    let hi = i64::from_le_bytes(s[..8].try_into().unwrap());
    // Divide the input into eight high digits and five low digits, padding
    // the latter on the left with zeros for the same SIMD reduction.
    let lo = (i64::from_le_bytes(s[5..13].try_into().unwrap()) & !0xff_ffff) | 0x30_3030;
    // Both 64-bit loads are within the 13-byte slice. SSSE3 is enabled.
    unsafe {
        let bytes = _mm_set_epi64x(lo, hi);
        let digits = _mm_sub_epi8(bytes, _mm_set1_epi8(b'0' as i8));
        if _mm_movemask_epi8(_mm_cmpeq_epi8(digits, _mm_min_epu8(digits, _mm_set1_epi8(9)))) != 0xffff {
            return None;
        }
        let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
        let groups = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0064));
        let [a, b, c, d]: [i32; 4] = std::mem::transmute(groups);
        Some((a as i64 * 10_000 + b as i64) * 100_000 + c as i64 * 10_000 + d as i64)
    }
}

#[cfg(all(test, target_feature = "sse4.1"))]
#[inline]
fn civil_fields(s: &[u8]) -> Option<[i64; 5]> {
    use std::arch::x86_64::*;
    let body: &[u8; 16] = s[4..20].try_into().unwrap();
    // body is exactly 16 bytes and SSSE3 is enabled.
    unsafe {
        let input = _mm_loadu_si128(body.as_ptr().cast());
        let literals = _mm_setr_epi8(45, 0, 0, 45, 0, 0, 84, 0, 0, 58, 0, 0, 58, 0, 0, 0);
        let same = _mm_movemask_epi8(_mm_cmpeq_epi8(input, literals));
        const SEPARATORS: i32 = (1 << 0) | (1 << 3) | (1 << 6) | (1 << 9) | (1 << 12);
        if same & SEPARATORS != SEPARATORS {
            return None;
        }
        let digits = _mm_sub_epi8(input, _mm_set1_epi8(b'0' as i8));
        let valid = _mm_movemask_epi8(_mm_cmpeq_epi8(digits, _mm_min_epu8(digits, _mm_set1_epi8(9))));
        const DIGITS: i32 = 0x7fff & !SEPARATORS;
        if valid & DIGITS != DIGITS {
            return None;
        }
        let positions = _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1);
        let packed = _mm_shuffle_epi8(digits, positions);
        let pairs = _mm_maddubs_epi16(packed, _mm_set1_epi16(0x010a));
        let [month, day, hour, minute, second, ..]: [u16; 8] = std::mem::transmute(pairs);
        Some([month as i64, day as i64, hour as i64, minute as i64, second as i64])
    }
}

#[cfg(all(test, not(target_feature = "sse4.1")))]
#[inline]
fn civil_fields(s: &[u8]) -> Option<[i64; 5]> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    Some([two(s, 5)?, two(s, 8)?, two(s, 11)?, two(s, 14)?, two(s, 17)?])
}

#[cfg(test)]
mod tests {
    use super::{parse, parse_reference};

    /// The table-driven parser agrees with the reference on mutated timestamps.
    #[test]
    fn matches_reference() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let seeds: [&[u8]; 8] = [
            b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00Z", b"1900-01-01T00:00:00Z", b"2199-12-31T23:59:59Z",
            b"2026-10-03T19:58:10-04:00", b"1899-12-31T23:59:59+12:00", b"1791071890000", b"2200-03-01T00:00:00Z",
        ];
        let alphabet = b"0123456789-T:Z+ /a";
        for round in 0..400_000 {
            let mut s = seeds[round % seeds.len()].to_vec();
            for _ in 0..(next() % 3) {
                let at = (next() as usize) % s.len();
                s[at] = alphabet[(next() as usize) % alphabet.len()];
            }
            if round % 7 == 0 {
                // A random valid-looking date, including impossible days.
                let v = next();
                let text = format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                    1890 + v % 330, (v >> 9) % 14, (v >> 13) % 33, (v >> 19) % 25, (v >> 25) % 61, (v >> 31) % 61
                );
                s = text.into_bytes();
            }
            assert_eq!(parse(&s), parse_reference(&s), "{:?}", String::from_utf8_lossy(&s));
        }
    }


    /// Every byte value at every position of valid forms agrees with the
    /// reference, and so do the calendar edges of the table path.
    #[test]
    fn single_byte_mutations_and_edges() {
        let seeds: [&[u8]; 5] = [
            b"2026-10-03T23:58:10Z", b"2000-02-29T00:00:00Z", b"2099-12-31T23:59:59Z",
            b"2026-10-03T19:58:10-04:00", b"1791071890000",
        ];
        for seed in seeds {
            for at in 0..seed.len() {
                for b in 0..=255u8 {
                    let mut s = seed.to_vec();
                    s[at] = b;
                    assert_eq!(parse(&s), parse_reference(&s), "{:?}", String::from_utf8_lossy(&s));
                }
            }
        }
        for year in [1899, 1900, 1999, 2000, 2001, 2024, 2028, 2099, 2100, 2199, 2200] {
            for month in 0..=13 {
                for day in [0, 1, 28, 29, 30, 31, 32] {
                    for (h, m, sec) in [(0, 0, 0), (23, 59, 59), (24, 0, 0), (0, 60, 0), (0, 0, 60), (12, 34, 56)] {
                        let s = format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{sec:02}Z");
                        assert_eq!(parse(s.as_bytes()), parse_reference(s.as_bytes()), "{s}");
                        let s = format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{sec:02}+05:30");
                        assert_eq!(parse(s.as_bytes()), parse_reference(s.as_bytes()), "{s}");
                    }
                }
            }
        }
        assert_eq!(parse(b"2000-02-29T00:00:00Z"), Some(951_782_400_000));
        assert_eq!(parse(b"2100-02-29T00:00:00Z"), None);
        assert_eq!(parse(b"2026-00-10T00:00:00Z"), None);
        assert_eq!(parse(b"2026-04-31T00:00:00Z"), None);
        assert_eq!(parse(b"2026-04-30T00:00:00Z"), Some(1_777_507_200_000));
    }
    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse(b"1791071890000"), Some(1_791_071_890_000));
        assert_eq!(parse(b"0001791071890000"), Some(1_791_071_890_000));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert!(parse(b"2026-10-03T23:58:10+23:59").is_some());
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
    }

    #[test]
    fn bad() {
        for s in [
            &b""[..], b"2026-10-03T23:58:10.000Z", b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T23:58:10Z", b"2026-02-30T23:58:10Z", b"2027-02-29T00:00:00Z",
            b"2026-10-03T24:00:00Z", b"2026-10-03T23:59:60Z", b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200", b"999999999999999999999",
            "\u{0662}\u{0665}\u{0660}".as_bytes(), "２０26-10-03T23:58:10Z".as_bytes(),
            b"2026-10-03T23:58:10Z\r",
        ] {
            assert_eq!(parse(s), None, "{:?}", String::from_utf8_lossy(s));
        }
    }
}
