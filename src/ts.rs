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

/// Epoch seconds of `YYYY-MM-DDTHH:MM:SS` in s[..19] (s.len() >= 19), or None
/// unless it is a valid calendar date and time.
#[cfg(target_feature = "ssse3")]
#[inline(always)]
fn civil_seconds(s: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    // Safety: s has at least 19 bytes, so both loads are inside it; SSSE3 is enabled.
    let fields: [u16; 8] = unsafe {
        let v = _mm_loadu_si128(s.as_ptr().cast());
        let literals = _mm_setr_epi8(0, 0, 0, 0, 45, 0, 0, 45, 0, 0, 84, 0, 0, 58, 0, 0);
        const SEPARATORS: i32 = (1 << 4) | (1 << 7) | (1 << 10) | (1 << 13);
        const DIGITS: i32 = 0xffff & !SEPARATORS;
        let same = _mm_movemask_epi8(_mm_cmpeq_epi8(v, literals));
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let digit = _mm_movemask_epi8(_mm_cmpeq_epi8(d, _mm_min_epu8(d, _mm_set1_epi8(9))));
        if (same & SEPARATORS) != SEPARATORS || (digit & DIGITS) != DIGITS {
            return None;
        }
        let packed = _mm_shuffle_epi8(d, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
        std::mem::transmute(_mm_maddubs_epi16(packed, _mm_set1_epi16(0x010a)))
    };
    let (colon, s1, s2) = (s[16], s[17].wrapping_sub(b'0'), s[18].wrapping_sub(b'0'));
    let [cc, yy, month, day, hour, minute, ..] = fields;
    let year = cc as i64 * 100 + yy as i64;
    let second = s1 as i64 * 10 + s2 as i64;
    let row = (year - TABLE_YEAR) as usize;
    if row >= TABLE_YEARS {
        return civil_seconds_slow(s);
    }
    let month0 = (month as usize).wrapping_sub(1);
    let ok = (colon == b':') & (s1 <= 9) & (s2 <= 9) & (month0 < 12) & (hour <= 23) & (minute <= 59) & (second <= 59);
    if !ok {
        return None;
    }
    let m = MONTHS[row * 12 + month0];
    if day == 0 || day as i32 > (m & 0xff) {
        return None;
    }
    Some(((m >> 8) as i64 + day as i64 - 1) * 86_400 + hour as i64 * 3_600 + minute as i64 * 60 + second)
}

#[cfg(not(target_feature = "ssse3"))]
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
        #[cfg(target_feature = "ssse3")]
        13 => digits_13(s),
        _ => digits(s),
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
            #[cfg(target_feature = "ssse3")]
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

#[cfg(target_feature = "ssse3")]
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

#[cfg(all(test, target_feature = "ssse3"))]
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

#[cfg(all(test, not(target_feature = "ssse3")))]
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
