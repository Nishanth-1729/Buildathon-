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
///
/// The 16 bytes at s[4..20] hold everything but the year: one shuffle and one
/// multiply-add give month, day, hour, minute and second as 16-bit lanes, one
/// compare checks all five ranges, and one more multiply-add gives the time
/// of day. The year is four digits read as one 32-bit word.
#[cfg(all(target_feature = "ssse3", target_feature = "sse4.1"))]
#[inline(always)]
fn civil_seconds(s: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    assert!(s.len() >= 20);
    // Safety: s has at least 20 bytes, so the 16 bytes at 4 and the 4 at 0 are
    // inside it; SSSE3 and SSE4.1 are enabled.
    unsafe {
        let v = _mm_loadu_si128(s.as_ptr().add(4).cast());
        // Lanes: 0 '-', 1-2 month, 3 '-', 4-5 day, 6 'T', 7-8 hour, 9 ':',
        // 10-11 minute, 12 ':', 13-14 second.
        let literals = _mm_setr_epi8(45, 0, 0, 45, 0, 0, 84, 0, 0, 58, 0, 0, 58, 0, 0, 0);
        const LITERALS: i32 = (1 << 0) | (1 << 3) | (1 << 6) | (1 << 9) | (1 << 12);
        const DIGITS: i32 = 0b0110_1101_1011_0110;
        let same = _mm_movemask_epi8(_mm_cmpeq_epi8(v, literals));
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let digit = _mm_movemask_epi8(_mm_cmpeq_epi8(d, _mm_min_epu8(d, _mm_set1_epi8(9))));
        // The year: four digits.
        let y = u32::from_le_bytes([s[0], s[1], s[2], s[3]]) ^ 0x3030_3030;
        let year_digits = ((y.wrapping_add(0x7676_7676)) | y) & 0x8080_8080 == 0;
        if (same & LITERALS) != LITERALS || (digit & DIGITS) != DIGITS || !year_digits {
            return None;
        }
        // [hour, minute, second, 0, month, day, 0, 0]
        let packed = _mm_shuffle_epi8(d, _mm_setr_epi8(7, 8, 10, 11, 13, 14, -1, -1, 1, 2, 4, 5, -1, -1, -1, -1));
        let lanes = _mm_maddubs_epi16(packed, _mm_set1_epi16(0x010a));
        let first = _mm_setr_epi16(0, 0, 0, 0, 1, 1, 0, 0);
        let limits = _mm_setr_epi16(23, 59, 59, 0, 11, 30, 0, 0);
        let shifted = _mm_sub_epi16(lanes, first);
        let in_range = _mm_cmpeq_epi16(_mm_min_epu16(shifted, limits), shifted);
        if _mm_movemask_epi8(in_range) != 0xffff {
            return None;
        }
        let weights = _mm_setr_epi16(3600, 60, 1, 0, 0, 0, 0, 0);
        let time = _mm_cvtsi128_si64(_mm_madd_epi16(lanes, weights));
        let seconds = (time as i32 + (time >> 32) as i32) as i64;
        let month_day = _mm_extract_epi64(shifted, 1) as u64;
        let (month0, day0) = ((month_day & 0xffff) as usize, ((month_day >> 16) & 0xffff) as i32);
        // y = ((d0*10 + d1) * 100) + (d2*10 + d3), from the pairs of digits.
        let pairs = y.wrapping_mul(10).wrapping_add(y >> 8);
        let year = ((pairs & 0xff) * 100 + ((pairs >> 16) & 0xff)) as i64;
        let row = (year - TABLE_YEAR) as usize;
        if row >= TABLE_YEARS {
            return civil_seconds_slow(s);
        }
        let m = MONTHS[row * 12 + month0];
        if day0 >= (m & 0xff) {
            return None;
        }
        Some(((m >> 8) as i64 + day0 as i64) * 86_400 + seconds)
    }
}

#[cfg(not(all(target_feature = "ssse3", target_feature = "sse4.1")))]
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

/// One slot of the `DateCache`.
#[derive(Clone, Copy)]
#[repr(C)]
struct Slot {
    /// The first 8 bytes of a validated `YYYY-MM-DDT`, then its last 3.
    a: u64,
    b: u32,
    days: i32,
}

const CACHE_SLOTS: usize = 4096;

/// A per-thread memo from the date prefix (`YYYY-MM-DDT`, 11 bytes) of a
/// validated timestamp to its days since the epoch. A data set holds a few
/// dozen dates, so nearly every lookup hits; a slot is only filled from a
/// prefix that `date_days` accepted, and a hit needs all 11 bytes equal.
pub struct DateCache {
    slots: Box<[Slot; CACHE_SLOTS]>,
}

impl DateCache {
    pub fn new() -> Self {
        // b has a bit that no 24-bit key has, so an empty slot never matches.
        let slots = vec![Slot { a: 0, b: 1 << 24, days: 0 }; CACHE_SLOTS].into_boxed_slice();
        DateCache { slots: slots.try_into().ok().unwrap() }
    }
}

/// Days since the epoch of the date in `YYYY-MM-DDT` (s.len() >= 11), if it
/// is a valid date of the table's years.
#[cfg(all(target_feature = "ssse3", target_feature = "sse4.1"))]
fn date_days(s: &[u8]) -> Option<i32> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' {
        return None;
    }
    let year = two(s, 0)? * 100 + two(s, 2)?;
    let (month, day) = (two(s, 5)?, two(s, 8)?);
    let row = year - TABLE_YEAR;
    if !(0..TABLE_YEARS as i64).contains(&row) || !(1..=12).contains(&month) {
        return None;
    }
    let m = MONTHS[row as usize * 12 + month as usize - 1];
    if day < 1 || day > (m & 0xff) as i64 {
        return None;
    }
    Some((m >> 8) + day as i32 - 1)
}

/// `civil_seconds` with the date from the cache: only the time of day is
/// parsed (one shuffle, one range compare, one multiply-add).
#[cfg(all(target_feature = "ssse3", target_feature = "sse4.1"))]
#[inline(always)]
fn civil_seconds_cached(s: &[u8], cache: &mut DateCache) -> Option<i64> {
    use std::arch::x86_64::*;
    assert!(s.len() >= 20);
    let a = u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);
    let b = u32::from_le_bytes([s[8], s[9], s[10], 0]);
    let slot = &mut cache.slots[((a ^ ((b as u64) << 40)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 52) as usize];
    let mut days = slot.days;
    if slot.a != a || slot.b != b {
        // A miss: a date not seen yet (or a bad one, which is never stored).
        match date_days(s) {
            Some(d) => {
                *slot = Slot { a, b, days: d };
                days = d;
            }
            None => return civil_seconds(s),
        }
    }
    // Safety: s has at least 20 bytes, so the 16 bytes at 4 are inside it;
    // SSSE3 and SSE4.1 are enabled.
    unsafe {
        let v = _mm_loadu_si128(s.as_ptr().add(4).cast());
        // Lanes: 7-8 hour, 9 ':', 10-11 minute, 12 ':', 13-14 second.
        let literals = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 58, 0, 0, 58, 0, 0, 0);
        const LITERALS: i32 = (1 << 9) | (1 << 12);
        const DIGITS: i32 = (1 << 7) | (1 << 8) | (1 << 10) | (1 << 11) | (1 << 13) | (1 << 14);
        let same = _mm_movemask_epi8(_mm_cmpeq_epi8(v, literals));
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let digit = _mm_movemask_epi8(_mm_cmpeq_epi8(d, _mm_min_epu8(d, _mm_set1_epi8(9))));
        // [hour, minute, second, 0, 0, 0, 0, 0]
        let packed = _mm_shuffle_epi8(d, _mm_setr_epi8(7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1));
        let lanes = _mm_maddubs_epi16(packed, _mm_set1_epi16(0x010a));
        let limits = _mm_setr_epi16(23, 59, 59, 0, 0, 0, 0, 0);
        let in_range = _mm_cmpeq_epi16(_mm_min_epu16(lanes, limits), lanes);
        if (same & LITERALS) != LITERALS || (digit & DIGITS) != DIGITS || _mm_movemask_epi8(in_range) != 0xffff {
            return None;
        }
        let time = _mm_cvtsi128_si64(_mm_madd_epi16(lanes, _mm_setr_epi16(3600, 60, 1, 0, 0, 0, 0, 0)));
        let seconds = (time as i32 + (time >> 32) as i32) as i64;
        Some(days as i64 * 86_400 + seconds)
    }
}

#[cfg(not(all(target_feature = "ssse3", target_feature = "sse4.1")))]
#[inline(always)]
fn civil_seconds_cached(s: &[u8], _cache: &mut DateCache) -> Option<i64> {
    civil_seconds(s)
}

/// When two UTC timestamps share a day, parse their clocks in parallel and
/// use one date lookup. Other formats and invalid pairs use the ordinary
/// parser so an invalid event can still take part in deduplication.
#[cfg(all(target_feature = "avx2", target_feature = "sse4.1"))]
#[inline(always)]
pub fn utc_pair(event: &[u8], ingest: &[u8], cache: &mut DateCache) -> Option<(i64, i64)> {
    use std::arch::x86_64::*;
    if event.len() != 20 || ingest.len() != 20 || event[19] != b'Z' || ingest[19] != b'Z' {
        return None;
    }
    let a = u64::from_le_bytes(event[..8].try_into().unwrap());
    let b = u32::from_le_bytes(event[8..12].try_into().unwrap()) & 0xff_ffff;
    if a != u64::from_le_bytes(ingest[..8].try_into().unwrap())
        || b != (u32::from_le_bytes(ingest[8..12].try_into().unwrap()) & 0xff_ffff)
    {
        return None;
    }
    let slot = &mut cache.slots[((a ^ ((b as u64) << 40)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 52) as usize];
    if slot.a != a || slot.b != b {
        *slot = Slot { a, b, days: date_days(event)? };
    }
    // Each input has 20 bytes. The loads at offset 4 are inside them.
    unsafe {
        let x = _mm_loadu_si128(event.as_ptr().add(4).cast());
        let y = _mm_loadu_si128(ingest.as_ptr().add(4).cast());
        let v = _mm256_inserti128_si256::<1>(_mm256_castsi128_si256(x), y);
        let pattern = _mm256_broadcastsi128_si256(
            _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 48, 48, 58, 48, 48, 58, 48, 48, 0));
        let limit = _mm256_broadcastsi128_si256(
            _mm_setr_epi8(-1, -1, -1, -1, -1, -1, -1, 9, 9, 0, 9, 9, 0, 9, 9, -1));
        let d = _mm256_sub_epi8(v, pattern);
        let shape = _mm256_cmpeq_epi8(_mm256_min_epu8(d, limit), d);
        let select = _mm256_broadcastsi128_si256(
            _mm_setr_epi8(7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1));
        let units = _mm256_maddubs_epi16(_mm256_shuffle_epi8(d, select), _mm256_set1_epi16(0x010a));
        let max = _mm256_broadcastsi128_si256(_mm_setr_epi16(23, 59, 59, 0, 0, 0, 0, 0));
        let range = _mm256_cmpeq_epi16(_mm256_min_epu16(units, max), units);
        if _mm256_movemask_epi8(_mm256_and_si256(shape, range)) != -1 {
            return None;
        }
        let scale = _mm256_broadcastsi128_si256(_mm_setr_epi16(3600, 60, 1, 0, 0, 0, 0, 0));
        let time = _mm256_madd_epi16(units, scale);
        let add = |lane| {
            let two = _mm_cvtsi128_si64(lane);
            i64::from(two as i32 + (two >> 32) as i32)
        };
        let day = i64::from(slot.days) * DAY;
        let event = day + add(_mm256_castsi256_si128(time)) * 1000;
        let ingest = day + add(_mm256_extracti128_si256::<1>(time)) * 1000;
        Some((event, ingest))
    }
}

#[cfg(not(all(target_feature = "avx2", target_feature = "sse4.1")))]
#[inline(always)]
pub fn utc_pair(_event: &[u8], _ingest: &[u8], _cache: &mut DateCache) -> Option<(i64, i64)> {
    None
}

/// `parse` with the date of F1 and F2 timestamps from `cache`.
#[inline(always)]
pub fn parse_cached(s: &[u8], cache: &mut DateCache) -> Option<i64> {
    match s.len() {
        20 if s[19] == b'Z' => civil_seconds_cached(s, cache).map(|v| v * 1_000),
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let (h, m) = (two(s, 20)?, two(s, 23)?);
            if h > 23 || m > 59 {
                return None;
            }
            let offset = if s[19] == b'+' { h * 60 + m } else { -(h * 60 + m) };
            civil_seconds_cached(s, cache).map(|v| v * 1_000 - offset * 60_000)
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

    #[cfg(all(target_feature = "avx2", target_feature = "sse4.1"))]
    #[test]
    fn paired_utc_clocks_require_valid_matching_dates() {
        use super::{utc_pair, DateCache};

        let mut cache = DateCache::new();
        let ingest = b"2028-02-29T23:59:59Z";
        for event in [b"2028-02-29T00:00:00Z", b"2028-02-29T12:34:56Z", ingest] {
            assert_eq!(utc_pair(event, ingest, &mut cache), parse(event).zip(parse(ingest)));
        }
        for event in [
            b"2028-03-01T00:00:00Z",
            b"2028-02-29T24:00:00Z",
            b"2028-02-29T12:60:00Z",
            b"2028-02-29T12:00:60Z",
            b"2028-02-29T12:00:00z",
            b"2028-02-29T1/:00:00Z",
            b"2028-02-29T12;00:00Z",
        ] {
            assert_eq!(utc_pair(event, ingest, &mut cache), None);
            assert_eq!(utc_pair(ingest, event, &mut cache), None);
        }
        let impossible = b"2028-02-30T12:00:00Z";
        assert_eq!(utc_pair(impossible, impossible, &mut cache), None);
    }


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
        let alphabet = b"0123456789-T:Z+ /a\0\xff\xc3\xba\x80";
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

    /// `parse_cached` agrees with `parse` on mutated timestamps, one cache
    /// shared by all of them (so hits, misses and refills all happen).
    #[test]
    fn cached_matches_parse() {
        use super::{parse_cached, DateCache};
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let seeds: [&[u8]; 10] = [
            b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00Z", b"1900-01-01T00:00:00Z", b"2199-12-31T23:59:59Z",
            b"2026-10-03T19:58:10-04:00", b"1899-12-31T23:59:59+12:00", b"1791071890000", b"2200-03-01T00:00:00Z",
            b"2026-10-03T19:58:10+05:30", b"2027-12-16T22:18:58Z",
        ];
        let alphabet = b"0123456789-T:Z+ /a\0\xff\xc3\xba\x80";
        let mut cache = DateCache::new();
        for round in 0..600_000 {
            let mut s = seeds[round % seeds.len()].to_vec();
            for _ in 0..(next() % 3) {
                let at = (next() as usize) % s.len();
                s[at] = alphabet[(next() as usize) % alphabet.len()];
            }
            if round % 5 == 0 {
                let v = next();
                let text = format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}",
                    1890 + v % 330, (v >> 9) % 14, (v >> 13) % 33, (v >> 19) % 25, (v >> 25) % 61, (v >> 31) % 61,
                    ["Z", "+02:30", "-11:59", "+24:00", "-05:60", "+0200"][(v >> 40) as usize % 6]
                );
                s = text.into_bytes();
            }
            // Few distinct dates, so that slots are hit; some collide.
            if round % 3 == 0 {
                let v = next();
                let text = format!("20{:02}-{:02}-{:02}T{:02}:{:02}:{:02}Z", 20 + v % 4, 1 + (v >> 8) % 12, 1 + (v >> 16) % 28, (v >> 24) % 24, (v >> 32) % 60, (v >> 40) % 60);
                s = text.into_bytes();
            }
            assert_eq!(parse_cached(&s, &mut cache), parse(&s), "{:?}", String::from_utf8_lossy(&s));
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
