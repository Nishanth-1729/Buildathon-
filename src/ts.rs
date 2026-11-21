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
    for &b in s {
        let d = digit(b)?;
        v = v.checked_mul(10)?.checked_add(d)?;
    }
    Some(v)
}

/// Epoch milliseconds of F1, F2 or F3, or None for a bad timestamp. An F3 value
/// too large for i64 is None too: it is outside every window anyway.
#[inline]
pub fn parse(s: &[u8]) -> Option<i64> {
    let offset_minutes = match s.len() {
        20 if s[19] == b'Z' => 0,
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let (h, m) = (two(s, 20)?, two(s, 23)?);
            if h > 23 || m > 59 {
                return None;
            }
            if s[19] == b'+' { h * 60 + m } else { -(h * 60 + m) }
        }
        _ => return digits(s),
    };
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = two(s, 0)? * 100 + two(s, 2)?;
    let (month, day) = (two(s, 5)?, two(s, 8)?);
    let (hour, minute, second) = (two(s, 11)?, two(s, 14)?, two(s, 17)?);
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    Some(seconds * 1_000 - offset_minutes * 60_000)
}

#[cfg(test)]
mod tests {
    use super::parse;

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
