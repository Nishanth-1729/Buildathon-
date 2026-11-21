//! Short non-NUL ASCII IDs pack into nine seven-bit lanes in byte order.
//! Other IDs are interned and use the reserved high bit.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Mutex;

pub type Key = u64;

pub const INTERNED: Key = 1 << 63;
const ASCII_LANES: u64 = 0x7f7f_7f7f_7f7f_7f7f;

pub struct Interner {
    inner: Mutex<(HashMap<Vec<u8>, u32>, Vec<Vec<u8>>)>,
}

impl Interner {
    pub fn new() -> Self {
        Interner { inner: Mutex::new((HashMap::new(), Vec::new())) }
    }

    #[inline]
    pub fn key(&self, s: &[u8]) -> Key {
        if (8..=9).contains(&s.len()) {
            let head = u64::from_be_bytes(s[..8].try_into().unwrap());
            let tail = if s.len() == 9 { s[8] } else { 0 };
            const LOW_BITS: u64 = 0x0101_0101_0101_0101;
            let zero_lanes = head.wrapping_sub(LOW_BITS) & !head & !ASCII_LANES;
            if (head | zero_lanes) & !ASCII_LANES == 0 && (s.len() == 8 || (1..128).contains(&tail)) {
                return (pack(head) << 7) | tail as u64;
            }
        } else if s.len() < 8 && s.iter().all(|&b| (1..128).contains(&b)) {
            let mut bytes = [0; 8];
            bytes[..s.len()].copy_from_slice(s);
            return pack(u64::from_be_bytes(bytes)) << 7;
        }
        self.intern(s)
    }

    #[cold]
    fn intern(&self, s: &[u8]) -> Key {
        let mut guard = self.inner.lock().unwrap();
        let (map, strings) = &mut *guard;
        let id = if let Some(&id) = map.get(s) {
            id
        } else {
            let id = strings.len() as u32;
            strings.push(s.to_vec());
            map.insert(s.to_vec(), id);
            id
        };
        INTERNED | id as Key
    }

    /// The bytes of a key, appended to `out`.
    pub fn bytes(&self, k: Key, out: &mut Vec<u8>) {
        if k & INTERNED != 0 {
            let guard = self.inner.lock().unwrap();
            out.extend_from_slice(&guard.1[(k & !INTERNED) as usize]);
        } else {
            let b = unpack(k >> 7).to_be_bytes();
            let len = 8 - (u64::from_be_bytes(b).trailing_zeros() as usize / 8);
            out.extend_from_slice(&b[..len]);
            if k & 127 != 0 {
                out.push((k & 127) as u8);
            }
        }
    }

    /// Byte-wise order of the strings behind two keys.
    #[inline]
    pub fn cmp(&self, a: Key, b: Key) -> Ordering {
        if (a | b) & INTERNED == 0 {
            a.cmp(&b)
        } else {
            self.cmp_slow(a, b)
        }
    }

    #[cold]
    fn cmp_slow(&self, a: Key, b: Key) -> Ordering {
        let (mut x, mut y) = (Vec::new(), Vec::new());
        self.bytes(a, &mut x);
        self.bytes(b, &mut y);
        x.cmp(&y)
    }
}

/// A fast, well-mixed 64-bit hash of a key, for partitioning and hash maps.
#[inline]
pub fn hash(k: Key) -> u64 {
    let x = (k ^ (k >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    let x = (x ^ (x >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    x ^ (x >> 32)
}

#[derive(Default)]
pub struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x51_7C_C1_B7_27_22_0A_95);
        }
    }
    fn write_u64(&mut self, k: u64) {
        self.0 = hash(k);
    }
}

pub type KeyMap<V> = HashMap<Key, V, BuildHasherDefault<KeyHasher>>;

#[inline]
fn pack(word: u64) -> u64 {
    #[cfg(target_feature = "bmi2")]
    // The build target guarantees BMI2.
    return unsafe { std::arch::x86_64::_pext_u64(word, ASCII_LANES) };
    #[cfg(not(target_feature = "bmi2"))]
    word.to_be_bytes().iter().fold(0, |key, &byte| (key << 7) | byte as u64)
}

#[inline]
fn unpack(bits: u64) -> u64 {
    #[cfg(target_feature = "bmi2")]
    // The build target guarantees BMI2.
    return unsafe { std::arch::x86_64::_pdep_u64(bits, ASCII_LANES) };
    #[cfg(not(target_feature = "bmi2"))]
    (0..8).fold(0, |word, i| word | (((bits >> (i * 7)) & 127) << (i * 8)))
}

#[cfg(test)]
mod tests {
    use super::Interner;

    #[test]
    fn round_trip_and_byte_order() {
        let ids = Interner::new();
        let values: &[&[u8]] = &[
            b"", b"\0", b"u", b"u0", b"u\0a", b"u123456", b"u1234567", b"u12345678",
            b"u123456789", b"u1234567\0", b"U12345678", "uÅ".as_bytes(),
        ];
        for &a in values {
            let ka = ids.key(a);
            let mut decoded = Vec::new();
            ids.bytes(ka, &mut decoded);
            assert_eq!(a, decoded);
            for &b in values {
                let kb = ids.key(b);
                assert_eq!(ka == kb, a == b);
                assert_eq!(ids.cmp(ka, kb), a.cmp(b));
            }
        }
    }
}
