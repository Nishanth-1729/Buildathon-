//! Short non-NUL ASCII IDs pack into nine seven-bit lanes in byte order.
//! Other IDs are interned and use the reserved high bit.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Mutex;

pub type Key = u64;

pub const INTERNED: Key = 1 << 63;
const ASCII_LANES: u64 = 0x7f7f_7f7f_7f7f_7f7f;
/// Interned strings live in this many independently locked tables, chosen by
/// the string's hash, so threads interning different IDs rarely meet.
const SHARDS: usize = 64;
const SHARD_BITS: u32 = 6;

#[derive(Default)]
struct Shard {
    ids: HashMap<Box<[u8]>, u64, BuildHasherDefault<BytesHasher>>,
    strings: Vec<Box<[u8]>>,
}

pub struct Interner {
    shards: Vec<Mutex<Shard>>,
}

/// Hashes a byte string eight bytes at a time.
#[derive(Default)]
struct BytesHasher(u64);

impl Hasher for BytesHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(word));
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = hash(self.0 ^ v);
    }
    fn write_usize(&mut self, v: usize) {
        self.write_u64(v as u64);
    }
}

fn shard_of(s: &[u8]) -> usize {
    let mut h = BytesHasher::default();
    h.write(s);
    h.write_usize(s.len());
    (h.finish() >> 58) as usize % SHARDS
}

impl Interner {
    pub fn new() -> Self {
        Interner { shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect() }
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
        let shard = shard_of(s);
        let mut guard = self.shards[shard].lock().unwrap();
        let Shard { ids, strings } = &mut *guard;
        let id = if let Some(&id) = ids.get(s) {
            id
        } else {
            let id = (strings.len() as u64) << SHARD_BITS | shard as u64;
            strings.push(s.into());
            ids.insert(s.into(), id);
            id
        };
        INTERNED | id
    }

    /// The bytes of a key, appended to `out`.
    pub fn bytes(&self, k: Key, out: &mut Vec<u8>) {
        if k & INTERNED != 0 {
            let id = k & !INTERNED;
            let guard = self.shards[(id % SHARDS as u64) as usize].lock().unwrap();
            out.extend_from_slice(&guard.strings[(id >> SHARD_BITS) as usize]);
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
    let product = (k as u128).wrapping_mul(0xa076_1d64_78bd_642f);
    product as u64 ^ (product >> 64) as u64
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

#[cfg(test)]
mod intern_tests {
    use super::Interner;

    /// Threads interning overlapping long IDs agree on every key, and each key
    /// reads back its string; the keys order like the strings.
    #[test]
    fn long_ids_across_threads() {
        let ids = Interner::new();
        let make = |i: usize| format!("u{:0width$}", i * 7919 % 5003, width = 9 + i % 3).into_bytes();
        let results: Vec<Vec<u64>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..4)
                .map(|t| {
                    let ids = &ids;
                    s.spawn(move || (0..5003).map(|i| ids.key(&make((i + t * 1000) % 5003))).collect::<Vec<_>>())
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for t in 0..4 {
            for i in 0..5003 {
                let want = make((i + t * 1000) % 5003);
                assert_eq!(results[t][i], ids.key(&want));
                let mut got = Vec::new();
                ids.bytes(results[t][i], &mut got);
                assert_eq!(got, want);
            }
        }
        let (a, b) = (ids.key(&make(1)), ids.key(&make(2)));
        assert_eq!(ids.cmp(a, b), make(1).cmp(&make(2)));
    }
}
