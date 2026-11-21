//! String keys. An ID of up to 15 bytes is packed into a u128 whose integer
//! order is the byte-wise order of the strings: the bytes left-aligned and
//! zero-padded, the length in the lowest byte. Longer strings are interned and
//! get a key with 0xFF in the lowest byte, which no packed key has.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Mutex;

pub type Key = u128;

const LONG: u128 = 0xFF;

pub struct Interner {
    inner: Mutex<(HashMap<Vec<u8>, u32>, Vec<Vec<u8>>)>,
}

impl Interner {
    pub fn new() -> Self {
        Interner { inner: Mutex::new((HashMap::new(), Vec::new())) }
    }

    #[inline]
    pub fn key(&self, s: &[u8]) -> Key {
        if s.len() <= 15 {
            let mut b = [0u8; 16];
            b[..s.len()].copy_from_slice(s);
            b[15] = s.len() as u8;
            u128::from_be_bytes(b)
        } else {
            self.long(s)
        }
    }

    #[cold]
    fn long(&self, s: &[u8]) -> Key {
        let mut guard = self.inner.lock().unwrap();
        let (map, strings) = &mut *guard;
        let id = *map.entry(s.to_vec()).or_insert_with(|| {
            strings.push(s.to_vec());
            (strings.len() - 1) as u32
        });
        ((id as u128) << 8) | LONG
    }

    /// The bytes of a key, appended to `out`.
    pub fn bytes(&self, k: Key, out: &mut Vec<u8>) {
        if k & 0xFF == LONG {
            let guard = self.inner.lock().unwrap();
            out.extend_from_slice(&guard.1[(k >> 8) as usize]);
        } else {
            let b = k.to_be_bytes();
            out.extend_from_slice(&b[..b[15] as usize]);
        }
    }

    /// Byte-wise order of the strings behind two keys.
    #[inline]
    pub fn cmp(&self, a: Key, b: Key) -> Ordering {
        if a & 0xFF != LONG && b & 0xFF != LONG {
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
    let x = (k as u64) ^ ((k >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let x = (x ^ (x >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
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
    fn write_u128(&mut self, k: u128) {
        self.0 = hash(k);
    }
}

pub type KeyMap<V> = HashMap<Key, V, BuildHasherDefault<KeyHasher>>;
