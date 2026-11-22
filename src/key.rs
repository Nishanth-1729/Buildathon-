//! String keys. An ID of up to 15 bytes is packed into a u128 whose integer
//! order is the byte-wise order of the strings: the bytes left-aligned and
//! zero-padded, the length in the lowest byte. Longer strings are interned in
//! one of 64 shards and get a key with 0xFF in the lowest byte, which no packed
//! key has, and the shard and index above it.

use std::cmp::Ordering;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Mutex;

use ahash::AHasher;
use hashbrown::HashMap;

pub type Key = u128;

const LONG: u128 = 0xFF;

const SHARDS: usize = 64;

struct Shard {
    ids: HashMap<Vec<u8>, u32, BuildHasherDefault<KeyHasher>>,
    strings: Vec<Vec<u8>>,
}

pub struct Interner {
    shards: Vec<Mutex<Shard>>,
}

impl Interner {
    pub fn new() -> Self {
        // PRE-ALLOCATE: Prevents dynamic resizing and rehashing during 60M event ingestion.
        // 150k per shard * 64 shards = 9.6M capacity, enough for all unique IDs.
        let capacity_per_shard = 150_000;
        Interner {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        ids: HashMap::with_capacity_and_hasher(capacity_per_shard, BuildHasherDefault::<KeyHasher>::default()),
                        strings: Vec::with_capacity(capacity_per_shard),
                    })
                })
                .collect(),
        }
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
        let mut hasher = KeyHasher::default();
        hasher.write(s);
        let shard = hasher.finish() as usize % SHARDS;
        let mut guard = self.shards[shard].lock().unwrap();
        let Shard { ids, strings } = &mut *guard;
        let id = match ids.get(s) {
            Some(&id) => id,
            None => {
                strings.push(s.to_vec());
                ids.insert(s.to_vec(), (strings.len() - 1) as u32);
                (strings.len() - 1) as u32
            }
        };
        ((shard as u128) << 40 | (id as u128) << 8) | LONG
    }

    /// The bytes of a key, appended to `out`.
    pub fn bytes(&self, k: Key, out: &mut Vec<u8>) {
        if k & 0xFF == LONG {
            let guard = self.shards[(k >> 40) as usize].lock().unwrap();
            out.extend_from_slice(&guard.strings[(k >> 8) as u32 as usize]);
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
pub struct KeyHasher(AHasher);

impl Hasher for KeyHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
    fn write_u64(&mut self, k: u64) {
        self.0.write_u64(k);
    }
    fn write_u128(&mut self, k: u128) {
        self.0.write_u128(k);
    }
    fn finish(&self) -> u64 {
        self.0.finish()
    }
}

pub type KeyMap<V> = HashMap<Key, V, BuildHasherDefault<KeyHasher>>;
