//! Short non-NUL ASCII IDs pack into nine seven-bit lanes in byte order.
//! Other IDs are interned and use the reserved high bit.

use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::{Acquire, Relaxed, Release}};
use std::sync::Mutex;

pub type Key = u64;

pub const INTERNED: Key = 1 << 63;
const ASCII_LANES: u64 = 0x7f7f_7f7f_7f7f_7f7f;
/// Marks an ID interned by the locked fallback table rather than the
/// lock-free one; the rest of the key is its index there.
const LOCKED: Key = 1 << 62;
/// Low 48 bits of a slot: the address of the record; high 16: a hash tag.
const ADDRESS: u64 = (1 << 48) - 1;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

fn reserve(bytes: usize) -> *mut u8 {
    const PROT_READ_WRITE: i32 = 3;
    const MAP_PRIVATE_ANONYMOUS_NORESERVE: i32 = 0x2 | 0x20 | 0x4000;
    const MADV_HUGEPAGE: i32 = 14;
    // Safety: a fresh anonymous mapping, zero-filled on first touch.
    unsafe {
        let p = mmap(std::ptr::null_mut(), bytes, PROT_READ_WRITE, MAP_PRIVATE_ANONYMOUS_NORESERVE, -1, 0);
        assert!(p as isize != -1, "mmap interner");
        madvise(p, bytes, MADV_HUGEPAGE);
        p
    }
}

/// Where interned strings are copied: one reserved range, handed out to
/// threads in blocks. A record is a u32 length and the bytes; records are
/// 4-byte aligned and never move, so a key can hold the address.
const ARENA: usize = 64 << 30;
const BLOCK: usize = 64 << 10;

thread_local! {
    /// The calling thread's current block: (next free, end).
    static BLOCK_AT: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    /// Inserts the calling thread may still make before counting again.
    static INSERTS: Cell<usize> = const { Cell::new(0) };
}

/// Long IDs go into an open-addressing table that threads search and add to
/// without locks: a slot is written once, by compare-and-swap, with the
/// address of a record already written. When the table gets crowded new
/// IDs go to a locked map instead; IDs already in the table are still found
/// there, as a probe only stops at an empty slot.
pub struct Interner {
    slots: *const AtomicU64,
    mask: usize,
    /// Inserts allowed into `slots` before it counts as crowded.
    room: usize,
    used: AtomicUsize,
    arena: usize,
    arena_next: AtomicUsize,
    locked: Mutex<Locked>,
}

// The slots and arena are shared through atomics; records are immutable
// once published.
unsafe impl Send for Interner {}
unsafe impl Sync for Interner {}

#[derive(Default)]
struct Locked {
    ids: HashMap<Box<[u8]>, u64, BuildHasherDefault<BytesHasher>>,
    strings: Vec<Box<[u8]>>,
}

/// Hashes a byte string eight bytes at a time.
#[derive(Default)]
struct BytesHasher(u64);

impl Hasher for BytesHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0 = hash_bytes(bytes);
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = hash(self.0 ^ v);
    }
    fn write_usize(&mut self, v: usize) {
        self.write_u64(v as u64);
    }
}

#[inline]
fn hash_bytes(s: &[u8]) -> u64 {
    let mut h = s.len() as u64;
    let mut words = s.chunks_exact(8);
    for w in &mut words {
        h = hash(h ^ u64::from_le_bytes(w.try_into().unwrap()));
    }
    let rest = words.remainder();
    if !rest.is_empty() {
        let mut word = [0u8; 8];
        word[..rest.len()].copy_from_slice(rest);
        h = hash(h ^ u64::from_le_bytes(word));
    }
    h
}

/// The bytes of the record at `address`.
#[inline]
fn record<'a>(address: usize) -> &'a [u8] {
    // Safety: a record published in the table: a u32 length, then bytes.
    unsafe {
        let len = (address as *const u32).read() as usize;
        std::slice::from_raw_parts((address + 4) as *const u8, len)
    }
}

impl Interner {
    pub fn new() -> Self {
        Self::with_capacity(1 << 16)
    }

    /// A table for about `ids` long IDs before it counts as crowded.
    pub fn with_capacity(ids: usize) -> Self {
        let n = (ids.max(1 << 16) * 2).next_power_of_two();
        Interner {
            slots: reserve(n * 8) as *const AtomicU64,
            mask: n - 1,
            room: n / 4 * 3 - 64 * 1024,
            used: AtomicUsize::new(0),
            arena: reserve(ARENA) as usize,
            arena_next: AtomicUsize::new(0),
            locked: Mutex::new(Locked::default()),
        }
    }

    #[inline(always)]
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

    /// True for an ID that `key` looks up in the table: a long one.
    #[inline(always)]
    pub fn is_long(s: &[u8]) -> bool {
        s.len() > 9
    }

    /// Starts loading the table slot of a long ID, for a `key` soon after.
    #[inline(always)]
    pub fn prefetch(&self, s: &[u8]) {
        let i = hash_bytes(s) as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        // Safety: a prefetch of an address inside the table.
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.add(i) as *const i8, _MM_HINT_T0);
        }
    }

    #[inline(never)]
    fn intern(&self, s: &[u8]) -> Key {
        let h = hash_bytes(s);
        let tag = h & !ADDRESS;
        let mut i = h as usize & self.mask;
        let mut mine = 0usize;
        loop {
            // Safety: i <= mask, inside the table.
            let slot = unsafe { &*self.slots.add(i) };
            let v = slot.load(Acquire);
            if v == 0 {
                if mine == 0 {
                    if self.crowded() {
                        return self.intern_locked(s);
                    }
                    mine = self.copy(s);
                }
                match slot.compare_exchange(0, tag | mine as u64, Release, Acquire) {
                    Ok(_) => return INTERNED | mine as u64,
                    Err(won) => {
                        if won & !ADDRESS == tag && record((won & ADDRESS) as usize) == s {
                            // The copy is left unused.
                            return INTERNED | (won & ADDRESS);
                        }
                    }
                }
            } else if v & !ADDRESS == tag && record((v & ADDRESS) as usize) == s {
                return INTERNED | (v & ADDRESS);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Counts an insert; true when the table is too full to take it.
    /// Threads count in batches, so the table can overshoot `room` by a
    /// batch per thread, which the slack above `room` absorbs.
    #[inline]
    fn crowded(&self) -> bool {
        const BATCH: usize = 1024;
        INSERTS.with(|n| {
            let left = n.get();
            if left != 0 {
                n.set(left - 1);
                return false;
            }
            if self.used.fetch_add(BATCH, Relaxed) >= self.room {
                return true;
            }
            n.set(BATCH - 1);
            false
        })
    }

    /// Copies `s` into the arena as a record; returns its address.
    fn copy(&self, s: &[u8]) -> usize {
        let need = (4 + s.len() + 3) & !3;
        let at = BLOCK_AT.with(|b| {
            let (mut next, mut end) = b.get();
            if next + need > end {
                let size = need.max(BLOCK);
                let off = self.arena_next.fetch_add(size, Relaxed);
                assert!(off + size <= ARENA, "interner arena full");
                next = self.arena + off;
                end = next + size;
            }
            b.set((next + need, end));
            next
        });
        // Safety: [at, at + need) was handed to this thread alone.
        unsafe {
            (at as *mut u32).write(s.len() as u32);
            std::ptr::copy_nonoverlapping(s.as_ptr(), (at + 4) as *mut u8, s.len());
        }
        at
    }

    #[cold]
    fn intern_locked(&self, s: &[u8]) -> Key {
        let mut guard = self.locked.lock().unwrap();
        let Locked { ids, strings } = &mut *guard;
        let id = if let Some(&id) = ids.get(s) {
            id
        } else {
            let id = strings.len() as u64;
            strings.push(s.into());
            ids.insert(s.into(), id);
            id
        };
        INTERNED | LOCKED | id
    }

    /// Calls `f` with the bytes of `k`.
    #[inline]
    fn with_bytes<R>(&self, k: Key, f: impl FnOnce(&[u8]) -> R) -> R {
        if k & INTERNED == 0 {
            let b = unpack(k >> 7).to_be_bytes();
            let len = 8 - (u64::from_be_bytes(b).trailing_zeros() as usize / 8);
            let mut out = [0u8; 9];
            out[..len].copy_from_slice(&b[..len]);
            let mut n = len;
            if k & 127 != 0 {
                out[n] = (k & 127) as u8;
                n += 1;
            }
            f(&out[..n])
        } else if k & LOCKED == 0 {
            f(record((k & ADDRESS) as usize))
        } else {
            let guard = self.locked.lock().unwrap();
            f(&guard.strings[(k & !(INTERNED | LOCKED)) as usize])
        }
    }

    pub fn bytes(&self, k: Key, out: &mut Vec<u8>) {
        self.with_bytes(k, |b| out.extend_from_slice(b));
    }

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
        let locked = |k: Key| k & (INTERNED | LOCKED) == INTERNED | LOCKED;
        if locked(a) && locked(b) {
            let (mut x, mut y) = (Vec::new(), Vec::new());
            self.bytes(a, &mut x);
            self.bytes(b, &mut y);
            return x.cmp(&y);
        }
        self.with_bytes(a, |x| self.with_bytes(b, |y| x.cmp(y)))
    }
}

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
