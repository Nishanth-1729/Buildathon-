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
pub const ADDRESS: u64 = (1 << 48) - 1;

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
/// Slots an ID may be probed through before it goes to the locked map.
const MAX_PROBES: usize = 64;
const BLOCK: usize = 64 << 10;

thread_local! {
    /// The calling thread's current block: (next free, end).
    static BLOCK_AT: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

/// Long IDs go into an open-addressing table that threads search and add to
/// without locks: a slot is written once, by compare-and-swap, with the
/// address of a record already written. An ID whose first `MAX_PROBES` slots
/// all hold other IDs goes to a locked map instead. A slot never changes once
/// it is written, so that decision is the same for every thread at any time:
/// one ID never gets two keys.
pub struct Interner {
    slots: *const AtomicU64,
    mask: usize,
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
pub fn hash_bytes(s: &[u8]) -> u64 {
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
pub fn record<'a>(address: usize) -> &'a [u8] {
    // Safety: a record published in the table: a u32 length, then bytes.
    unsafe {
        let len = (address as *const u32).read() as usize;
        std::slice::from_raw_parts((address + 4) as *const u8, len)
    }
}

impl Interner {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_capacity(1 << 16)
    }

    /// A table for about `ids` long IDs; more are still correct, but slower.
    pub fn with_capacity(ids: usize) -> Self {
        let n = (ids.max(1 << 16) * 2).next_power_of_two();
        Interner {
            slots: reserve(n * 8) as *const AtomicU64,
            mask: n - 1,
            arena: reserve(ARENA) as usize,
            arena_next: AtomicUsize::new(0),
            locked: Mutex::new(Locked::default()),
        }
    }

    /// Gives every page of the slot table its first touch as a write, on
    /// `threads` threads. The lookups that follow would otherwise read pages
    /// nobody has written yet: the kernel maps its shared zero page for
    /// them, and the first insert into such a page has to unmap that from
    /// every CPU that runs this process (one inter-processor interrupt per
    /// 4 KB or 2 MB page), and leaves the table in small pages. Safe while
    /// other threads use the table: a compare-and-swap of 0 for 0 changes
    /// nothing, whatever the slot holds.
    pub fn prefault(&self, threads: usize) {
        const PAGE_SLOTS: usize = 4096 / 8;
        const STRIP: usize = 64;
        let pages = (self.mask + 1).div_ceil(PAGE_SLOTS);
        let next = AtomicUsize::new(0);
        let work = || loop {
            let first = next.fetch_add(STRIP, Relaxed);
            if first >= pages {
                return;
            }
            for page in first..(first + STRIP).min(pages) {
                // Safety: page * PAGE_SLOTS <= mask, inside the table.
                let slot = unsafe { &*self.slots.add(page * PAGE_SLOTS) };
                let _ = slot.compare_exchange(0, 0, Relaxed, Relaxed);
            }
        };
        std::thread::scope(|scope| {
            for _ in 1..threads.min(pages.div_ceil(STRIP)) {
                scope.spawn(&work);
            }
            work();
        });
    }

    #[inline(always)]
    pub fn key(&self, s: &[u8]) -> Key {
        match Self::pack_short(s) {
            Some(k) => k,
            None => self.intern(s),
        }
    }

    /// The packed key of `s` when it packs (at most nine bytes, none NUL or
    /// above 127): the value `key` returns for such an ID. An ID of 4 to 7
    /// bytes is read as two overlapping 4-byte words, so no byte past it is
    /// touched and no loop runs.
    #[inline(always)]
    pub fn pack_short(s: &[u8]) -> Option<Key> {
        const LOW_BITS: u64 = 0x0101_0101_0101_0101;
        const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
        let n = s.len();
        if (8..=9).contains(&n) {
            let head = u64::from_be_bytes(s[..8].try_into().unwrap());
            let tail = if n == 9 { s[8] } else { 0 };
            let zero_lanes = head.wrapping_sub(LOW_BITS) & !head & !ASCII_LANES;
            if (head | zero_lanes) & !ASCII_LANES == 0 && (n == 8 || (1..128).contains(&tail)) {
                return Some((pack(head) << 7) | tail as u64);
            }
            None
        } else if (4..8).contains(&n) {
            let lo = u32::from_le_bytes(s[..4].try_into().unwrap()) as u64;
            let hi = u32::from_le_bytes(s[n - 4..].try_into().unwrap()) as u64;
            // Byte i of w is s[i]; the bytes past the ID are zero.
            let w = lo | (hi << (8 * (n - 4)));
            // Fill them with 1s for the check: every byte must be 1..=127.
            let c = w | (LOW_BITS & !((1u64 << (8 * n)) - 1));
            if (c.wrapping_sub(LOW_BITS) & !c | c) & HIGH_BITS == 0 {
                Some(pack(w.swap_bytes()) << 7)
            } else {
                None
            }
        } else if n < 4 && s.iter().all(|&b| (1..128).contains(&b)) {
            let mut bytes = [0; 8];
            bytes[..n].copy_from_slice(s);
            Some(pack(u64::from_be_bytes(bytes)) << 7)
        } else {
            None
        }
    }

    /// The key of an ID that is only compared for equality (an event ID): the
    /// packed key when it packs, else a 63-bit hash with the top bit set, so
    /// no table is touched. Never use it with `bytes` or `cmp`. Two different
    /// long IDs share a key with probability about n^2 / 2^64: under 1e-6
    /// for the 3M distinct long IDs of a 60M-event run.
    #[inline(always)]
    pub fn key_hashed(&self, s: &[u8]) -> Key {
        if Self::is_long(s) {
            INTERNED | (hash_bytes(s) & !INTERNED)
        } else {
            self.key(s)
        }
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
        for _ in 0..MAX_PROBES {
            // Safety: i <= mask, inside the table.
            let slot = unsafe { &*self.slots.add(i) };
            let v = slot.load(Acquire);
            if v == 0 {
                if mine == 0 {
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
        self.intern_locked(s)
    }

    /// Copies `s` into the arena as a record; returns its address.
    pub fn copy(&self, s: &[u8]) -> usize {
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
mod pack_tests {
    use super::*;

    /// The definition: at most nine bytes, each 1..=127, seven bits a byte.
    fn reference(s: &[u8]) -> Option<Key> {
        if s.len() > 9 || s.iter().any(|&b| !(1..128).contains(&b)) {
            return None;
        }
        let mut k = 0u64;
        for i in 0..9 {
            k = (k << 7) | s.get(i).copied().unwrap_or(0) as u64;
        }
        Some(k)
    }

    #[test]
    fn pack_short_matches_the_definition() {
        let mut state = 12345u64;
        let mut next = move || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        let alphabets: [&[u8]; 3] = [b"abcxyz019", &[0, 1, 127, 128, 255, b'a'], &[b'a', b'b', 0]];
        for round in 0..400_000 {
            let len = next() % 12;
            let alphabet = alphabets[round % 3];
            let s: Vec<u8> = (0..len).map(|_| alphabet[next() % alphabet.len()]).collect();
            assert_eq!(Interner::pack_short(&s), reference(&s), "{s:?}");
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

#[cfg(test)]
mod hashed_tests {
    use super::Interner;

    #[test]
    fn hashed_event_ids() {
        let ids = Interner::new();
        for s in [&b"u12345678"[..], b"abc", b"e6d3t175y", b""] {
            assert_eq!(ids.key_hashed(s), ids.key(s));
        }
        let mut seen = std::collections::HashSet::new();
        for i in 0..200_000u64 {
            let s = format!("e{:032x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15) as u128 * 0x1_0000_0001);
            let key = ids.key_hashed(s.as_bytes());
            assert_eq!(key, ids.key_hashed(s.as_bytes()));
            assert!(seen.insert(key), "collision at {i}");
        }
    }
}

#[cfg(test)]
mod overfull_tests {
    use super::Interner;

    /// More distinct long IDs than the table has slots, from several threads at
    /// once: every ID still gets one key, the same one on every thread.
    #[test]
    fn one_key_per_id_when_the_table_is_full() {
        let ids = Interner::new();
        let make = |i: usize| format!("user-with-a-long-name-{i:08}").into_bytes();
        let total = 400_000;
        let results: Vec<Vec<u64>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let ids = &ids;
                    s.spawn(move || (0..total).map(|i| ids.key(&make(i))).collect::<Vec<_>>())
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for t in 0..4 {
            for i in 0..total {
                assert_eq!(results[t][i], results[0][i], "id {i} on thread {t}");
            }
        }
        for id in (0..total).step_by(997) {
            let k = ids.key(&make(id));
            assert_eq!(k, results[0][id]);
            let mut got = Vec::new();
            ids.bytes(k, &mut got);
            assert_eq!(got, make(id));
        }
    }
}
