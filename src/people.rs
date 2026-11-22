//! The people table: one lock-free open-addressing table holding every user
//! ID of the links, events and clicks. A slot's index is the ID's number; the
//! person of an ID is the number of the slot of their name (the byte-wise
//! smallest ID of the linked group), so a person is a plain `u32`.
//!
//! A slot is a 64-bit word that names the ID and a parent link. The word is
//! the packed key of a short ID, or, for any other ID, a marker bit, 15 bits
//! of its hash and the address of its bytes in the interner's arena. The word
//! is written once, by compare-and-swap, after the bytes. Linking is a
//! union-find over the parent links in which the root with the larger name is
//! hung under the other, so a group's root is always its name; once all links
//! are joined `settle` points every slot straight at its root.

use std::ptr::null;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::{AcqRel, Acquire, Relaxed}};

use crate::key::{hash, hash_bytes, record, Interner, Key, ADDRESS, INTERNED};

#[repr(C)]
struct Slot {
    word: AtomicU64,
    /// 0 for a root, else the parent slot + 1.
    parent: AtomicU32,
    _spare: u32,
}

/// Marks the word of an ID that does not pack; packed keys never have it.
const STRING: u64 = 1 << 63;
/// The hash bits kept in the word of a string ID.
const TAG: u64 = 0x7fff << 48;
const SLOT_BYTES: usize = std::mem::size_of::<Slot>();
const HUGE: usize = 2 << 20;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

/// An ID looked at but not yet found: its word and first slot, with that
/// slot's cache line already on its way.
#[derive(Clone, Copy)]
pub struct IdRef {
    word: u64,
    home: u32,
    /// 0 for a packed ID, else the length of the bytes at `ptr`.
    len: u32,
    ptr: *const u8,
}

impl IdRef {
    pub const EMPTY: IdRef = IdRef { word: 0, home: 0, len: 0, ptr: null() };
}

pub struct People {
    slots: *const Slot,
    mask: usize,
    /// IDs the table takes before it counts as full.
    limit: usize,
    count: AtomicUsize,
}

// The slots are only touched through atomics.
unsafe impl Send for People {}
unsafe impl Sync for People {}

impl People {
    /// A table of 2^bits slots.
    pub fn new(bits: u32) -> People {
        let n = 1usize << bits;
        const PROT_READ_WRITE: i32 = 3;
        const MAP_PRIVATE_ANONYMOUS_NORESERVE: i32 = 0x2 | 0x20 | 0x4000;
        const MADV_HUGEPAGE: i32 = 14;
        // Safety: a fresh anonymous mapping, zero-filled on first touch; zero is an empty slot.
        let slots = unsafe {
            let p = mmap(std::ptr::null_mut(), n * SLOT_BYTES + HUGE, PROT_READ_WRITE, MAP_PRIVATE_ANONYMOUS_NORESERVE, -1, 0);
            assert!(p as isize != -1, "mmap people table");
            let start = (p as usize + HUGE - 1) & !(HUGE - 1);
            madvise(start as *mut u8, n * SLOT_BYTES, MADV_HUGEPAGE);
            start as *const Slot
        };
        People { slots, mask: n - 1, limit: n / 4 * 3, count: AtomicUsize::new(0) }
    }

    /// Address and size in bytes of the table.
    pub fn extent(&self) -> (usize, usize) {
        (self.slots as usize, self.capacity() * SLOT_BYTES)
    }

    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Number of 2 MB pieces of the table, for `populate_piece`.
    pub fn pieces(&self) -> usize {
        (self.capacity() * SLOT_BYTES).div_ceil(HUGE)
    }

    /// Faults in the whole table on `threads` threads. Every thread exists
    /// before the first fault: while some threads fault, the kernel holds the
    /// address space against a thread being created, which would then wait for
    /// the whole table. The threads are started here, not by `run`, for that.
    pub fn populate(&self, threads: usize) {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let (go, next) = (AtomicBool::new(false), AtomicUsize::new(0));
        let n = self.pieces();
        let work = || {
            while !go.load(Relaxed) {
                std::hint::spin_loop();
            }
            loop {
                let i = next.fetch_add(1, Relaxed);
                if i >= n {
                    return;
                }
                self.populate_piece(i);
            }
        };
        std::thread::scope(|scope| {
            for _ in 1..threads.min(n) {
                scope.spawn(&work);
            }
            go.store(true, Relaxed);
            work();
        });
    }

    /// Touches piece `i` with a write, so that the kernel backs it with one
    /// huge page now: a page first read maps the shared zero page, and its
    /// first write would then split the huge range into small pages.
    pub fn populate_piece(&self, i: usize) {
        // Safety: the slot is inside the table; a zero stored over a zero leaves it empty.
        unsafe { (*self.slots.add(i * HUGE / SLOT_BYTES)).parent.store(0, Relaxed) };
    }

    #[inline(always)]
    fn slot(&self, i: usize) -> &Slot {
        // Safety: callers keep i <= mask.
        unsafe { &*self.slots.add(i) }
    }

    #[inline(always)]
    fn prefetch(&self, home: u32) {
        #[cfg(target_arch = "x86_64")]
        // Safety: a prefetch of an address inside the table.
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.add(home as usize).cast::<i8>(), _MM_HINT_T0);
        }
    }

    /// Looks at an ID (never empty): its word and first slot; asks for the slot.
    #[inline(always)]
    pub fn locate(&self, s: &[u8]) -> IdRef {
        match Interner::pack_short(s) {
            Some(k) => {
                let home = (hash(k) as usize & self.mask) as u32;
                self.prefetch(home);
                IdRef { word: k, home, len: 0, ptr: null() }
            }
            None => {
                let h = hash_bytes(s);
                let home = (h as usize & self.mask) as u32;
                self.prefetch(home);
                IdRef { word: STRING | (h & TAG), home, len: s.len() as u32, ptr: s.as_ptr() }
            }
        }
    }

    /// Some time after `locate`: if the slot already holds a string ID, asks
    /// for its bytes, which the comparison in `resolve` will read.
    #[inline(always)]
    pub fn touch(&self, r: &IdRef) {
        if r.len != 0 {
            let w = self.slot(r.home as usize).word.load(Relaxed);
            if w & STRING != 0 {
                #[cfg(target_arch = "x86_64")]
                // Safety: prefetches are hints and never fault.
                unsafe {
                    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                    let a = (w & ADDRESS) as *const i8;
                    _mm_prefetch(a, _MM_HINT_T0);
                    _mm_prefetch(a.wrapping_add(40), _MM_HINT_T0);
                }
            }
        }
    }

    /// The slot of the ID, added if it is new. `ins` counts this thread's
    /// additions since it last told the table.
    #[inline(always)]
    pub fn resolve(&self, ids: &Interner, r: &IdRef, ins: &mut usize) -> u32 {
        if r.len == 0 {
            self.resolve_packed(r, ins)
        } else {
            self.resolve_string(ids, r, ins)
        }
    }

    #[inline(always)]
    fn resolve_packed(&self, r: &IdRef, ins: &mut usize) -> u32 {
        let mut i = r.home as usize;
        loop {
            let slot = self.slot(i);
            let w = slot.word.load(Relaxed);
            if w == r.word {
                return i as u32;
            }
            if w == 0 {
                match slot.word.compare_exchange(0, r.word, Relaxed, Relaxed) {
                    Ok(_) => {
                        self.added(ins);
                        return i as u32;
                    }
                    Err(won) if won == r.word => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(never)]
    fn resolve_string(&self, ids: &Interner, r: &IdRef, ins: &mut usize) -> u32 {
        // Safety: ptr/len are the bytes `locate` was given; they outlive this call.
        let s = unsafe { std::slice::from_raw_parts(r.ptr, r.len as usize) };
        let mut i = r.home as usize;
        let mut mine = 0usize;
        loop {
            let slot = self.slot(i);
            let w = slot.word.load(Acquire);
            if w == 0 {
                if mine == 0 {
                    mine = ids.copy(s);
                }
                match slot.word.compare_exchange(0, r.word | mine as u64, AcqRel, Acquire) {
                    Ok(_) => {
                        self.added(ins);
                        return i as u32;
                    }
                    Err(won) => {
                        if won & (STRING | TAG) == r.word && record((won & ADDRESS) as usize) == s {
                            return i as u32;
                        }
                    }
                }
            } else if w & (STRING | TAG) == r.word && record((w & ADDRESS) as usize) == s {
                return i as u32;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Counts an addition; a table three quarters full cannot go on.
    #[inline(always)]
    fn added(&self, ins: &mut usize) {
        *ins += 1;
        if *ins == 256 {
            *ins = 0;
            if self.count.fetch_add(256, Relaxed) + 256 > self.limit {
                crate::fast::fallback("the people table is full");
            }
        }
    }

    /// IDs added so far, to within the threads' unreported additions.
    pub fn len(&self) -> usize {
        self.count.load(Relaxed)
    }

    /// The key of the ID in `slot`, as `Interner::bytes` and `cmp` read it.
    #[inline]
    pub fn key_of(&self, slot: u32) -> Key {
        let w = self.slot(slot as usize).word.load(Acquire);
        if w & STRING != 0 {
            INTERNED | (w & ADDRESS)
        } else {
            w
        }
    }

    /// The root above `x`; every slot on the way is pointed at its
    /// grandparent, which keeps later searches short.
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.slot(x as usize).parent.load(Acquire);
            if p == 0 {
                return x;
            }
            let g = self.slot(p as usize - 1).parent.load(Acquire);
            if g != 0 {
                let _ = self.slot(x as usize).parent.compare_exchange(p, g, AcqRel, Relaxed);
            }
            x = p - 1;
        }
    }

    /// Joins the groups of two slots. The root with the larger name goes
    /// under the other: every step up is to a smaller name, so no cycle can
    /// form, and a group's root is its smallest name.
    pub fn join(&self, ids: &Interner, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        loop {
            a = self.find(a);
            b = self.find(b);
            if a == b {
                return;
            }
            let (child, root) = if ids.cmp(self.key_of(b), self.key_of(a)).is_lt() { (a, b) } else { (b, a) };
            if self.slot(child as usize).parent.compare_exchange(0, root + 1, AcqRel, Acquire).is_ok() {
                return;
            }
        }
    }

    /// Asks for the parent slot of slot `i`, for `join` a few IDs later.
    #[inline(always)]
    pub fn prefetch_parent(&self, i: u32) {
        let p = self.slot(i as usize).parent.load(Relaxed);
        if p != 0 {
            self.prefetch(p - 1);
        }
    }

    /// Once no more joins happen: points every linked slot in `lo..hi` at its root.
    pub fn settle(&self, lo: usize, hi: usize) {
        const AHEAD: usize = 24;
        for i in lo..hi {
            if i + AHEAD < hi {
                self.prefetch_parent((i + AHEAD) as u32);
            }
            if self.slot(i).parent.load(Relaxed) != 0 {
                let root = self.find(i as u32);
                self.slot(i).parent.store(root + 1, Relaxed);
            }
        }
    }

    /// The person of the ID in `slot`, after `settle`.
    #[inline(always)]
    pub fn person(&self, slot: u32) -> u32 {
        match self.slot(slot as usize).parent.load(Relaxed) {
            0 => slot,
            p => p - 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lcg(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    /// IDs of every kind, joined from several threads in a random order: after
    /// `settle` each one's person is the slot of the smallest ID of its group.
    #[test]
    fn groups_are_named_by_their_smallest_id() {
        let ids = Interner::new();
        let people = People::new(16);
        let make = |n: u32| -> Vec<u8> {
            let x = n.wrapping_mul(2654435761) % 1_000_000;
            match n % 4 {
                0 => format!("u{x:06}").into_bytes(),
                1 => format!("u{x:06}{:032x}", x as u128 * 0x1_0000_0001).into_bytes(),
                2 => format!("Z{x}").into_bytes(),
                _ => format!("é{x:05}").into_bytes(),
            }
        };
        let mut rng = 5u64;
        let mut links = Vec::new();
        let mut groups: Vec<Vec<u32>> = Vec::new();
        let mut next = 0u32;
        for g in 0..1500u32 {
            let len = 1 + (lcg(&mut rng) % 5) as u32 + if g % 300 == 0 { 120 } else { 0 };
            let members: Vec<u32> = (0..len).map(|_| { next += 1; next }).collect();
            let mut shuffled = members.clone();
            for i in (1..shuffled.len()).rev() {
                shuffled.swap(i, (lcg(&mut rng) % (i as u64 + 1)) as usize);
            }
            for w in shuffled.windows(2) {
                links.push((w[0], w[1]));
            }
            groups.push(members);
        }
        for i in (1..links.len()).rev() {
            links.swap(i, (lcg(&mut rng) % (i as u64 + 1)) as usize);
        }
        let slot_of = |ins: &mut usize, n: u32| {
            let b = make(n);
            people.resolve(&ids, &people.locate(&b), ins)
        };
        std::thread::scope(|s| {
            for part in links.chunks(links.len() / 4 + 1) {
                let (people, ids) = (&people, &ids);
                s.spawn(move || {
                    let mut ins = 0;
                    for &(a, b) in part {
                        let (x, y) = (people.resolve(ids, &people.locate(&make(a)), &mut ins), people.resolve(ids, &people.locate(&make(b)), &mut ins));
                        people.join(ids, x, y);
                    }
                });
            }
        });
        people.settle(0, people.capacity());
        let mut ins = 0;
        for members in &groups {
            let name = members.iter().map(|&m| make(m)).min().unwrap();
            for &m in members {
                let slot = slot_of(&mut ins, m);
                let person = people.person(slot);
                let mut got = Vec::new();
                ids.bytes(people.key_of(person), &mut got);
                assert_eq!(got, name);
            }
        }
        // An ID that no link mentions is its own person.
        let lone = slot_of(&mut ins, 4_000_000_001);
        assert_eq!(people.person(lone), lone);
        assert_eq!(people.resolve(&ids, &people.locate(&make(4_000_000_001)), &mut ins), lone);
    }
}
