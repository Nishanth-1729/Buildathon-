//! Bucket lists made of fixed-size chunks carved from huge-page slabs.
//!
//! A `Vec` per bucket grows by doubling: every regrowth copies and faults in
//! fresh 4 KB pages. A bucket here only ever appends: when its chunk is full
//! it takes the next 32 KB chunk of its thread's slab, a 32 MB mapping that
//! asks for transparent huge pages. Slab memory is never returned; the
//! process exits when the tables are written.

use std::mem::size_of;
use std::ptr::null_mut;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

const PROT_READ_WRITE: i32 = 3;
const MAP_PRIVATE_ANONYMOUS: i32 = 0x2 | 0x20;
const MADV_HUGEPAGE: i32 = 14;
const HUGE: usize = 2 << 20;
const REFILL: usize = 32 << 20;
/// A multiple of 64, so every chunk starts on a cache line.
pub const CHUNK_BYTES: usize = 32 << 10;

/// One thread's supply of chunks.
pub struct Slab {
    cur: usize,
    end: usize,
}

impl Slab {
    pub fn new() -> Self {
        Slab { cur: 0, end: 0 }
    }

    #[cold]
    fn refill(&mut self) {
        // Safety: an anonymous private mapping; the aligned part is ours.
        unsafe {
            let p = mmap(null_mut(), REFILL + HUGE, PROT_READ_WRITE, MAP_PRIVATE_ANONYMOUS, -1, 0);
            assert!(p as isize != -1, "mmap slab");
            let start = (p as usize + HUGE - 1) & !(HUGE - 1);
            madvise(start as *mut u8, REFILL, MADV_HUGEPAGE);
            self.cur = start;
            self.end = start + REFILL;
        }
    }

    #[inline]
    fn take(&mut self) -> *mut u8 {
        if self.cur + CHUNK_BYTES > self.end {
            self.refill();
        }
        let p = self.cur;
        self.cur += CHUNK_BYTES;
        p as *mut u8
    }
}

/// An append-only list of `T`: full chunks, then one partly filled chunk.
pub struct Bucket<T> {
    chunks: Vec<*mut T>,
    cur: *mut T,
    end: *mut T,
}

// The chunks belong to the bucket alone (slab memory is never reused), and
// are only read through `&Bucket` once appending is over.
unsafe impl<T: Send> Send for Bucket<T> {}
unsafe impl<T: Sync> Sync for Bucket<T> {}

impl<T: Copy> Bucket<T> {
    const CAP: usize = CHUNK_BYTES / size_of::<T>();

    pub fn new() -> Self {
        Bucket { chunks: Vec::new(), cur: null_mut(), end: null_mut() }
    }

    #[inline(always)]
    pub fn push(&mut self, slab: &mut Slab, v: T) {
        if self.cur == self.end {
            self.grow(slab);
        }
        // Safety: cur < end, inside the current chunk.
        unsafe {
            self.cur.write(v);
            self.cur = self.cur.add(1);
        }
    }

    #[cold]
    fn grow(&mut self, slab: &mut Slab) {
        let p = slab.take() as *mut T;
        self.chunks.push(p);
        self.cur = p;
        // Safety: a chunk holds CAP elements.
        self.end = unsafe { p.add(Self::CAP) };
    }

    pub fn len(&self) -> usize {
        match self.chunks.last() {
            None => 0,
            Some(&last) => (self.chunks.len() - 1) * Self::CAP + (self.cur as usize - last as usize) / size_of::<T>(),
        }
    }

    /// The elements, one slice per chunk, in order.
    pub fn slices(&self) -> impl Iterator<Item = &[T]> + Clone + '_ {
        let n = self.chunks.len();
        let cur = self.cur as usize;
        self.chunks.iter().enumerate().map(move |(i, &p)| {
            let len = if i + 1 == n { (cur - p as usize) / size_of::<T>() } else { Self::CAP };
            // Safety: the first `len` elements of the chunk were written.
            unsafe { std::slice::from_raw_parts(p, len) }
        })
    }
}
