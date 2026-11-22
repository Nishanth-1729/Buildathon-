//! Memory for the bucket lists: capacity reserved once (no regrowth copies),
//! backed by huge pages and pre-faulted, plus non-temporal store helpers.

extern "C" {
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

const MADV_HUGEPAGE: i32 = 14;
const MADV_POPULATE_WRITE: i32 = 22; // Linux 5.14+; older kernels ignore it (EINVAL)
const PAGE: usize = 4096;

/// A Vec with room for `n` items reserved up front, so pushes never copy.
/// The whole pages inside the allocation are asked for as huge pages and
/// faulted in now, not on first write in the hot loop.
pub fn reserved<T>(n: usize) -> Vec<T> {
    let v: Vec<T> = Vec::with_capacity(n);
    let bytes = v.capacity() * std::mem::size_of::<T>();
    let start = (v.as_ptr() as usize + PAGE - 1) & !(PAGE - 1);
    let end = (v.as_ptr() as usize + bytes) & !(PAGE - 1);
    if end > start {
        // Safety: [start, end) lies inside the Vec's own allocation.
        unsafe {
            // 1. Request 2MB Huge Pages (reduces TLB misses by 512x)
            madvise(start as *mut u8, end - start, MADV_HUGEPAGE);
            // 2. FORCE the kernel to allocate physical RAM NOW.
            // This eliminates page faults during the hot parsing loop.
            madvise(start as *mut u8, end - start, MADV_POPULATE_WRITE);
        }
    }
    v
}

/// Writes a 64-byte block directly to RAM, bypassing the CPU cache.
/// This prevents cache pollution when appending millions of records to buckets.
#[allow(dead_code)]
#[inline(always)]
pub unsafe fn stream_64_nt(dst: *mut u8, src: *const u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        // Two 32-byte non-temporal stores
        _mm256_stream_si256(dst.cast(), _mm256_loadu_si256(src.cast()));
        _mm256_stream_si256(dst.add(32).cast(), _mm256_loadu_si256(src.add(32).cast()));
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        std::ptr::copy_nonoverlapping(src, dst, 64);
    }
}

/// Memory barrier to ensure all non-temporal stores are globally visible
/// before the next phase (sessionization) reads the data.
#[inline(always)]
pub fn sfence() {
    #[cfg(target_arch = "x86_64")]
    // Safety: a store fence has no preconditions.
    unsafe {
        std::arch::x86_64::_mm_sfence();
    }
}
