//! The one-shuffle pipeline.
//!
//! A person is a slot number of the people table (`people.rs`), so an event
//! can be sent to its person's partition the moment it is parsed:
//!
//! 1. Links: every link ID goes into the table and the two IDs of a line are
//!    joined; then every slot is pointed at the root of its group, the name.
//! 2. Events and clicks, from one queue of chunks. A valid event becomes a
//!    12-byte record in the partition of its person; every event, valid or
//!    not, also leaves a 16-byte entry (event key, ingest second, record
//!    number) in a bucket chosen by its event key. Clicks go to partitions.
//! 3. Per bucket of entries: the keys that occur twice are found with a bit
//!    filter and sorted; all copies but the earliest ingested are marked
//!    dropped in their records, wherever they were sent.
//! 4. Per person partition: counting sort by person, sessions, credit,
//!    assists. Then both tables are written.
//!
//! Anything outside this layout's limits restarts the process on the general
//! pipeline (`fallback`): nothing has been written by then.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Instant;

use super::*;
use crate::people::{IdRef, People};

/// Link chunks are small: the links are a few percent of the input and the
/// work on them is cache misses, so every thread should get a share.
const LINK_CHUNK: usize = 256 << 10;
/// Event and click chunks, read into a buffer of that size per thread: small
/// enough that the bytes pread just copied are still in the core caches when
/// they are scanned (4 MB chunks, as the mapped input had, measured 8% slower).
const READ_CHUNK: usize = 512 << 10;
/// `ERec::person` of a copy that dedup found to be later than another.
const DROPPED: u32 = u32::MAX;
/// Lookups in flight per thread while events and clicks wait for their slots.
const RING: usize = 16;

/// A valid event in the partition of its person.
#[derive(Clone, Copy)]
#[repr(C)]
struct ERec {
    person: u32,
    ts: u32,
    pc: u32,
}

/// One parsed event line in its bucket: `loc` is 1 + the offset of its `ERec`
/// from the slab region, in 4-byte units; 0 for an invalid or late event, which
/// still takes part in dedup. `ingest` is whole seconds after `base`.
#[derive(Clone, Copy)]
#[repr(C)]
struct LRec {
    key: u64,
    ingest: u32,
    loc: u32,
}

/// A valid click in the partition of its person.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    cid: Key,
    person: u32,
    ts: u32,
    campaign: u32,
    _spare: u32,
}

struct Cx<'a> {
    lk: &'a Lookups,
    people: People,
    region: usize,
    /// person >> pshift is the person's partition.
    pshift: u32,
    /// hash(event key) >> qshift is the event key's bucket.
    qshift: u32,
}

impl Cx<'_> {
    #[inline(always)]
    fn loc(&self, rec: *mut ERec) -> u32 {
        let units = (rec as usize - self.region) >> 2;
        if units >= u32::MAX as usize {
            fallback("more records than the 32-bit record numbers cover");
        }
        units as u32 + 1
    }

    #[inline(always)]
    fn rec(&self, loc: u32) -> *mut ERec {
        (self.region + ((loc as usize - 1) << 2)) as *mut ERec
    }

    #[inline(always)]
    fn bucket(&self, key: Key) -> usize {
        (hash(key) >> self.qshift) as usize
    }
}

/// Restarts the process on the general pipeline: for the rare input this
/// layout cannot hold, found before any output is written.
#[cold]
pub fn fallback(why: &str) -> ! {
    use std::os::unix::process::CommandExt;
    static ONCE: AtomicBool = AtomicBool::new(false);
    if ONCE.swap(true, Relaxed) {
        loop {
            std::thread::park(); // another thread is already restarting
        }
    }
    eprintln!("{why}: restarting on the general pipeline");
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from(std::env::args_os().next().unwrap_or_default()));
    let err = std::process::Command::new(exe).args(std::env::args_os().skip(1)).env("SESSIONS_GENERAL", "1").exec();
    panic!("exec: {err}");
}

/// An event or click waiting for its person's slot.
#[derive(Clone, Copy)]
struct Pend {
    /// Event key, or click ID key.
    a: Key,
    id: IdRef,
    ts: u32,
    /// `pc` of an event, or the campaign of a click.
    x: u32,
    ingest: u32,
}

/// One parse thread's output.
struct Shard {
    dates: ts::DateCache,
    slab: Slab,
    ev: Vec<Bucket<ERec>>,
    ck: Vec<Bucket<CRec>>,
    lg: Vec<Bucket<LRec>>,
    ring: [Pend; RING],
    head: usize,
    ins: usize,
}

// The buckets own their chunks; after the parse they are only read, except for
// `ERec::person` of distinct records, written by the dedup pass alone.
unsafe impl Send for Shard {}
unsafe impl Sync for Shard {}

impl Shard {
    fn new(np: usize, nq: usize) -> Shard {
        let empty = Pend { a: 0, id: IdRef::EMPTY, ts: 0, x: 0, ingest: 0 };
        Shard {
            dates: ts::DateCache::new(),
            slab: Slab::new(),
            ev: (0..np).map(|_| Bucket::new()).collect(),
            ck: (0..np).map(|_| Bucket::new()).collect(),
            lg: (0..nq).map(|_| Bucket::new()).collect(),
            ring: [empty; RING],
            head: 0,
            ins: 0,
        }
    }

    /// Queues `p` until its slot has been loaded; finishes the one queued
    /// RING entries ago. Half way, the bytes of a string ID are asked for.
    #[inline(always)]
    fn push<const EVENT: bool>(&mut self, cx: &Cx, p: Pend) {
        let i = self.head & (RING - 1);
        if self.head >= RING {
            let old = self.ring[i];
            self.finish::<EVENT>(cx, &old);
        }
        if self.head >= RING / 2 {
            cx.people.touch(&self.ring[(self.head - RING / 2) & (RING - 1)].id);
        }
        self.ring[i] = p;
        self.head += 1;
    }

    /// Finishes what is still queued; at the end of every chunk.
    fn drain<const EVENT: bool>(&mut self, cx: &Cx) {
        for k in self.head.saturating_sub(RING)..self.head {
            let p = self.ring[k & (RING - 1)];
            self.finish::<EVENT>(cx, &p);
        }
        self.head = 0;
    }

    #[inline(always)]
    fn finish<const EVENT: bool>(&mut self, cx: &Cx, p: &Pend) {
        let slot = cx.people.resolve(&cx.lk.ids, &p.id, &mut self.ins);
        let person = cx.people.person(slot);
        let part = (person >> cx.pshift) as usize;
        if EVENT {
            let rec = self.ev[part].push(&mut self.slab, ERec { person, ts: p.ts, pc: p.x });
            let loc = cx.loc(rec);
            self.log(cx, p.a, p.ingest, loc);
        } else {
            self.ck[part].push(&mut self.slab, CRec { cid: p.a, person, ts: p.ts, campaign: p.x, _spare: 0 });
        }
    }

    #[inline(always)]
    fn log(&mut self, cx: &Cx, key: Key, ingest: u32, loc: u32) {
        self.lg[cx.bucket(key)].push(&mut self.slab, LRec { key, ingest, loc });
    }
}

/// Whole seconds from `base`; u32::MAX for an ingest_ts that did not parse.
#[inline(always)]
fn ingest_seconds(ingest: i64, base: i64) -> u32 {
    if ingest == i64::MAX {
        return u32::MAX;
    }
    let d = ingest.wrapping_sub(base);
    if d < 0 || d >= u32::MAX as i64 * 1000 || d % 1000 != 0 {
        fallback("an ingest_ts outside the layout of the compact entries");
    }
    (d / 1000) as u32
}

/// rules.md "Events" 3 and 4 for a parsed line: the time after `base` and
/// `pc` of a valid, not late event.
#[inline(always)]
fn event_payload(f: &[&[u8]; 7], time: Option<i64>, ingest: i64, lk: &Lookups) -> Option<(u32, u32)> {
    if f[1].is_empty() || f[5].is_empty() {
        return None;
    }
    let ts = time?;
    if ts < lk.window_start || ts >= lk.window_end || ingest.saturating_sub(ts) > DAY {
        return None;
    }
    // `view` and `cart` are told apart from the rest without a branch on
    // which of the two it is: that is a coin toss in the data.
    let word = if f[4].len() >= 4 { u32::from_le_bytes([f[4][0], f[4][1], f[4][2], f[4][3]]) } else { 0 };
    let view_or_cart = (f[4].len() == 4) & ((word == u32::from_le_bytes(*b"view")) | (word == u32::from_le_bytes(*b"cart")));
    let pc = match f[4] {
        _ if view_or_cart => 0,
        b"purchase" => {
            let price = match ts::digits(f[6]) {
                Some(p) if (1..=10_000_000).contains(&p) => p as u32,
                _ => return None,
            };
            let category = *lk.products.get(&lk.ids.key(f[5])).unwrap_or(&lk.unknown);
            price | (category as u32) << 24
        }
        _ => return None,
    };
    Some(((ts - lk.base) as u32, pc))
}

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], cx: &Cx, st: &mut Shard) {
    if f[0].is_empty() {
        return;
    }
    let lk = cx.lk;
    let (time, ingest) = match ts::utc_pair(f[2], f[3], &mut st.dates) {
        Some((time, ingest)) => (Some(time), ingest),
        None => (ts::parse_cached(f[2], &mut st.dates), ts::parse_cached(f[3], &mut st.dates).unwrap_or(i64::MAX)),
    };
    let ingest_s = ingest_seconds(ingest, lk.base);
    let key = lk.ids.key_hashed(f[0]);
    match event_payload(f, time, ingest, lk) {
        Some((ts, pc)) => {
            let id = cx.people.locate(f[1]);
            st.push::<true>(cx, Pend { a: key, id, ts, x: pc, ingest: ingest_s });
        }
        None => st.log(cx, key, ingest_s, 0),
    }
}

#[inline(always)]
fn parse_click(f: &[&[u8]; 4], cx: &Cx, st: &mut Shard) {
    if f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
        return;
    }
    let lk = cx.lk;
    let Some(ts) = ts::parse_cached(f[3], &mut st.dates) else { return };
    if ts < lk.base || ts >= lk.window_end {
        return;
    }
    let campaign = match campaign_number(f[2]).and_then(|n| lk.campaign_table.get(n)) {
        Some(&slot) if slot != 0 => slot - 1,
        // Not of the form `c_123`, or not a campaign: the exact map decides.
        _ => match lk.campaigns.get(&lk.ids.key(f[2])) {
            Some(&campaign) => campaign,
            None => return,
        },
    };
    let id = cx.people.locate(f[1]);
    st.push::<false>(cx, Pend { a: click_key(&lk.ids, f[0]), id, ts: (ts - lk.base) as u32, x: campaign, ingest: 0 });
}

/// The key of a click ID: its packed key when it packs, else a pointer to a
/// copy of its bytes in the interner's arena: INTERNED, the length in bits
/// 48..56 and the address below. (A pointer into the click file would fault in
/// the whole file, 64 KB around every long ID that is compared, and its pages
/// would have to be freed at the exit.) Two copies of one click then have different keys, so
/// `click_id_eq` and `click_id_cmp` compare such keys by their bytes; that is
/// only needed for clicks of one person at one timestamp.
#[inline(always)]
fn click_key(ids: &Interner, s: &[u8]) -> Key {
    match Interner::pack_short(s) {
        Some(k) => k,
        None => {
            if s.len() > 255 {
                fallback("a click ID of more than 255 bytes");
            }
            // The record is a u32 length, then the bytes.
            INTERNED | (s.len() as u64) << 48 | (ids.copy(s) + 4) as u64
        }
    }
}

/// The bytes of a click key, handed to `f`.
fn with_click_bytes<R>(ids: &Interner, k: Key, f: impl FnOnce(&[u8]) -> R) -> R {
    if k & INTERNED == 0 {
        let mut v = Vec::new();
        ids.bytes(k, &mut v);
        f(&v)
    } else {
        // Safety: made by `click_key` from bytes copied into the arena, which is never freed.
        f(unsafe { std::slice::from_raw_parts((k & key::ADDRESS) as *const u8, (k >> 48 & 255) as usize) })
    }
}

/// Byte-wise order of two click IDs.
fn click_id_cmp(ids: &Interner, a: Key, b: Key) -> std::cmp::Ordering {
    if (a | b) & INTERNED == 0 {
        return a.cmp(&b);
    }
    with_click_bytes(ids, a, |x| with_click_bytes(ids, b, |y| x.cmp(y)))
}

/// Whether two click IDs are the same bytes: keys differ for copies of a click
/// with a long ID, so those are compared by their bytes, but only when the
/// clicks are also at one time (copies are identical), which keeps the page
/// reads of the click file to the few real copies.
fn click_id_eq(ids: &Interner, a: ClickItem, b: ClickItem) -> bool {
    let (x, y) = (click_cid(a), click_cid(b));
    if x == y {
        return true;
    }
    (x & y) & INTERNED != 0 && click_ts(a) == click_ts(b) && with_click_bytes(ids, x, |p| with_click_bytes(ids, y, |q| p == q))
}

/// A link line on its way: both IDs found, then joined, a few lines apart so
/// that every table slot has been asked for before it is read.
#[derive(Clone, Copy)]
struct LinkPend {
    a: IdRef,
    b: IdRef,
    slots: (u32, u32),
    live: bool,
}

struct Links {
    ring: [LinkPend; 16],
    n: usize,
    ins: usize,
}

// The IdRefs point into a read buffer, but none outlives its chunk: `drain` ends every chunk.
unsafe impl Send for Links {}

impl Links {
    fn new() -> Links {
        let dead = LinkPend { a: IdRef::EMPTY, b: IdRef::EMPTY, slots: (0, 0), live: false };
        Links { ring: [dead; 16], n: 0, ins: 0 }
    }

    /// One step of the three-stage pipeline: the entry 16 steps old is
    /// joined, the one 8 steps old is looked up, `new` is queued.
    #[inline(always)]
    fn step(&mut self, cx: &Cx, new: Option<(IdRef, IdRef)>) {
        let i = self.n & 15;
        if self.n >= 16 {
            let e = &self.ring[i];
            if e.live && e.slots.0 != e.slots.1 {
                cx.people.join(&cx.lk.ids, e.slots.0, e.slots.1);
            }
        }
        if self.n >= 8 {
            let e = &mut self.ring[(self.n - 8) & 15];
            if e.live {
                let sa = cx.people.resolve(&cx.lk.ids, &e.a, &mut self.ins);
                let sb = cx.people.resolve(&cx.lk.ids, &e.b, &mut self.ins);
                cx.people.prefetch_parent(sa);
                cx.people.prefetch_parent(sb);
                e.slots = (sa, sb);
            }
        }
        self.ring[i] = match new {
            Some((a, b)) => LinkPend { a, b, slots: (0, 0), live: true },
            None => LinkPend { a: IdRef::EMPTY, b: IdRef::EMPTY, slots: (0, 0), live: false },
        };
        self.n += 1;
    }

    fn drain(&mut self, cx: &Cx) {
        for _ in 0..16 {
            self.step(cx, None);
        }
        self.n = 0;
    }
}

/// The CSV files in `dir`, opened for `pread`.
fn input_files(dir: &Path) -> Vec<csv::Input> {
    csv::Input::open_all(dir)
}

/// Every chunk of the files, `size` bytes each: (file number, start, end).
fn chunks_sized(files: &[csv::Input], size: usize) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    for (i, file) in files.iter().enumerate() {
        for (s, e) in csv::chunks(file.len(), size) {
            out.push((i, s, e));
        }
    }
    out
}

fn file_bytes(dir: &Path) -> usize {
    csv::list(dir).iter().filter_map(|p| std::fs::metadata(p).ok()).map(|m| m.len() as usize).sum()
}

fn env_num(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// log2 of the power of two nearest to `n` (at least 1).
fn log2_near(n: usize) -> u32 {
    let n = n.max(1);
    let lo = usize::BITS - 1 - n.leading_zeros();
    lo + (n >= (3usize << lo) / 2) as u32
}

/// Scratch of the duplicate finder, one per thread.
#[derive(Default)]
struct Finder {
    marks: Vec<u64>,
    cand: Vec<LRec>,
    /// The earliest copy seen so far of each candidate key; key 0 is a free slot.
    best: Vec<LRec>,
    /// Record numbers to mark dropped.
    drops: Vec<u32>,
}

impl Finder {
    /// Fills `drops` with the `loc` of every valid copy that has an earlier
    /// copy (smaller ingest) among `copies`; returns the number of candidates.
    fn find(&mut self, copies: &[&[LRec]]) -> usize {
        let n: usize = copies.iter().map(|c| c.len()).sum();
        // Equal keys always select the same bit: a bit set once proves its key
        // unique; a bit hit twice (a repeated key or a collision) makes every
        // entry with it a candidate. Two bits per position: seen, repeated.
        let words = n.div_ceil(2).clamp(1, 1 << 16).next_power_of_two();
        self.marks.clear();
        self.marks.resize(words, 0);
        let marks = &mut self.marks[..];
        let at = |key: Key| {
            let h = hash(key) as usize;
            ((h >> 5) & (words - 1), 1u64 << ((h & 31) * 2))
        };
        for copy in copies {
            for r in *copy {
                let (w, bit) = at(r.key);
                let m = &mut marks[w];
                *m |= bit | ((*m & bit) << 1);
            }
        }
        self.cand.clear();
        for copy in copies {
            for r in *copy {
                let (w, bit) = at(r.key);
                if marks[w] & (bit << 1) != 0 {
                    self.cand.push(*r);
                }
            }
        }
        // The candidates go through an exact table that keeps the earliest
        // copy of each key; whichever of two copies is later is dropped.
        let slots = (self.cand.len() * 2).next_power_of_two().max(16);
        self.best.clear();
        self.best.resize(slots, LRec { key: 0, ingest: 0, loc: 0 });
        self.drops.clear();
        for r in &self.cand {
            let mut i = hash(r.key) as usize & (slots - 1);
            loop {
                let b = &mut self.best[i];
                if b.key == 0 {
                    *b = *r;
                    break;
                }
                if b.key == r.key {
                    let loser = if r.ingest < b.ingest { std::mem::replace(b, *r) } else { *r };
                    if loser.loc != 0 {
                        self.drops.push(loser.loc);
                    }
                    break;
                }
                i = (i + 1) & (slots - 1);
            }
        }
        self.cand.len()
    }
}

/// Per-thread buffers of the sessions pass.
struct Part {
    start: Vec<u32>,
    pos: Vec<u32>,
    items: Vec<u64>,
    click_start: Vec<u32>,
    click_pos: Vec<u32>,
    click_items: Vec<ClickItem>,
    scratch: Scratch,
    dense_sessions: BinnedSessions,
    out: Output,
}

/// Prefix sums of `count` (which has one extra slot): count[i] becomes the
/// start of item i, and `pos` the same starts, to fill in.
fn starts(count: &mut [u32], pos: &mut Vec<u32>) {
    let mut total = 0u32;
    for c in count.iter_mut() {
        let n = *c;
        *c = total;
        total += n;
    }
    pos.clear();
    pos.extend_from_slice(&count[..count.len() - 1]);
}

/// The sessions pass for one person partition: persons are told apart by the
/// low bits of their slot number, so a counting sort needs no hash table.
fn sessions_part(cx: &Cx, part: usize, events: &[&[ERec]], clicks: &[&[CRec]], st: &mut Part) {
    let lk = cx.lk;
    let width = 1usize << cx.pshift;
    let mask = (width - 1) as u32;
    let n: usize = events.iter().map(|s| s.len()).sum();
    if n == 0 {
        return;
    }
    st.start.clear();
    st.start.resize(width + 1, 0);
    let mut live = 0usize;
    for s in events {
        for e in s.iter() {
            if e.person != DROPPED {
                st.start[(e.person & mask) as usize] += 1;
                live += 1;
            }
        }
    }
    if live == 0 {
        return;
    }
    starts(&mut st.start, &mut st.pos);
    st.items.clear();
    st.items.resize(live, 0);
    for s in events {
        for e in s.iter() {
            if e.person != DROPPED {
                let p = &mut st.pos[(e.person & mask) as usize];
                st.items[*p as usize] = (e.ts as u64) << 32 | e.pc as u64;
                *p += 1;
            }
        }
    }

    st.click_start.clear();
    st.click_start.resize(width + 1, 0);
    let mut interned = false;
    for s in clicks {
        for c in s.iter() {
            st.click_start[(c.person & mask) as usize] += 1;
            interned |= c.cid & INTERNED != 0;
        }
    }
    starts(&mut st.click_start, &mut st.click_pos);
    let total = *st.click_start.last().unwrap() as usize;
    st.click_items.clear();
    st.click_items.resize(total, 0);
    for s in clicks {
        for c in s.iter() {
            let p = &mut st.click_pos[(c.person & mask) as usize];
            st.click_items[*p as usize] = (c.ts as u128) << 96 | (c.cid as u128) << 32 | c.campaign as u128;
            *p += 1;
        }
    }

    let max = (lk.window_end - 1 - lk.base) as u32;
    let first = (part as u32) << cx.pshift;
    for i in 0..width {
        let (a, z) = (st.start[i] as usize, st.start[i + 1] as usize);
        if a == z {
            continue;
        }
        let ev = &st.items[a..z];
        let cl = &mut st.click_items[st.click_start[i] as usize..st.click_start[i + 1] as usize];
        let mut m = cl.len();
        if m > 1 {
            if interned {
                cl.sort_unstable_by(|&a, &b| {
                    click_ts(a).cmp(&click_ts(b)).then_with(|| click_id_cmp(&lk.ids, click_cid(a), click_cid(b)))
                });
            } else {
                cl.sort_unstable();
            }
            // Copies of a click are identical, so they are now adjacent.
            m = 1;
            for r in 1..cl.len() {
                if !click_id_eq(&lk.ids, cl[r], cl[m - 1]) {
                    cl[m] = cl[r];
                    m += 1;
                }
            }
        }
        let sessions = st.dense_sessions.build(ev, max);
        person(cx.people.key_of(first + i as u32), sessions, &cl[..m], lk, &mut st.scratch, &mut st.out);
    }
}

pub fn execute(lk: &Lookups, data: &Path, out: &Path, threads: usize, clock: &Instant) -> ! {
    let lap = |what: &str| eprintln!("{:>8.3}s {what}", clock.elapsed().as_secs_f64());
    let (link_bytes, ev_bytes, ck_bytes) = (file_bytes(&data.join("links")), file_bytes(&data.join("events")), file_bytes(&data.join("clicks")));
    // The generator's data holds one distinct ID per 16 bytes of links
    // (plus a little for events and clicks); the table is made 2.2 times that
    // and rounded up to a power of two, and counts as full at three quarters.
    // A wrong guess costs speed (a crowded table) or a restart (a full one).
    let id_est = link_bytes / 16 + ev_bytes / 16_000 + ck_bytes / 16_000;
    let bits = env_num("SESS_ID_BITS").map(|v| v as u32).unwrap_or(usize::BITS - (id_est * 22 / 10).leading_zeros()).clamp(16, 30);
    // About 90,000 events per person partition and event bucket.
    let ev_est = ev_bytes / 72;
    let pbits = env_num("SESS_PBITS").map(|v| v as u32).unwrap_or_else(|| log2_near(ev_est / 90_000)).clamp(3, 11.min(bits - 4));
    let qbits = env_num("SESS_QBITS").map(|v| v as u32).unwrap_or_else(|| log2_near(ev_est / 90_000)).clamp(3, 11);
    let (np, nq) = (1usize << pbits, 1usize << qbits);

    let cx = Cx { lk, people: People::new(bits), region: chunks::region_base(), pshift: bits - pbits, qshift: 64 - qbits };
    let people = &cx.people;
    run(threads, people.pieces(), || (), |_, i| people.populate_piece(i));

    // 1. Links.
    let link_files = input_files(&data.join("links"));
    let link_chunks = chunks_sized(&link_files, LINK_CHUNK);
    run(threads, link_chunks.len(), || (Links::new(), csv::ChunkBuf::new(LINK_CHUNK)), |(st, buf), t| {
        let (file, s, e) = link_chunks[t];
        let (data, s, e) = buf.load(&link_files[file], s, e);
        csv::rows::<2>(data, s, e, |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                st.step(&cx, Some((people.locate(f[0]), people.locate(f[1]))));
            }
        });
        st.drain(&cx);
    });
    drop(link_chunks);
    drop(link_files);
    const SPAN: usize = 1 << 15;
    run(threads, people.capacity().div_ceil(SPAN), || (), |_, t| people.settle(t * SPAN, ((t + 1) * SPAN).min(people.capacity())));
    lap(&format!("links: {} IDs in 2^{bits} slots", people.len()));

    // 2. Events and clicks.
    let event_files = input_files(&data.join("events"));
    let click_files = input_files(&data.join("clicks"));
    let chunk = env_num("SESS_CHUNK").unwrap_or(READ_CHUNK);
    let event_chunks = chunks_sized(&event_files, chunk);
    let click_chunks = chunks_sized(&click_files, chunk);
    let ne = event_chunks.len();
    let shards = run(
        threads,
        ne + click_chunks.len(),
        || (Shard::new(np, nq), csv::ChunkBuf::new(chunk)),
        |(st, buf), t| {
            if t < ne {
                let (file, s, e) = event_chunks[t];
                let (data, s, e) = buf.load(&event_files[file], s, e);
                csv::rows::<7>(data, s, e, |f| parse_event(f, &cx, st));
                st.drain::<true>(&cx);
            } else {
                let (file, s, e) = click_chunks[t - ne];
                let (data, s, e) = buf.load(&click_files[file], s, e);
                csv::rows::<4>(data, s, e, |f| parse_click(f, &cx, st));
                st.drain::<false>(&cx);
            }
        },
    );
    let shards: Vec<Shard> = shards.into_iter().map(|(shard, _buf)| shard).collect();
    drop((event_chunks, click_chunks));
    drop(event_files);
    drop(click_files);
    lap(&format!("parse: {} IDs", people.len()));

    // 3. Duplicates.
    let finders = run(threads, nq, Finder::default, |fd, b| {
        let copies: Vec<&[LRec]> = shards.iter().flat_map(|s| s.lg[b].slices()).collect();
        fd.find(&copies);
        let ahead = |l: u32| {
            #[cfg(target_arch = "x86_64")]
            // Safety: a prefetch of the address of a record; hints never fault.
            unsafe {
                use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                _mm_prefetch(cx.rec(l) as *const i8, _MM_HINT_T0);
            }
        };
        for k in 0..fd.drops.len() {
            if let Some(&l) = fd.drops.get(k + 8) {
                ahead(l);
            }
            // Safety: a record of a live bucket; each record has one entry, in
            // one bucket, so no other thread writes it, and nothing reads it now.
            unsafe { (*cx.rec(fd.drops[k])).person = DROPPED };
        }
    });
    drop(finders);
    lap("duplicates");

    // 4. Sessions per person partition, the largest first.
    let sizes: Vec<usize> = (0..np).map(|p| shards.iter().map(|s| s.ev[p].len()).sum()).collect();
    let order = by_size(&sizes);
    let slots = (lk.channels.len() + 1) * lk.days;
    let results = run(
        threads,
        np,
        || Part {
            start: Vec::new(),
            pos: Vec::new(),
            items: Vec::new(),
            click_start: Vec::new(),
            click_pos: Vec::new(),
            click_items: Vec::new(),
            scratch: Scratch::default(),
            dense_sessions: BinnedSessions::default(),
            out: Output { rows: Bucket::new(), slab: Slab::new(), counts: vec![Counts::default(); slots] },
        },
        |st, t| {
            let p = order[t];
            let events: Vec<&[ERec]> = shards.iter().flat_map(|s| s.ev[p].slices()).collect();
            let clicks: Vec<&[CRec]> = shards.iter().flat_map(|s| s.ck[p].slices()).collect();
            sessions_part(&cx, p, &events, &clicks, st);
        },
    );
    lap("sessions");

    let outputs: Vec<Output> = results.into_iter().map(|s| s.out).collect();
    write_tables(out, &outputs, lk, threads);
    lap("write");
    // Everything is written and closed: skip freeing the buffers.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lcg(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    /// Every valid copy but the earliest ingested of each key is dropped, also
    /// when most keys collide on the same filter bit (few entries) or the filter
    /// is roomy, and an invalid earliest copy (loc 0) still drops the later ones.
    #[test]
    fn finder_drops_all_but_the_earliest_copy() {
        let mut rng = 3u64;
        for &(keys, copies) in &[(40usize, 300usize), (500, 700), (3000, 3100), (5, 400), (1, 1), (7, 7)] {
            // Distinct ingest seconds, so each key has one earliest copy.
            let mut entries: Vec<LRec> = Vec::new();
            for i in 0..copies {
                let key = if i < keys { i as u64 + 1 } else { lcg(&mut rng) % keys as u64 + 1 };
                let loc = if lcg(&mut rng) % 7 == 0 { 0 } else { i as u32 + 1 };
                entries.push(LRec { key, ingest: (i as u32 * 7919) % 100_003, loc });
            }
            let mut earliest: std::collections::HashMap<u64, u32> = Default::default();
            for e in &entries {
                let b = earliest.entry(e.key).or_insert(e.ingest);
                *b = (*b).min(e.ingest);
            }
            let (a, b) = entries.split_at(entries.len() / 3);
            let mut f = Finder::default();
            f.find(&[a, b]);
            let mut got = f.drops.clone();
            got.sort_unstable();
            let mut want: Vec<u32> = entries.iter().filter(|e| e.loc != 0 && e.ingest != earliest[&e.key]).map(|e| e.loc).collect();
            want.sort_unstable();
            assert_eq!(got, want);
        }
    }
    /// Click IDs of every kind (packed, long, odd bytes) compare by their bytes
    /// whichever way their key was made, and copies of a click at one time are
    /// equal although a long ID's key is a pointer.
    #[test]
    fn click_ids_compare_by_bytes() {
        let ids = Interner::new();
        let long_a = format!("{:09}{:032x}", 17, 0xfeedu64).into_bytes();
        let long_b = format!("{:09}{:032x}", 17, 0xfeeeu64).into_bytes();
        let samples: Vec<Vec<u8>> = vec![
            b"a".to_vec(), b"ab".to_vec(), b"c0000001".to_vec(), b"c00000010".to_vec(),
            long_a.clone(), long_a.clone(), long_b, vec![0xff, b'z'], vec![b'x', 0, b'y'], b"000000017".to_vec(),
            b"0000000170".to_vec(),
        ];
        let keys: Vec<Key> = samples.iter().map(|s| click_key(&ids, s)).collect();
        let item = |key: Key, ts: u32| (ts as u128) << 96 | (key as u128) << 32;
        for (i, (a, ka)) in samples.iter().zip(&keys).enumerate() {
            for (j, (b, kb)) in samples.iter().zip(&keys).enumerate() {
                assert_eq!(click_id_cmp(&ids, *ka, *kb), a.cmp(b), "{i} {j}");
                assert_eq!(click_id_eq(&ids, item(*ka, 5), item(*kb, 5)), a == b, "{i} {j}");
                if a != b {
                    assert!(!click_id_eq(&ids, item(*ka, 5), item(*kb, 6)));
                }
            }
        }
    }
}
