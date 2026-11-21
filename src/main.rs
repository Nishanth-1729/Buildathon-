//! Shopping Sessions: rules.md in three parallel passes over cache-sized buckets.
//!
//! 1. Parse: links, events and clicks in 4 MB chunks on every core. Event
//!    copies go to buckets by event_id hash. The worker that parses the last
//!    link chunk resolves the linked IDs while the others keep parsing.
//! 2. Route: per event bucket, keep each event's earliest copy (a bitmap
//!    proves most IDs unique), drop it if invalid, and send the survivors to
//!    buckets by person. Clicks are routed by person too.
//! 3. Sessions: per person bucket, group by person with a counting sort, sort
//!    each person's events by time, then sessions, credit and assists.
//! 4. Write both tables as Parquet.
//!
//! Timestamps are kept as u32 milliseconds after `base` (7 days before the
//! window). When a dataset breaks that or another limit of this layout, the
//! general pipeline in `legacy` runs instead.

mod chunks;
mod csv;
mod key;
mod legacy;
mod parquet;
mod ts;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::{AcqRel, Relaxed}};
use std::sync::Mutex;
use std::time::Instant;

use chunks::{Bucket, Slab};
use key::{hash, Interner, Key, KeyMap, INTERNED};
use parquet::{Column, Values};
use ts::DAY;

const DAY32: u32 = 86_400_000;
const GAP: u32 = 1_800_000;
const REACH: u32 = 7 * DAY32;
const BUCKETS: usize = 512;
/// Up to this many sessions, assists are found session by session.
const FEW_SESSIONS: usize = 12;
const CHUNK: usize = 4 << 20;
/// `Raw::ts` of an invalid event copy.
const INVALID: u32 = u32::MAX;
const NO_CATEGORY: u8 = u8::MAX;
/// `pc` packs a purchase's price (1..=10,000,000 < 2^24) and its category.
const PRICE_MASK: u32 = (1 << 24) - 1;

/// One copy of an event, before dedup.
#[derive(Clone, Copy)]
#[repr(C)]
struct Raw {
    eid: Key,
    uid: Key,
    ingest: i64,
    /// INVALID unless the copy passes validation.
    ts: u32,
    pc: u32,
}

/// A surviving event, routed to its person's bucket.
#[derive(Clone, Copy)]
#[repr(C)]
struct Event {
    person: Key,
    ts: u32,
    pc: u32,
}

/// A valid click; `person` is the raw user ID until it is routed.
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    person: Key,
    cid: Key,
    ts: u32,
    campaign: u32,
}

/// A click of one person as `ts << 96 | cid << 32 | campaign`, so that plain
/// integer order is (ts, cid) order, unless an ID is interned.
type ClickItem = u128;

#[inline(always)]
fn click_ts(c: ClickItem) -> u32 {
    (c >> 96) as u32
}

#[inline(always)]
fn click_campaign(c: ClickItem) -> usize {
    c as u32 as usize
}

#[inline(always)]
fn click_cid(c: ClickItem) -> Key {
    (c >> 32) as u64
}

#[derive(Clone, Copy)]
struct UserRow {
    person: Key,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u8,
}

#[derive(Clone, Copy, Default)]
struct Counts {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

struct Lookups {
    ids: Interner,
    window_start: i64,
    window_end: i64,
    /// window_start - 7 days; every kept timestamp is base + a u32.
    base: i64,
    products: KeyMap<u8>,
    /// Sorted byte-wise, so a lower index is a byte-wise smaller name.
    categories: Vec<Vec<u8>>,
    unknown: u8,
    /// campaign key -> index; `organic` is index `channels.len()`.
    campaigns: KeyMap<u32>,
    campaign_names: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    first_day: i64,
    days: usize,
}

/// Runs `work` on task indexes 0..tasks across `threads` threads, each with
/// its own state from `init`; returns the states.
fn run<S: Send>(threads: usize, tasks: usize, init: impl Fn() -> S + Sync, work: impl Fn(&mut S, usize) + Sync) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let worker = || {
        let mut state = init();
        loop {
            let task = next.fetch_add(1, Relaxed);
            if task >= tasks {
                return state;
            }
            work(&mut state, task);
        }
    };
    std::thread::scope(|scope| {
        let handles: Vec<_> = (1..threads.min(tasks.max(1))).map(|_| scope.spawn(&worker)).collect();
        let here = worker();
        std::iter::once(here).chain(handles.into_iter().map(|h| h.join().unwrap())).collect()
    })
}

#[inline(always)]
fn bucket(k: Key) -> usize {
    (hash(k) >> 55) as usize % BUCKETS
}

fn manifest_time(text: &str, name: &str) -> i64 {
    let at = text.find(&format!("\"{name}\"")).unwrap_or_else(|| panic!("manifest: no {name}"));
    let rest = &text[at + name.len() + 2..];
    let open = rest.find('"').unwrap() + 1;
    let close = open + rest[open..].find('"').unwrap();
    ts::parse(rest[open..close].as_bytes()).unwrap_or_else(|| panic!("manifest: bad {name}"))
}

/// Rows of a small two-column lookup file.
fn lookup_rows(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mapping = csv::Mapping::open(path);
    let data = mapping.bytes();
    let mut out = Vec::new();
    csv::lines(data, 0, data.len(), |line| {
        let mut f = [None; 2];
        if csv::fields::<2>(line, &mut f) {
            if let Some(k) = f[0] {
                out.push((k.to_vec(), f[1].unwrap_or(b"").to_vec()));
            }
        }
    });
    out
}

/// None when the dataset is outside this layout's limits.
fn lookups(data: &Path) -> Option<Lookups> {
    let ids = Interner::new();
    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let window_start = manifest_time(&manifest, "window_start");
    let window_end = manifest_time(&manifest, "window_end");
    let base = window_start.checked_sub(REACH as i64)?;
    let span = window_end.checked_sub(base)?;
    // Click timestamps + 7 days must fit a u32 below INVALID.
    if window_end <= window_start || span >= (u32::MAX - REACH - 1) as i64 {
        return None;
    }

    let mapping = csv::Mapping::open(&data.join("products.csv"));
    let mut product_rows: Vec<(&[u8], &[u8])> = Vec::new();
    let product_data = mapping.bytes();
    csv::rows::<2>(product_data, 0, product_data.len(), |f| {
        if !f[0].is_empty() {
            product_rows.push((f[0], f[1]));
        }
    });
    // Distinct categories, numbered in first-seen order (unknown is 0), then
    // renumbered in byte order so a lower index is a byte-wise smaller name.
    let mut seen: std::collections::HashMap<&[u8], u8, std::hash::BuildHasherDefault<key::KeyHasher>> =
        Default::default();
    let mut names: Vec<&[u8]> = vec![b"unknown"];
    seen.insert(b"unknown", 0);
    let mut first_seen = Vec::with_capacity(product_rows.len());
    for &(_, c) in &product_rows {
        let next = names.len();
        let id = *seen.entry(c).or_insert(next as u8);
        if id as usize == next {
            names.push(c);
            if names.len() > NO_CATEGORY as usize {
                return None;
            }
        }
        first_seen.push(id);
    }
    let mut order: Vec<u8> = (0..names.len() as u8).collect();
    order.sort_unstable_by(|&a, &b| names[a as usize].cmp(names[b as usize]));
    let mut renumber = vec![0u8; names.len()];
    for (rank, &old) in order.iter().enumerate() {
        renumber[old as usize] = rank as u8;
    }
    let categories: Vec<Vec<u8>> = order.iter().map(|&o| names[o as usize].to_vec()).collect();
    let mut products = KeyMap::default();
    products.reserve(product_rows.len());
    for (&(p, _), &c) in product_rows.iter().zip(&first_seen) {
        products.entry(ids.key(p)).or_insert(renumber[c as usize]);
    }
    let unknown = renumber[0];

    let mut campaigns = KeyMap::default();
    let (mut campaign_names, mut channels) = (Vec::new(), Vec::new());
    for (c, ch) in lookup_rows(&data.join("campaigns.csv")) {
        let k = ids.key(&c);
        if !campaigns.contains_key(&k) {
            campaigns.insert(k, channels.len() as u32);
            campaign_names.push(c);
            channels.push(ch);
        }
    }
    let first_day = window_start.div_euclid(DAY);
    let days = ((window_end - 1).div_euclid(DAY) - first_day + 1).max(1) as usize;
    Some(Lookups {
        ids,
        window_start,
        window_end,
        base,
        products,
        categories,
        unknown,
        campaigns,
        campaign_names,
        channels,
        first_day,
        days,
    })
}

fn mapped_csvs(dir: &Path) -> Vec<csv::Mapping> {
    csv::list(dir).iter().map(|path| csv::Mapping::open(path)).collect()
}

/// Every chunk of the mapped CSVs: (file contents, start, end).
fn chunks_of(mappings: &[csv::Mapping]) -> Vec<(&[u8], usize, usize)> {
    let mut out = Vec::new();
    for mapping in mappings {
        let data = mapping.bytes();
        for (s, e) in csv::chunks(data.len(), CHUNK) {
            out.push((data, s, e));
        }
    }
    out
}

/// Linked ID -> person name, for every ID whose name is not itself. Open
/// addressing with linear probing; key 0 (the empty string) marks a free slot.
struct Names {
    mask: usize,
    slots: Vec<[Key; 2]>,
    /// One bit per hash of every linked ID: a clear bit proves an ID unlinked.
    filter: Vec<u64>,
    filter_mask: usize,
}

impl Names {
    fn empty() -> Self {
        Names { mask: 0, slots: vec![[0, 0]], filter: vec![0], filter_mask: 0 }
    }

    #[inline(always)]
    fn slot(&self, k: Key) -> usize {
        hash(k) as usize & self.mask
    }

    /// (word, bit) of k in the filter, from hash bits the slots do not use.
    #[inline(always)]
    fn filter_bit(&self, k: Key) -> (usize, u64) {
        let h = hash(k);
        ((h >> 38) as usize & self.filter_mask, 1 << (h >> 32 & 63))
    }

    /// Where to look for k, branch-free: the first slot with HIT set if the
    /// filter does not rule k out, else slot 0 (always cached) without HIT.
    #[inline(always)]
    fn locate(&self, k: Key) -> u32 {
        let (word, bit) = self.filter_bit(k);
        let hit = self.filter[word] & bit != 0;
        (self.slot(k) as u32 | HIT) & (hit as u32).wrapping_neg()
    }

    #[inline(always)]
    fn prefetch(&self, at: u32) {
        #[cfg(target_arch = "x86_64")]
        // `at` came from `locate`: its slot index is below the table size.
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add((at & !HIT) as usize).cast::<i8>(), _MM_HINT_T0);
        }
    }

    /// The name of k, given `locate(k)`. The first slot is checked without
    /// branching on the result; only a collision on a possible hit probes on.
    #[inline(always)]
    fn resolve(&self, k: Key, at: u32) -> Key {
        let i = (at & !HIT) as usize;
        let [sk, sv] = self.slots[i];
        let found = sk == k;
        let name = if found { sv } else { k };
        if (at & HIT != 0) & !found & (sk != 0) {
            return self.probe_on(k, i);
        }
        name
    }

    #[cold]
    #[inline(never)]
    fn probe_on(&self, k: Key, mut i: usize) -> Key {
        loop {
            i = (i + 1) & self.mask;
            let [sk, sv] = self.slots[i];
            if sk == k {
                return sv;
            }
            if sk == 0 {
                return k;
            }
        }
    }

    /// Union-find over the link pairs. IDs are numbered in byte order, and a
    /// component's root is always its lowest number, so the root is the name.
    fn build(lists: Vec<Vec<(Key, Key)>>, ids: &Interner) -> Self {
        let n: usize = lists.iter().map(Vec::len).sum();
        if n == 0 {
            return Self::empty();
        }
        let mut sides: Vec<(Key, u32)> = Vec::with_capacity(2 * n);
        for list in &lists {
            for &(a, b) in list {
                let i = sides.len() as u32;
                sides.push((a, i));
                sides.push((b, i + 1));
            }
        }
        drop(lists);
        if sides.iter().any(|s| s.0 & INTERNED != 0) {
            sides.sort_unstable_by(|x, y| ids.cmp(x.0, y.0));
        } else {
            sides.sort_unstable_by_key(|s| s.0);
        }
        let mut side_id = vec![0u32; sides.len()];
        for id in side_id.iter_mut().step_by(4096 / 4) {
            *id = 0;
        }
        let mut keys: Vec<Key> = Vec::with_capacity(sides.len());
        for &(k, i) in &sides {
            if keys.last() != Some(&k) {
                keys.push(k);
            }
            side_id[i as usize] = keys.len() as u32 - 1;
        }
        drop(sides);
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                let up = parent[parent[x as usize] as usize];
                parent[x as usize] = up;
                x = up;
            }
            x
        }
        let mut parent: Vec<u32> = (0..keys.len() as u32).collect();
        for pair in side_id.chunks_exact(2) {
            let (ra, rb) = (find(&mut parent, pair[0]), find(&mut parent, pair[1]));
            if ra < rb {
                parent[rb as usize] = ra;
            } else if rb < ra {
                parent[ra as usize] = rb;
            }
        }
        let mut linked = 0;
        for i in 0..keys.len() as u32 {
            let r = find(&mut parent, i);
            parent[i as usize] = r;
            linked += (r != i) as usize;
        }
        let cap = (linked * 2).next_power_of_two().max(16);
        let words = (linked * 16 / 64).next_power_of_two().max(1);
        let mut names = Names { mask: cap - 1, slots: vec![[0, 0]; cap], filter: vec![0; words], filter_mask: words - 1 };
        // First touches in random order are slow page faults; fault the
        // table in sequentially (a write per 4 KB page) before filling it.
        for slot in names.slots.iter_mut().step_by(4096 / 16) {
            slot[1] = 0;
        }
        for (i, &r) in parent.iter().enumerate() {
            if r as usize != i {
                let k = keys[i];
                let (word, bit) = names.filter_bit(k);
                names.filter[word] |= bit;
                let mut s = names.slot(k);
                while names.slots[s][0] != 0 {
                    s = (s + 1) & names.mask;
                }
                names.slots[s] = [k, keys[r as usize]];
            }
        }
        names
    }
}

/// Resolves IDs to person names through a sliding window: each slot is
/// prefetched `WINDOW` lookups before it is read.
const WINDOW: usize = 32;
const HIT: u32 = 1 << 31;

struct Batch<T: Copy> {
    keys: [Key; WINDOW],
    at: [u32; WINDOW],
    items: [T; WINDOW],
    pushed: usize,
    done: usize,
}

impl<T: Copy> Batch<T> {
    fn new(fill: T) -> Self {
        Batch { keys: [0; WINDOW], at: [0; WINDOW], items: [fill; WINDOW], pushed: 0, done: 0 }
    }

    #[inline(always)]
    fn push(&mut self, names: &Names, k: Key, item: T, sink: &mut impl FnMut(Key, T)) {
        if self.pushed - self.done == WINDOW {
            let i = self.done % WINDOW;
            sink(names.resolve(self.keys[i], self.at[i]), self.items[i]);
            self.done += 1;
        }
        let at = names.locate(k);
        names.prefetch(at);
        let i = self.pushed % WINDOW;
        self.keys[i] = k;
        self.at[i] = at;
        self.items[i] = item;
        self.pushed += 1;
    }

    #[inline(always)]
    fn flush(&mut self, names: &Names, sink: &mut impl FnMut(Key, T)) {
        while self.done < self.pushed {
            let i = self.done % WINDOW;
            sink(names.resolve(self.keys[i], self.at[i]), self.items[i]);
            self.done += 1;
        }
    }
}

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], lk: &Lookups, out: &mut [Bucket<Raw>], slab: &mut Slab) {
    if f[0].is_empty() {
        return;
    }
    let ingest = ts::parse(f[3]).unwrap_or(i64::MAX);
    let mut raw = Raw { eid: lk.ids.key(f[0]), uid: 0, ingest, ts: INVALID, pc: 0 };
    'valid: {
        if f[1].is_empty() || f[5].is_empty() {
            break 'valid;
        }
        let Some(ts) = ts::parse(f[2]) else { break 'valid };
        if ts < lk.window_start || ts >= lk.window_end || ingest.saturating_sub(ts) > DAY {
            break 'valid;
        }
        raw.pc = match f[4] {
            b"view" | b"cart" => 0,
            b"purchase" => {
                let price = match ts::digits(f[6]) {
                    Some(p) if (1..=10_000_000).contains(&p) => p as u32,
                    _ => break 'valid,
                };
                let category = *lk.products.get(&lk.ids.key(f[5])).unwrap_or(&lk.unknown);
                price | (category as u32) << 24
            }
            _ => break 'valid,
        };
        raw.uid = lk.ids.key(f[1]);
        raw.ts = (ts - lk.base) as u32;
    }
    out[bucket(raw.eid)].push(slab, raw);
}

#[inline(always)]
fn parse_click(f: &[&[u8]; 4], lk: &Lookups, out: &mut Bucket<Click>, slab: &mut Slab) {
    if f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
        return;
    }
    let Some(ts) = ts::parse(f[3]) else { return };
    if ts < lk.base || ts >= lk.window_end {
        return;
    }
    let Some(&campaign) = lk.campaigns.get(&lk.ids.key(f[2])) else { return };
    out.push(slab, Click { person: lk.ids.key(f[1]), cid: lk.ids.key(f[0]), ts: (ts - lk.base) as u32, campaign });
}

/// Task order: biggest first, so one large bucket does not finish last.
fn by_size(sizes: &[usize]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..sizes.len()).collect();
    order.sort_unstable_by_key(|&b| std::cmp::Reverse(sizes[b]));
    order
}

#[derive(Default)]
struct Dedup {
    marks: Vec<u64>,
    candidates: KeyMap<Raw>,
}

impl Dedup {
    /// Calls `keep` with the earliest copy of every event_id among `copies`.
    fn winners(&mut self, len: usize, copies: &[&[Raw]], mut keep: impl FnMut(&Raw)) {
        // Equal IDs always select the same bit, so a bit set once proves its
        // ID unique. Repeated bits, collisions included, go to an exact map.
        let words = len.div_ceil(2).clamp(1, 1 << 16).next_power_of_two();
        self.marks.clear();
        self.marks.resize(words, 0);
        self.candidates.clear();
        let marks = &mut self.marks[..];
        let position = |id| {
            let h = hash(id) as usize;
            ((h >> 5) & (words - 1), 1u64 << ((h & 31) * 2))
        };
        for slice in copies {
            for e in *slice {
                let (word, bit) = position(e.eid);
                let m = &mut marks[word];
                *m |= bit | ((*m & bit) << 1);
            }
        }
        for slice in copies {
            for e in *slice {
                let (word, bit) = position(e.eid);
                if marks[word] & (bit << 1) == 0 {
                    keep(e);
                } else {
                    let best = self.candidates.entry(e.eid).or_insert(*e);
                    if e.ingest < best.ingest {
                        *best = *e;
                    }
                }
            }
        }
        self.candidates.values().for_each(keep);
    }
}

/// Person key -> dense local index, for one bucket.
struct LocalIndex {
    keys: Vec<Key>,
    index: Vec<u32>,
    mask: usize,
    persons: Vec<Key>,
}

impl LocalIndex {
    fn reset(&mut self, expected: usize) {
        let cap = (expected * 2).next_power_of_two().max(64);
        self.keys.clear();
        self.keys.resize(cap, 0);
        self.index.resize(cap, 0);
        self.mask = cap - 1;
        self.persons.clear();
    }

    #[inline(always)]
    fn insert(&mut self, k: Key) -> u32 {
        let mut s = hash(k) as usize & self.mask;
        loop {
            let sk = self.keys[s];
            if sk == k {
                return self.index[s];
            }
            if sk == 0 {
                break;
            }
            s = (s + 1) & self.mask;
        }
        let i = self.persons.len() as u32;
        self.keys[s] = k;
        self.index[s] = i;
        self.persons.push(k);
        if self.persons.len() * 2 > self.keys.len() {
            self.grow();
        }
        i
    }

    #[cold]
    fn grow(&mut self) {
        let cap = self.keys.len() * 2;
        self.keys.clear();
        self.keys.resize(cap, 0);
        self.index.resize(cap, 0);
        self.mask = cap - 1;
        for (i, &k) in self.persons.iter().enumerate() {
            let mut s = hash(k) as usize & self.mask;
            while self.keys[s] != 0 {
                s = (s + 1) & self.mask;
            }
            self.keys[s] = k;
            self.index[s] = i as u32;
        }
    }

    #[inline(always)]
    fn get(&self, k: Key) -> Option<u32> {
        let mut s = hash(k) as usize & self.mask;
        loop {
            let sk = self.keys[s];
            if sk == k {
                return Some(self.index[s]);
            }
            if sk == 0 {
                return None;
            }
            s = (s + 1) & self.mask;
        }
    }
}

/// Per-thread buffers of the sessions pass.
struct Sessions {
    local: LocalIndex,
    idx: Vec<u32>,
    count: Vec<u32>,
    start: Vec<u32>,
    items: Vec<u64>,
    click_idx: Vec<u32>,
    click_start: Vec<u32>,
    click_items: Vec<ClickItem>,
    scratch: Scratch,
    dense_sessions: BinnedSessions,
    out: Output,
}

#[derive(Default)]
struct Scratch {
    starts: Vec<u32>,
    days: Vec<u32>,
    day_end: Vec<u32>,
    /// Per campaign: the end of its merged click intervals so far (0 = none)
    /// and the number of session starts up to that end.
    covered: Vec<(u32, u32)>,
    /// Per campaign: the last session (by serial) it assisted.
    assisted_by: Vec<u32>,
    serial: u32,
    categories: Vec<(u8, u64)>,
}

struct Output {
    rows: Bucket<UserRow>,
    slab: Slab,
    counts: Vec<Counts>,
}

struct Session<'a> {
    start: u32,
    events: u32,
    /// The low 32 bits carry the purchase amount and category.
    items: &'a [u64],
}

#[cfg(test)]
fn sorted_sessions(mut events: &[u64]) -> impl Iterator<Item = Session<'_>> {
    std::iter::from_fn(move || {
        let start = (*events.first()? >> 32) as u32;
        let mut end = 1;
        let mut prev = start;
        while end < events.len() {
            let t = (events[end] >> 32) as u32;
            if t - prev > GAP {
                break;
            }
            prev = t;
            end += 1;
        }
        let (items, rest) = events.split_at(end);
        events = rest;
        Some(Session { start, events: end as u32, items })
    })
}

#[derive(Clone, Copy)]
struct TimeBin {
    first: u32,
    last: u32,
    events: u32,
    purchase: u32,
}

#[derive(Default)]
struct BinnedSessions {
    /// All empty between calls.
    bins: Vec<TimeBin>,
    touched: Vec<u32>,
    purchase_links: Vec<u64>,
    items: Vec<u64>,
    sessions: Vec<SessionSpan>,
}

struct SessionSpan {
    start: u32,
    events: u32,
    purchase_start: usize,
    purchase_end: usize,
}

impl BinnedSessions {
    /// Events in one GAP-wide bin form one run. Neighboring runs extend a
    /// session exactly when their two closest timestamps are within GAP.
    fn build(&mut self, events: &[u64], max_offset: u32) -> impl Iterator<Item = Session<'_>> {
        const EMPTY: TimeBin = TimeBin { first: u32::MAX, last: 0, events: 0, purchase: 0 };
        let len = (max_offset / GAP) as usize + 1;
        if self.bins.len() < len {
            self.bins.resize(len, EMPTY);
        }
        self.touched.clear();
        self.purchase_links.clear();
        self.items.clear();
        self.sessions.clear();

        for &event in events {
            let ts = (event >> 32) as u32;
            let pc = event as u32;
            let at = (ts / GAP) as usize;
            let bin = &mut self.bins[at];
            if bin.events == 0 {
                self.touched.push(at as u32);
            }
            bin.first = bin.first.min(ts);
            bin.last = bin.last.max(ts);
            bin.events += 1;
            if pc != 0 {
                self.purchase_links.push((bin.purchase as u64) << 32 | pc as u64);
                bin.purchase = self.purchase_links.len() as u32;
            }
        }
        // Visit the used bins in time order; few used bins are sorted, many
        // are found by a scan. Each visited bin is emptied again.
        if self.touched.len() * 8 > len {
            self.touched.clear();
            self.touched.extend((0..len as u32).filter(|&i| self.bins[i as usize].events != 0));
        } else {
            self.touched.sort_unstable();
        }

        let mut prev = 0;
        for &at in &self.touched {
            let bin = std::mem::replace(&mut self.bins[at as usize], EMPTY);
            let start_item = self.items.len();
            let mut p = bin.purchase;
            while p != 0 {
                let link = self.purchase_links[p as usize - 1];
                self.items.push(link as u32 as u64);
                p = (link >> 32) as u32;
            }
            let end_item = self.items.len();
            if self.sessions.is_empty() || bin.first - prev > GAP {
                self.sessions.push(SessionSpan {
                    start: bin.first,
                    events: bin.events,
                    purchase_start: start_item,
                    purchase_end: end_item,
                });
            } else {
                let last = self.sessions.last_mut().unwrap();
                last.events += bin.events;
                last.purchase_end = end_item;
            }
            prev = bin.last;
        }

        self.sessions.iter().map(|span| Session {
            start: span.start,
            events: span.events,
            items: &self.items[span.purchase_start..span.purchase_end],
        })
    }
}

#[cfg(test)]
#[test]
fn time_bins_match_sorted_sessions_at_gap_boundary() {
    let event = |time: u32, purchase: u32| (time as u64) << 32 | purchase as u64;
    let events = [
        event(3 * GAP, 37),
        event(GAP, 19),
        event(0, 0),
        event(2 * GAP + 1, 0),
        event(4 * GAP + 1, 59),
        event(GAP, 23),
    ];
    let summary = |session: Session<'_>| {
        let mut purchases: Vec<_> = session.items.iter()
            .map(|&event| event as u32)
            .filter(|&purchase| purchase != 0)
            .collect();
        purchases.sort_unstable();
        (session.start, session.events, purchases)
    };
    let mut sorted = events;
    sorted.sort_unstable();
    let reference: Vec<_> = sorted_sessions(&sorted).map(summary).collect();
    let actual: Vec<_> = BinnedSessions::default()
        .build(&events, 4 * GAP + 1)
        .map(summary)
        .collect();
    assert_eq!(actual, reference);
    assert_eq!(actual.len(), 3);
}

/// Aggregate a person's sessions and credit their sorted clicks.
fn person<'a>(
    person: Key,
    sessions: impl Iterator<Item = Session<'a>>,
    clicks: &[ClickItem],
    lk: &Lookups,
    sc: &mut Scratch,
    out: &mut Output,
) {
    let organic = lk.channels.len();
    sc.starts.clear();
    sc.days.clear();
    sc.categories.clear();
    let mut row: Option<UserRow> = None;
    let mut click_at = 0;
    let mut credit = 0;
    for session in sessions {
        let start = session.start;
        let day_abs = (lk.base + start as i64).div_euclid(DAY);
        let day = (day_abs - lk.first_day) as usize;
        if row.as_ref().map_or(true, |r| r.day as i64 != day_abs) {
            if let Some(r) = row.take() {
                flush(r, &mut sc.categories, &mut out.rows, &mut out.slab);
            }
            row = Some(UserRow { person, day: day_abs as i32, sessions: 0, events: 0, revenue: 0, top: NO_CATEGORY });
        }
        let r = row.as_mut().unwrap();
        r.sessions += 1;
        r.events += session.events;
        let mut revenue = 0u64;
        for &e in session.items {
            let price = (e as u32 & PRICE_MASK) as u64;
            if price > 0 {
                revenue += price;
                let category = (e as u32 >> 24) as u8;
                match sc.categories.iter_mut().find(|(c, _)| *c == category) {
                    Some((_, sum)) => *sum += price,
                    None => sc.categories.push((category, price)),
                }
            }
        }
        r.revenue += revenue;

        // The latest click at or before the start; at one timestamp the first
        // click has the smallest click_id.
        while click_at < clicks.len() && click_ts(clicks[click_at]) <= start {
            if click_ts(clicks[click_at]) != click_ts(clicks[credit]) {
                credit = click_at;
            }
            click_at += 1;
        }
        let campaign = if click_at > 0 && start - click_ts(clicks[credit]) <= REACH {
            click_campaign(clicks[credit])
        } else {
            organic
        };
        let c = &mut out.counts[campaign * lk.days + day];
        c.sessions += 1;
        c.purchasing += (revenue > 0) as u64;
        c.revenue += revenue;

        if !clicks.is_empty() {
            sc.starts.push(start);
            sc.days.push(day as u32);
        }
    }
    if let Some(r) = row.take() {
        flush(r, &mut sc.categories, &mut out.rows, &mut out.slab);
    }
    if clicks.is_empty() {
        return;
    }
    assists(clicks, lk, sc, out);
}

/// Assists: every campaign with a click in [start - 7 days, start] assists
/// the session starting at `start` once, counted on the session's day.
fn assists(clicks: &[ClickItem], lk: &Lookups, sc: &mut Scratch, out: &mut Output) {
    let n = sc.starts.len();
    let campaigns = lk.channels.len();
    if n <= FEW_SESSIONS {
        // Few sessions: look at each session's window of clicks.
        if sc.assisted_by.len() < campaigns {
            sc.assisted_by.resize(campaigns, 0);
        }
        let (mut hi, mut lo) = (0, 0);
        for j in 0..n {
            let start = sc.starts[j];
            while hi < clicks.len() && click_ts(clicks[hi]) <= start {
                hi += 1;
            }
            while lo < hi && click_ts(clicks[lo]) + REACH < start {
                lo += 1;
            }
            if lo < hi {
                sc.serial += 1;
                let day = sc.days[j] as usize;
                for click in &clicks[lo..hi] {
                    let slot = &mut sc.assisted_by[click_campaign(*click)];
                    if *slot != sc.serial {
                        *slot = sc.serial;
                        out.counts[click_campaign(*click) * lk.days + day].assisted += 1;
                    }
                }
            }
        }
        return;
    }

    // Many sessions: per campaign, merge [click, click + 7 days] intervals;
    // `clicks` are sorted by time, so each click only adds the part of its
    // interval beyond the previous one, and every session in it is assisted.
    sc.day_end.clear();
    sc.day_end.resize(n, n as u32);
    for k in (0..n.saturating_sub(1)).rev() {
        sc.day_end[k] = if sc.days[k] == sc.days[k + 1] { sc.day_end[k + 1] } else { k as u32 + 1 };
    }
    if sc.covered.len() < campaigns {
        sc.covered.resize(campaigns, (0, 0));
    }
    // Both ends move forward with the click time: `from` is the first
    // session starting at or after the click, `to` the first one after
    // its 7 days.
    let (mut from, mut to) = (0, 0);
    for c in clicks {
        while from < n && sc.starts[from] < click_ts(*c) {
            from += 1;
        }
        let hi = click_ts(*c) + REACH;
        while to < n && sc.starts[to] <= hi {
            to += 1;
        }
        let slot = &mut sc.covered[click_campaign(*c)];
        // Sessions up to `slot.1` already count for this campaign.
        let mut a = if slot.0 != 0 && click_ts(*c) <= slot.0 { slot.1 as usize } else { from };
        *slot = (hi, to as u32);
        while a < to {
            let next = (sc.day_end[a] as usize).min(to);
            out.counts[click_campaign(*c) * lk.days + sc.days[a] as usize].assisted += (next - a) as u64;
            a = next;
        }
    }
    for c in clicks {
        sc.covered[click_campaign(*c)] = (0, 0);
    }
}

fn flush(mut r: UserRow, categories: &mut Vec<(u8, u64)>, rows: &mut Bucket<UserRow>, slab: &mut Slab) {
    // Highest sum; categories are indexed in byte order, so ties go to the lowest index.
    r.top = categories.iter().max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0))).map_or(NO_CATEGORY, |c| c.0);
    categories.clear();
    rows.push(slab, r);
}

/// The sessions pass for one person bucket.
fn sessions_bucket(events: &[&[Event]], clicks: &[&[Click]], lk: &Lookups, st: &mut Sessions) {
    let n: usize = events.iter().map(|s| s.len()).sum();
    if n == 0 {
        return;
    }
    // Dense local index per person, and a counting sort by it.
    st.local.reset(n / 8 + 16);
    st.idx.clear();
    st.count.clear();
    for s in events {
        for e in s.iter() {
            let i = st.local.insert(e.person);
            if i as usize == st.count.len() {
                st.count.push(0);
            }
            st.count[i as usize] += 1;
            st.idx.push(i);
        }
    }
    let persons = st.local.persons.len();
    st.start.clear();
    st.start.reserve(persons + 1);
    let mut total = 0u32;
    for &c in &st.count {
        st.start.push(total);
        total += c;
    }
    st.start.push(total);
    // count becomes the write position of each person.
    st.count.copy_from_slice(&st.start[..persons]);
    st.items.clear();
    st.items.resize(n, 0);
    let mut k = 0;
    for s in events {
        for e in s.iter() {
            let i = st.idx[k] as usize;
            st.items[st.count[i] as usize] = (e.ts as u64) << 32 | e.pc as u64;
            st.count[i] += 1;
            k += 1;
        }
    }

    // Clicks of the people with events, grouped the same way.
    st.click_idx.clear();
    st.count.clear();
    st.count.resize(persons, 0);
    let mut interned = false;
    for s in clicks {
        for c in s.iter() {
            let i = st.local.get(c.person).unwrap_or(u32::MAX);
            if i != u32::MAX {
                st.count[i as usize] += 1;
                interned |= c.cid & INTERNED != 0;
            }
            st.click_idx.push(i);
        }
    }
    st.click_start.clear();
    let mut total = 0u32;
    for &c in &st.count {
        st.click_start.push(total);
        total += c;
    }
    st.click_start.push(total);
    st.count.copy_from_slice(&st.click_start[..persons]);
    st.click_items.clear();
    st.click_items.resize(total as usize, 0);
    let mut k = 0;
    for s in clicks {
        for c in s.iter() {
            let i = st.click_idx[k];
            k += 1;
            if i != u32::MAX {
                st.click_items[st.count[i as usize] as usize] =
                    (c.ts as u128) << 96 | (c.cid as u128) << 32 | c.campaign as u128;
                st.count[i as usize] += 1;
            }
        }
    }

    let max = (lk.window_end - 1 - lk.base) as u32;
    for p in 0..persons {
        let ev = &st.items[st.start[p] as usize..st.start[p + 1] as usize];
        let cl = &mut st.click_items[st.click_start[p] as usize..st.click_start[p + 1] as usize];
        let mut m = cl.len();
        if m > 1 {
            if interned {
                cl.sort_unstable_by(|&a, &b| {
                    click_ts(a).cmp(&click_ts(b)).then_with(|| lk.ids.cmp(click_cid(a), click_cid(b)))
                });
            } else {
                cl.sort_unstable();
            }
            // Copies of a click are identical, so they are now adjacent.
            m = 1;
            for r in 1..cl.len() {
                if click_cid(cl[r]) != click_cid(cl[m - 1]) {
                    cl[m] = cl[r];
                    m += 1;
                }
            }
        }
        let sessions = st.dense_sessions.build(ev, max);
        person(st.local.persons[p], sessions, &cl[..m], lk, &mut st.scratch, &mut st.out);
    }
}

fn write_tables(out: &Path, outputs: &[Output], counts: &[Counts], lk: &Lookups, threads: usize) {
    let user_dir = out.join("user_daily");
    let campaign_dir = out.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&campaign_dir).unwrap();

    let total: usize = outputs.iter().map(|o| o.rows.len()).sum();
    // Pieces of whole chunks, about `per` rows each: small enough that every
    // thread reuses its column buffers.
    let per = total.div_ceil(threads * 2).clamp(1, 1 << 16);
    let mut pieces: Vec<(usize, Vec<&[UserRow]>)> = Vec::new();
    let (mut rows_in, mut parts): (usize, Vec<&[UserRow]>) = (0, Vec::new());
    for o in outputs {
        for s in o.rows.slices() {
            parts.push(s);
            rows_in += s.len();
            if rows_in >= per {
                pieces.push((rows_in, std::mem::take(&mut parts)));
                rows_in = 0;
            }
        }
    }
    if rows_in > 0 || pieces.is_empty() {
        pieces.push((rows_in, parts));
    }
    #[derive(Default)]
    struct Columns {
        ids: Vec<u8>,
        day: Vec<i32>,
        sessions: Vec<u32>,
        events: Vec<u32>,
        revenue: Vec<i64>,
        levels: Vec<u8>,
        top: Vec<u8>,
    }
    run(threads, pieces.len(), Columns::default, |c, p| {
        let (n, parts) = &pieces[p];
        let Columns { ids, day, sessions, events, revenue, levels, top } = c;
        for v in [&mut *ids, &mut *top, &mut *levels] {
            v.clear();
        }
        day.clear();
        sessions.clear();
        events.clear();
        revenue.clear();
        // Definition levels: a 4-byte length, then one bit-packed run of
        // one bit per row (set when the row has a category).
        let groups = n.div_ceil(8);
        levels.extend_from_slice(&[0; 4]);
        let mut header = [0u8; 10];
        let mut v = ((groups as u64) << 1) | 1;
        let mut h = 0;
        while v >= 0x80 {
            header[h] = (v as u8) | 0x80;
            v >>= 7;
            h += 1;
        }
        header[h] = v as u8;
        levels.extend_from_slice(&header[..=h]);
        let bits_at = levels.len();
        levels.resize(bits_at + groups, 0);
        let mut i = 0;
        for r in parts.iter().flat_map(|s| s.iter()) {
            let start = ids.len();
            ids.extend_from_slice(&[0; 4]);
            lk.ids.bytes(r.person, ids);
            let len = (ids.len() - start - 4) as u32;
            ids[start..start + 4].copy_from_slice(&len.to_le_bytes());
            day.push(r.day);
            sessions.push(r.sessions);
            events.push(r.events);
            revenue.push(r.revenue as i64);
            if r.top != NO_CATEGORY {
                let name = &lk.categories[r.top as usize];
                top.extend_from_slice(&(name.len() as u32).to_le_bytes());
                top.extend_from_slice(name);
                levels[bits_at + i / 8] |= 1 << (i % 8);
            }
            i += 1;
        }
        let level_bytes = (levels.len() - 4) as u32;
        levels[..4].copy_from_slice(&level_bytes.to_le_bytes());
        parquet::write(
            &user_dir.join(format!("part-{p:05}.parquet")),
            &[
                Column { name: "user_id", values: Values::PlainStr { rows: *n, bytes: ids } },
                Column { name: "day", values: Values::Date(day) },
                Column { name: "sessions", values: Values::U32(sessions) },
                Column { name: "events", values: Values::U32(events) },
                Column { name: "revenue_cents", values: Values::I64(revenue) },
                Column { name: "top_category", values: Values::OptStrPage { rows: *n, levels, values: top } },
            ],
        )
        .expect("write user_daily");
    });

    let (mut id, mut channel, mut day) = (Vec::new(), Vec::new(), Vec::new());
    let (mut sessions, mut purchasing, mut revenue, mut assisted) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let organic = lk.channels.len();
    for campaign in 0..=organic {
        for d in 0..lk.days {
            let c = counts[campaign * lk.days + d];
            if c.sessions == 0 && c.assisted == 0 {
                continue;
            }
            let (name, ch): (&[u8], &[u8]) = if campaign == organic {
                (b"organic", b"organic")
            } else {
                (&lk.campaign_names[campaign], &lk.channels[campaign])
            };
            id.push(name);
            channel.push(ch);
            day.push((lk.first_day + d as i64) as i32);
            sessions.push(c.sessions as i64);
            purchasing.push(c.purchasing as i64);
            revenue.push(c.revenue as i64);
            assisted.push(c.assisted as i64);
        }
    }
    parquet::write(
        &campaign_dir.join("part-00000.parquet"),
        &[
            Column { name: "campaign_id", values: Values::Str(&id) },
            Column { name: "channel", values: Values::Str(&channel) },
            Column { name: "day", values: Values::Date(&day) },
            Column { name: "sessions", values: Values::I64(&sessions) },
            Column { name: "purchasing_sessions", values: Values::I64(&purchasing) },
            Column { name: "revenue_cents", values: Values::I64(&revenue) },
            Column { name: "assisted_sessions", values: Values::I64(&assisted) },
        ],
    )
    .expect("write campaign_daily");
}

struct Parsed {
    raw: Vec<Bucket<Raw>>,
    clicks: Bucket<Click>,
    slab: Slab,
}

struct Routed {
    events: Vec<Bucket<Event>>,
    clicks: Vec<Bucket<Click>>,
    dedup: Dedup,
    slab: Slab,
}

fn main() {
    let clock = Instant::now();
    let lap = |what: &str| eprintln!("{:>8.3}s {what}", clock.elapsed().as_secs_f64());
    // Arguments win over the environment, as run_fixtures.py passes both.
    let args: Vec<String> = std::env::args().collect();
    let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let data = PathBuf::from(args.get(1).cloned().unwrap_or_else(|| env("DATA_DIR", "/data")));
    let out = PathBuf::from(args.get(2).cloned().unwrap_or_else(|| env("OUTPUT_DIR", "/output")));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()))
        .max(1);

    let Some(lk) = lookups(&data) else {
        eprintln!("dataset outside the fast layout's limits: general pipeline");
        legacy::run_legacy(data, out, threads);
        return;
    };
    lap("lookups");

    // 1. Parse. Link chunks come first so the names are ready early.
    let link_files = mapped_csvs(&data.join("links"));
    let event_files = mapped_csvs(&data.join("events"));
    let click_files = mapped_csvs(&data.join("clicks"));
    let link_chunks = chunks_of(&link_files);
    let event_chunks = chunks_of(&event_files);
    let click_chunks = chunks_of(&click_files);
    let (nl, ne) = (link_chunks.len(), event_chunks.len());
    let link_pairs = Mutex::new(Vec::new());
    let links_left = AtomicUsize::new(nl);
    let names = Mutex::new(None);
    let parsed = run(
        threads,
        nl + ne + click_chunks.len(),
        || Parsed { raw: (0..BUCKETS).map(|_| Bucket::new()).collect(), clicks: Bucket::new(), slab: Slab::new() },
        |st, t| {
            if t < nl {
                let (data, s, e) = link_chunks[t];
                let mut pairs = Vec::new();
                csv::rows::<2>(data, s, e, |f| {
                    if !f[0].is_empty() && !f[1].is_empty() {
                        pairs.push((lk.ids.key(f[0]), lk.ids.key(f[1])));
                    }
                });
                csv::release(data, s, e);
                link_pairs.lock().unwrap().push(pairs);
                if links_left.fetch_sub(1, AcqRel) == 1 {
                    let all = std::mem::take(&mut *link_pairs.lock().unwrap());
                    let built = Names::build(all, &lk.ids);
                    *names.lock().unwrap() = Some(built);
                }
            } else if t < nl + ne {
                let (data, s, e) = event_chunks[t - nl];
                csv::rows::<7>(data, s, e, |f| parse_event(f, &lk, &mut st.raw, &mut st.slab));
                csv::release(data, s, e);
            } else {
                let (data, s, e) = click_chunks[t - nl - ne];
                csv::rows::<4>(data, s, e, |f| parse_click(f, &lk, &mut st.clicks, &mut st.slab));
                csv::release(data, s, e);
            }
        },
    );
    let names = names.into_inner().unwrap().unwrap_or_else(Names::empty);
    // Parsed data owns its IDs and timestamps. The input pages can be
    // released before routing allocates the next set of buckets.
    drop((link_chunks, event_chunks, click_chunks));
    drop((link_files, event_files, click_files));
    lap("parse");

    // 2. Route: dedup event buckets, and clicks, to person buckets.
    let mut by_bucket: Vec<Vec<Bucket<Raw>>> = (0..BUCKETS).map(|_| Vec::with_capacity(threads)).collect();
    let mut click_lists = Vec::with_capacity(parsed.len());
    for p in parsed {
        for (b, v) in p.raw.into_iter().enumerate() {
            by_bucket[b].push(v);
        }
        click_lists.push(p.clicks);
    }
    let sizes: Vec<usize> = by_bucket.iter().map(|b| b.iter().map(Bucket::len).sum()).collect();
    let order = by_size(&sizes);
    let by_bucket: Vec<Mutex<Vec<Bucket<Raw>>>> = by_bucket.into_iter().map(Mutex::new).collect();
    let click_slices: Vec<&[Click]> = click_lists.iter().flat_map(|l| l.slices()).collect();
    let routed = run(
        threads,
        BUCKETS + click_slices.len(),
        || Routed {
            events: (0..BUCKETS).map(|_| Bucket::new()).collect(),
            clicks: (0..BUCKETS).map(|_| Bucket::new()).collect(),
            dedup: Dedup::default(),
            slab: Slab::new(),
        },
        |Routed { events, clicks, dedup, slab }, t| {
            if t < BUCKETS {
                let b = order[t];
                let copies = std::mem::take(&mut *by_bucket[b].lock().unwrap());
                let mut batch = Batch::new((0u32, 0u32));
                let mut sink = |person: Key, (ts, pc): (u32, u32)| events[bucket(person)].push(slab, Event { person, ts, pc });
                let slices: Vec<&[Raw]> = copies.iter().flat_map(|c| c.slices()).collect();
                dedup.winners(sizes[b], &slices, |e| {
                    if e.ts != INVALID {
                        batch.push(&names, e.uid, (e.ts, e.pc), &mut sink);
                    }
                });
                batch.flush(&names, &mut sink);
                // Nothing reads these copies again: their memory serves the
                // events and clicks routed next, already faulted in.
                drop(slices);
                copies.into_iter().for_each(Bucket::recycle);
            } else {
                let mut batch = Batch::new((0u64, 0u32, 0u32));
                let mut sink = |person: Key, (cid, ts, campaign): (Key, u32, u32)| {
                    clicks[bucket(person)].push(slab, Click { person, cid, ts, campaign })
                };
                for c in click_slices[t - BUCKETS] {
                    batch.push(&names, c.person, (c.cid, c.ts, c.campaign), &mut sink);
                }
                batch.flush(&names, &mut sink);
            }
        },
    );
    drop(click_slices);
    drop(click_lists);
    lap("dedup + route");

    // 3. Sessions per person bucket.
    let slots = (lk.channels.len() + 1) * lk.days;
    let person_sizes: Vec<usize> =
        (0..BUCKETS).map(|b| routed.iter().map(|r| r.events[b].len()).sum()).collect();
    let order = by_size(&person_sizes);
    let results = run(
        threads,
        BUCKETS,
        || Sessions {
            local: LocalIndex { keys: Vec::new(), index: Vec::new(), mask: 0, persons: Vec::new() },
            idx: Vec::new(),
            count: Vec::new(),
            start: Vec::new(),
            items: Vec::new(),
            click_idx: Vec::new(),
            click_start: Vec::new(),
            click_items: Vec::new(),
            scratch: Scratch::default(),
            dense_sessions: BinnedSessions::default(),
            out: Output { rows: Bucket::new(), slab: Slab::new(), counts: vec![Counts::default(); slots] },
        },
        |st, t| {
            let b = order[t];
            let events: Vec<&[Event]> = routed.iter().flat_map(|r| r.events[b].slices()).collect();
            let clicks: Vec<&[Click]> = routed.iter().flat_map(|r| r.clicks[b].slices()).collect();
            sessions_bucket(&events, &clicks, &lk, st);
        },
    );
    lap("sessions");

    let outputs: Vec<Output> = results.into_iter().map(|s| s.out).collect();
    let mut counts = vec![Counts::default(); slots];
    for o in &outputs {
        for (total, c) in counts.iter_mut().zip(&o.counts) {
            total.sessions += c.sessions;
            total.purchasing += c.purchasing;
            total.revenue += c.revenue;
            total.assisted += c.assisted;
        }
    }
    write_tables(&out, &outputs, &counts, &lk, threads);
    lap("write");
    // Everything is written and closed: skip freeing the buffers.
    std::process::exit(0);
}

#[cfg(test)]
mod route_tests {
    use super::*;

    fn lcg(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    /// Dedup keeps the smallest `ingest` per ID, also when most IDs collide on
    /// the same mark bit (tiny `len`) and when the mark table is roomy.
    #[test]
    fn dedup_keeps_earliest_copy_under_collisions() {
        let mut rng = 7u64;
        for &(ids, copies, len_hint) in &[(40usize, 300usize, 3usize), (500, 700, 700), (2000, 2100, 2100), (5, 400, 1)] {
            let raws: Vec<Raw> = (0..copies)
                .map(|i| {
                    let eid = if i < ids { i as Key + 1 } else { lcg(&mut rng) % ids as u64 + 1 };
                    let ingest = (lcg(&mut rng) % 50) as i64;
                    // Equal ingest means identical copies: derive the rest from it.
                    Raw { eid, uid: eid * 3, ingest, ts: ingest as u32 * 7 + eid as u32, pc: ingest as u32 }
                })
                .collect();
            let mut expected: std::collections::HashMap<Key, Raw> = std::collections::HashMap::new();
            for r in &raws {
                let best = expected.entry(r.eid).or_insert(*r);
                if r.ingest < best.ingest {
                    *best = *r;
                }
            }
            let (a, b) = raws.split_at(raws.len() / 3);
            let mut got = Vec::new();
            Dedup::default().winners(len_hint, &[a, b], |e| got.push((e.eid, e.ingest, e.ts, e.pc)));
            got.sort_unstable();
            let mut want: Vec<_> = expected.values().map(|e| (e.eid, e.ingest, e.ts, e.pc)).collect();
            want.sort_unstable();
            assert_eq!(got, want);
        }
    }

    /// Every linked ID resolves to the smallest ID of its chain; others to themselves.
    #[test]
    fn names_resolve_through_filter_and_collisions() {
        let ids = Interner::new();
        let mut pairs = Vec::new();
        let mut expected = Vec::new();
        let mut rng = 11u64;
        let mut next = 0u32;
        for group in 0..3000u32 {
            let len = 2 + (lcg(&mut rng) % 5) as u32 + if group % 500 == 0 { 300 } else { 0 };
            let members: Vec<u32> = (0..len).map(|_| { next += 1 + (lcg(&mut rng) % 3) as u32; next }).collect();
            let id = |n: u32| format!("u{:07}", n.wrapping_mul(2654435761) % 10_000_000);
            let mut shuffled = members.clone();
            for i in (1..shuffled.len()).rev() {
                shuffled.swap(i, (lcg(&mut rng) % (i as u64 + 1)) as usize);
            }
            for w in shuffled.windows(2) {
                pairs.push((ids.key(id(w[0]).as_bytes()), ids.key(id(w[1]).as_bytes())));
            }
            let name = members.iter().map(|&m| id(m)).min().unwrap();
            for &m in &members {
                expected.push((ids.key(id(m).as_bytes()), ids.key(name.as_bytes())));
            }
        }
        let names = Names::build(vec![pairs], &ids);
        for &(k, name) in &expected {
            assert_eq!(names.resolve(k, names.locate(k)), name);
        }
        for n in 0..5000u32 {
            let k = ids.key(format!("x{n}").as_bytes());
            assert_eq!(names.resolve(k, names.locate(k)), k);
        }
        // Through the sliding window, in order.
        let mut batch = Batch::new(0u32);
        let mut got = Vec::new();
        for (i, &(k, _)) in expected.iter().enumerate() {
            batch.push(&names, k, i as u32, &mut |name, i| got.push((i, name)));
        }
        batch.flush(&names, &mut |name, i| got.push((i, name)));
        assert_eq!(got.len(), expected.len());
        for (i, name) in got {
            assert_eq!(name, expected[i as usize].1);
        }
    }
}

/// `lookups` reads products.csv through the row scanner: categories are
/// numbered in byte order with `unknown` among them, quotes are unwrapped,
/// the first row of a repeated product wins, and rows without a product_id
/// are skipped.
#[cfg(test)]
#[test]
fn lookups_number_categories_in_byte_order() {
    let dir = std::env::temp_dir().join(format!("lookups-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("manifest.json"),
        r#"{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("products.csv"),
        "product_id,category\np1,toys\n\"p2\",\"books\"\np3,toys\n,ghost\np1,games\np4,\np5,Books\np6,unknown\nbad\"row,x\n",
    )
    .unwrap();
    std::fs::write(dir.join("campaigns.csv"), "campaign_id,channel\nc_1,email\n").unwrap();
    let lk = lookups(&dir).unwrap();
    let names: Vec<&[u8]> = lk.categories.iter().map(|c| c.as_slice()).collect();
    // "" (p4) sorts first, then byte order. Categories of repeated rows count.
    assert_eq!(names, [&b""[..], b"Books", b"books", b"games", b"toys", b"unknown"]);
    assert_eq!(lk.unknown, 5);
    let category = |p: &[u8]| lk.products.get(&lk.ids.key(p)).copied();
    assert_eq!(category(b"p1"), Some(4));
    assert_eq!(category(b"p2"), Some(2));
    assert_eq!(category(b"p3"), Some(4));
    assert_eq!(category(b"p4"), Some(0));
    assert_eq!(category(b"p5"), Some(1));
    assert_eq!(category(b"p6"), Some(5));
    assert_eq!(category(b"ghost"), None);
    std::fs::remove_dir_all(&dir).unwrap();
}
