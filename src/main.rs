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

mod csv;
mod key;
mod legacy;
mod parquet;
mod ts;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::{AcqRel, Relaxed}};
use std::sync::Mutex;
use std::time::Instant;

use key::{hash, Interner, Key, KeyMap, INTERNED};
use parquet::{Column, Values};
use ts::DAY;

const DAY32: u32 = 86_400_000;
const GAP: u32 = 1_800_000;
const REACH: u32 = 7 * DAY32;
const BUCKETS: usize = 512;
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

#[derive(Clone, Copy)]
struct ClickItem {
    cid: Key,
    ts: u32,
    campaign: u32,
}

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

    let product_rows = lookup_rows(&data.join("products.csv"));
    let mut categories: Vec<Vec<u8>> = product_rows.iter().map(|(_, c)| c.clone()).collect();
    categories.push(b"unknown".to_vec());
    categories.sort_unstable();
    categories.dedup();
    if categories.len() > NO_CATEGORY as usize {
        return None;
    }
    let index = |c: &[u8]| categories.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u8;
    let mut products = KeyMap::default();
    for (p, c) in &product_rows {
        products.entry(ids.key(p)).or_insert(index(c));
    }
    let unknown = index(b"unknown");

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

    #[inline(always)]
    fn prefetch(&self, k: Key) {
        #[cfg(target_arch = "x86_64")]
        // Both indexes are masked to their tables.
        unsafe {
            use std::arch::x86_64::*;
            let (word, bit) = self.filter_bit(k);
            if *self.filter.get_unchecked(word) & bit != 0 {
                _mm_prefetch(self.slots.as_ptr().add(self.slot(k)).cast::<i8>(), _MM_HINT_T0);
            }
        }
    }

    #[inline(always)]
    fn get(&self, k: Key) -> Key {
        let (word, bit) = self.filter_bit(k);
        if self.filter[word] & bit == 0 {
            return k;
        }
        let mut i = self.slot(k);
        loop {
            let [sk, sv] = self.slots[i];
            if sk == k {
                return sv;
            }
            if sk == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
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

struct Batch<T: Copy> {
    keys: [Key; WINDOW],
    items: [T; WINDOW],
    pushed: usize,
    done: usize,
}

impl<T: Copy> Batch<T> {
    fn new(fill: T) -> Self {
        Batch { keys: [0; WINDOW], items: [fill; WINDOW], pushed: 0, done: 0 }
    }

    #[inline(always)]
    fn push(&mut self, names: &Names, k: Key, item: T, sink: &mut impl FnMut(Key, T)) {
        if self.pushed - self.done == WINDOW {
            let i = self.done % WINDOW;
            sink(names.get(self.keys[i]), self.items[i]);
            self.done += 1;
        }
        names.prefetch(k);
        let i = self.pushed % WINDOW;
        self.keys[i] = k;
        self.items[i] = item;
        self.pushed += 1;
    }

    #[inline(always)]
    fn flush(&mut self, names: &Names, sink: &mut impl FnMut(Key, T)) {
        while self.done < self.pushed {
            let i = self.done % WINDOW;
            sink(names.get(self.keys[i]), self.items[i]);
            self.done += 1;
        }
    }
}

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], lk: &Lookups, out: &mut [Vec<Raw>]) {
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
    out[bucket(raw.eid)].push(raw);
}

#[inline(always)]
fn parse_click(f: &[&[u8]; 4], lk: &Lookups, out: &mut Vec<Click>) {
    if f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
        return;
    }
    let Some(ts) = ts::parse(f[3]) else { return };
    if ts < lk.base || ts >= lk.window_end {
        return;
    }
    let Some(&campaign) = lk.campaigns.get(&lk.ids.key(f[2])) else { return };
    out.push(Click { person: lk.ids.key(f[1]), cid: lk.ids.key(f[0]), ts: (ts - lk.base) as u32, campaign });
}

/// Task order: biggest first, so one large bucket does not finish last.
fn by_size(sizes: &[usize]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..sizes.len()).collect();
    order.sort_unstable_by_key(|&b| std::cmp::Reverse(sizes[b]));
    order
}

#[derive(Default)]
struct Dedup {
    seen: Vec<u64>,
    repeated: Vec<u64>,
    candidates: KeyMap<Raw>,
}

impl Dedup {
    /// Calls `keep` with the earliest copy of every event_id among `copies`.
    fn winners<'a>(&mut self, len: usize, copies: impl Iterator<Item = &'a Raw> + Clone, mut keep: impl FnMut(&Raw)) {
        // Equal IDs always select the same bit, so a bit set once proves its
        // ID unique. Repeated bits, collisions included, go to an exact map.
        let words = len.div_ceil(4).clamp(1, 1 << 15).next_power_of_two();
        self.seen.clear();
        self.seen.resize(words, 0);
        self.repeated.clear();
        self.repeated.resize(words, 0);
        self.candidates.clear();
        let position = |id| {
            let h = hash(id) as usize;
            ((h >> 6) & (words - 1), 1u64 << (h & 63))
        };
        for e in copies.clone() {
            let (word, bit) = position(e.eid);
            self.repeated[word] |= self.seen[word] & bit;
            self.seen[word] |= bit;
        }
        for e in copies {
            let (word, bit) = position(e.eid);
            if self.repeated[word] & bit == 0 {
                keep(e);
            } else {
                let best = self.candidates.entry(e.eid).or_insert(*e);
                if e.ingest < best.ingest {
                    *best = *e;
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
    click_tmp: Vec<(u32, ClickItem)>,
    click_start: Vec<u32>,
    click_items: Vec<ClickItem>,
    scratch: Scratch,
    out: Output,
}

#[derive(Default)]
struct Scratch {
    starts: Vec<u32>,
    days: Vec<u32>,
    day_end: Vec<u32>,
    by_campaign: Vec<(u32, u32)>,
    categories: Vec<(u8, u64)>,
}

struct Output {
    rows: Vec<UserRow>,
    counts: Vec<Counts>,
}

/// One person's sessions, credit and assists. `events` hold ts << 32 | pc,
/// sorted; `clicks` are sorted by (ts, click_id) without copies.
fn person(person: Key, events: &[u64], clicks: &[ClickItem], lk: &Lookups, sc: &mut Scratch, out: &mut Output) {
    let organic = lk.channels.len();
    sc.starts.clear();
    sc.days.clear();
    sc.categories.clear();
    let mut row: Option<UserRow> = None;
    let mut i = 0;
    let mut click_at = 0;
    let mut credit = 0;
    while i < events.len() {
        let start = (events[i] >> 32) as u32;
        let mut j = i + 1;
        let mut prev = start;
        while j < events.len() {
            let t = (events[j] >> 32) as u32;
            if t - prev > GAP {
                break;
            }
            prev = t;
            j += 1;
        }
        let day_abs = (lk.base + start as i64).div_euclid(DAY);
        let day = (day_abs - lk.first_day) as usize;
        if row.as_ref().map_or(true, |r| r.day as i64 != day_abs) {
            if let Some(r) = row.take() {
                flush(r, &mut sc.categories, &mut out.rows);
            }
            row = Some(UserRow { person, day: day_abs as i32, sessions: 0, events: 0, revenue: 0, top: NO_CATEGORY });
        }
        let r = row.as_mut().unwrap();
        r.sessions += 1;
        r.events += (j - i) as u32;
        let mut revenue = 0u64;
        for &e in &events[i..j] {
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
        while click_at < clicks.len() && clicks[click_at].ts <= start {
            if clicks[click_at].ts != clicks[credit].ts {
                credit = click_at;
            }
            click_at += 1;
        }
        let campaign = if click_at > 0 && start - clicks[credit].ts <= REACH {
            clicks[credit].campaign as usize
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
        i = j;
    }
    if let Some(r) = row.take() {
        flush(r, &mut sc.categories, &mut out.rows);
    }
    if clicks.is_empty() {
        return;
    }

    // Assists: per campaign, merge [click, click + 7 days] intervals; every
    // session starting inside one is assisted once, counted per day.
    let n = sc.starts.len();
    sc.day_end.clear();
    sc.day_end.resize(n, n as u32);
    for k in (0..n.saturating_sub(1)).rev() {
        sc.day_end[k] = if sc.days[k] == sc.days[k + 1] { sc.day_end[k + 1] } else { k as u32 + 1 };
    }
    sc.by_campaign.clear();
    sc.by_campaign.extend(clicks.iter().map(|c| (c.campaign, c.ts)));
    sc.by_campaign.sort_unstable();
    let mut k = 0;
    while k < sc.by_campaign.len() {
        let (campaign, ts) = sc.by_campaign[k];
        let (lo, mut hi) = (ts, ts + REACH);
        k += 1;
        while k < sc.by_campaign.len() && sc.by_campaign[k].0 == campaign && sc.by_campaign[k].1 <= hi {
            hi = sc.by_campaign[k].1 + REACH;
            k += 1;
        }
        let mut a = sc.starts.partition_point(|&s| s < lo);
        let b = sc.starts.partition_point(|&s| s <= hi);
        while a < b {
            let next = (sc.day_end[a] as usize).min(b);
            out.counts[campaign as usize * lk.days + sc.days[a] as usize].assisted += (next - a) as u64;
            a = next;
        }
    }
}

fn flush(mut r: UserRow, categories: &mut Vec<(u8, u64)>, rows: &mut Vec<UserRow>) {
    // Highest sum; categories are indexed in byte order, so ties go to the lowest index.
    r.top = categories.iter().max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0))).map_or(NO_CATEGORY, |c| c.0);
    categories.clear();
    rows.push(r);
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
    st.click_tmp.clear();
    for s in clicks {
        for c in s.iter() {
            if let Some(i) = st.local.get(c.person) {
                st.click_tmp.push((i, ClickItem { cid: c.cid, ts: c.ts, campaign: c.campaign }));
            }
        }
    }
    st.count.clear();
    st.count.resize(persons, 0);
    for &(i, _) in &st.click_tmp {
        st.count[i as usize] += 1;
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
    st.click_items.resize(st.click_tmp.len(), ClickItem { cid: 0, ts: 0, campaign: 0 });
    for &(i, c) in &st.click_tmp {
        st.click_items[st.count[i as usize] as usize] = c;
        st.count[i as usize] += 1;
    }

    for p in 0..persons {
        let ev = &mut st.items[st.start[p] as usize..st.start[p + 1] as usize];
        ev.sort_unstable();
        let cl = &mut st.click_items[st.click_start[p] as usize..st.click_start[p + 1] as usize];
        let mut m = cl.len();
        if m > 1 {
            if cl.iter().any(|c| c.cid & INTERNED != 0) {
                cl.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| lk.ids.cmp(a.cid, b.cid)));
            } else {
                cl.sort_unstable_by_key(|c| (c.ts, c.cid));
            }
            // Copies of a click are identical, so they are now adjacent.
            m = 1;
            for r in 1..cl.len() {
                if cl[r].cid != cl[m - 1].cid {
                    cl[m] = cl[r];
                    m += 1;
                }
            }
        }
        person(st.local.persons[p], ev, &cl[..m], lk, &mut st.scratch, &mut st.out);
    }
}

fn write_tables(out: &Path, outputs: &[Output], counts: &[Counts], lk: &Lookups, threads: usize) {
    let user_dir = out.join("user_daily");
    let campaign_dir = out.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&campaign_dir).unwrap();

    let total: usize = outputs.iter().map(|o| o.rows.len()).sum();
    let per = total.div_ceil(threads * 2).max(1);
    let pieces: Vec<&[UserRow]> =
        if total == 0 { vec![&[]] } else { outputs.iter().flat_map(|o| o.rows.chunks(per)).collect() };
    run(threads, pieces.len(), || (), |_, p| {
        let piece = pieces[p];
        let mut arena = Vec::with_capacity(piece.len() * 12);
        let mut ends = Vec::with_capacity(piece.len());
        for r in piece {
            lk.ids.bytes(r.person, &mut arena);
            ends.push(arena.len());
        }
        let mut ids: Vec<&[u8]> = Vec::with_capacity(piece.len());
        let mut start = 0;
        for &e in &ends {
            ids.push(&arena[start..e]);
            start = e;
        }
        let day: Vec<i32> = piece.iter().map(|r| r.day).collect();
        let sessions: Vec<i64> = piece.iter().map(|r| r.sessions as i64).collect();
        let events: Vec<i64> = piece.iter().map(|r| r.events as i64).collect();
        let revenue: Vec<i64> = piece.iter().map(|r| r.revenue as i64).collect();
        let top: Vec<Option<&[u8]>> =
            piece.iter().map(|r| (r.top != NO_CATEGORY).then(|| lk.categories[r.top as usize].as_slice())).collect();
        parquet::write(
            &user_dir.join(format!("part-{p:05}.parquet")),
            &[
                Column { name: "user_id", values: Values::Str(&ids) },
                Column { name: "day", values: Values::Date(&day) },
                Column { name: "sessions", values: Values::I64(&sessions) },
                Column { name: "events", values: Values::I64(&events) },
                Column { name: "revenue_cents", values: Values::I64(&revenue) },
                Column { name: "top_category", values: Values::OptStr(&top) },
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
    raw: Vec<Vec<Raw>>,
    clicks: Vec<Click>,
}

struct Routed {
    events: Vec<Vec<Event>>,
    clicks: Vec<Vec<Click>>,
    dedup: Dedup,
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
    let event_bytes: usize = event_files.iter().map(|m| m.bytes().len()).sum();
    let per_bucket = event_bytes / 64 / BUCKETS / threads + 16;
    let link_pairs = Mutex::new(Vec::new());
    let links_left = AtomicUsize::new(nl);
    let names = Mutex::new(None);
    let parsed = run(
        threads,
        nl + ne + click_chunks.len(),
        || Parsed { raw: (0..BUCKETS).map(|_| Vec::with_capacity(per_bucket)).collect(), clicks: Vec::new() },
        |st, t| {
            if t < nl {
                let (data, s, e) = link_chunks[t];
                let mut pairs = Vec::new();
                csv::rows::<2>(data, s, e, |f| {
                    if !f[0].is_empty() && !f[1].is_empty() {
                        pairs.push((lk.ids.key(f[0]), lk.ids.key(f[1])));
                    }
                });
                link_pairs.lock().unwrap().push(pairs);
                if links_left.fetch_sub(1, AcqRel) == 1 {
                    let all = std::mem::take(&mut *link_pairs.lock().unwrap());
                    let built = Names::build(all, &lk.ids);
                    *names.lock().unwrap() = Some(built);
                }
            } else if t < nl + ne {
                let (data, s, e) = event_chunks[t - nl];
                csv::rows::<7>(data, s, e, |f| parse_event(f, &lk, &mut st.raw));
            } else {
                let (data, s, e) = click_chunks[t - nl - ne];
                csv::rows::<4>(data, s, e, |f| parse_click(f, &lk, &mut st.clicks));
            }
        },
    );
    let names = names.into_inner().unwrap().unwrap_or_else(Names::empty);
    lap("parse");

    // 2. Route: dedup event buckets, and clicks, to person buckets.
    let mut by_bucket: Vec<Vec<Vec<Raw>>> = (0..BUCKETS).map(|_| Vec::with_capacity(threads)).collect();
    let mut click_lists = Vec::with_capacity(parsed.len());
    for p in parsed {
        for (b, v) in p.raw.into_iter().enumerate() {
            by_bucket[b].push(v);
        }
        click_lists.push(p.clicks);
    }
    let sizes: Vec<usize> = by_bucket.iter().map(|b| b.iter().map(Vec::len).sum()).collect();
    let order = by_size(&sizes);
    let by_bucket: Vec<Mutex<Vec<Vec<Raw>>>> = by_bucket.into_iter().map(Mutex::new).collect();
    const CLICK_SLICE: usize = 1 << 16;
    let click_slices: Vec<&[Click]> = click_lists.iter().flat_map(|l| l.chunks(CLICK_SLICE)).collect();
    let routed = run(
        threads,
        BUCKETS + click_slices.len(),
        || Routed {
            events: (0..BUCKETS).map(|_| Vec::new()).collect(),
            clicks: (0..BUCKETS).map(|_| Vec::new()).collect(),
            dedup: Dedup::default(),
        },
        |Routed { events, clicks, dedup }, t| {
            if t < BUCKETS {
                let b = order[t];
                let copies = std::mem::take(&mut *by_bucket[b].lock().unwrap());
                let mut batch = Batch::new((0u32, 0u32));
                let mut sink = |person: Key, (ts, pc): (u32, u32)| events[bucket(person)].push(Event { person, ts, pc });
                dedup.winners(sizes[b], copies.iter().flatten(), |e| {
                    if e.ts != INVALID {
                        batch.push(&names, e.uid, (e.ts, e.pc), &mut sink);
                    }
                });
                batch.flush(&names, &mut sink);
            } else {
                let mut batch = Batch::new((0u64, 0u32, 0u32));
                let mut sink = |person: Key, (cid, ts, campaign): (Key, u32, u32)| {
                    clicks[bucket(person)].push(Click { person, cid, ts, campaign })
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
            click_tmp: Vec::new(),
            click_start: Vec::new(),
            click_items: Vec::new(),
            scratch: Scratch::default(),
            out: Output { rows: Vec::new(), counts: vec![Counts::default(); slots] },
        },
        |st, t| {
            let b = order[t];
            let events: Vec<&[Event]> = routed.iter().map(|r| r.events[b].as_slice()).collect();
            let clicks: Vec<&[Click]> = routed.iter().map(|r| r.clicks[b].as_slice()).collect();
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
