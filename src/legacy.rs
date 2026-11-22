//! The general pipeline (kept as the fallback): parallel buckets.
//!
//! 1. Links: parse, union-find, name each person by their smallest ID.
//! 2. Events and clicks: parse every file in chunks. Events go to buckets by
//!    event_id hash (dedup needs all copies together); valid clicks go
//!    straight to buckets by person hash.
//! 3. Per event bucket: skip exact lookups for IDs with unique hash bits,
//!    resolve the remaining IDs exactly, and route valid winners by person.
//! 4. Per person bucket: sessions, the crediting click, assists, and the
//!    user_daily rows and campaign_daily counters.
//! 5. Write both tables as Parquet.


use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

use crate::key::{hash, Interner, Key, KeyMap};
use crate::{csv, ts};
use crate::parquet::{self, Column, Values};
use crate::ts::DAY;

const GAP: i64 = 1_800_000;
const REACH: i64 = 7 * DAY;
const BUCKETS: usize = 512;
const CHUNK: usize = 4 << 20;
const NO_CATEGORY: u16 = u16::MAX;

/// One copy of an event, before dedup. `uid` is 0 unless the copy is valid.
#[derive(Clone, Copy)]
struct RawEvent {
    eid: Key,
    uid: Key,
    ts: i64,
    ingest: i64,
    price: u32,
    category: u16,
    valid: bool,
}

/// A surviving event: `price` is 0 unless it is a purchase.
#[derive(Clone, Copy)]
struct Event {
    person: Key,
    ts: i64,
    price: u32,
    category: u16,
}

#[derive(Clone, Copy)]
struct Click {
    person: Key,
    cid: Key,
    ts: i64,
    campaign: u32,
}

struct UserRow {
    person: Key,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u16,
}

/// campaign_daily counters for one (campaign, day).
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
    /// product key -> category index; categories sorted byte-wise.
    products: KeyMap<u16>,
    categories: Vec<Vec<u8>>,
    unknown: u16,
    /// campaign key -> index; `organic` is index `channels.len()`.
    campaigns: KeyMap<u32>,
    campaign_names: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    first_day: i64,
    days: usize,
}

/// Runs `work` on task indexes 0..tasks across `threads` threads, each with
/// its own state from `init`; returns the states.
fn run<S: Send>(
    threads: usize,
    tasks: usize,
    init: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    if tasks == 0 {
        return Vec::new();
    }
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
        let handles: Vec<_> = (1..threads.min(tasks)).map(|_| scope.spawn(&worker)).collect();
        let here = worker();
        std::iter::once(here).chain(handles.into_iter().map(|h| h.join().unwrap())).collect()
    })
}

#[inline]
fn bucket(k: Key) -> usize {
    (hash(k) >> 55) as usize % BUCKETS
}

#[derive(Clone, Copy, Default)]
struct Alias {
    uid: Key,
    person: Key,
    bucket: usize,
}

struct AliasCache([Alias; 2048]);

impl AliasCache {
    fn new() -> Self {
        Self([Alias::default(); 2048])
    }

    #[inline]
    fn resolve(&mut self, uid: Key, names: &KeyMap<Key>) -> (Key, usize) {
        let i = ((uid >> 4) ^ (uid >> 23)) as usize & (self.0.len() - 1);
        let entry = &mut self.0[i];
        if entry.uid != uid {
            let person = *names.get(&uid).unwrap_or(&uid);
            *entry = Alias { uid, person, bucket: bucket(person) };
        }
        (entry.person, entry.bucket)
    }
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

fn lookups(data: &Path) -> Lookups {
    // A distinct long ID takes at least 34 bytes of input, so a table for
    // input / 40 IDs never counts as crowded. A crowded table sends new IDs to
    // a locked map while other threads may still add the same ID to the table:
    // one ID, two keys, and wrong people at high thread counts.
    let input: u64 = ["links", "events", "clicks"]
        .iter()
        .flat_map(|d| csv::list(&data.join(d)))
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();
    let ids = Interner::with_capacity((input / 40).min(1 << 28) as usize);
    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let window_start = manifest_time(&manifest, "window_start");
    let window_end = manifest_time(&manifest, "window_end");

    let product_rows = lookup_rows(&data.join("products.csv"));
    let mut categories: Vec<Vec<u8>> = product_rows.iter().map(|(_, c)| c.clone()).collect();
    categories.push(b"unknown".to_vec());
    categories.sort_unstable();
    categories.dedup();
    let index = |c: &[u8]| categories.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u16;
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
    Lookups {
        ids,
        window_start,
        window_end,
        products,
        categories,
        unknown,
        campaigns,
        campaign_names,
        channels,
        first_day,
        days,
    }
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

/// Linked IDs: key -> the person's name, for every ID whose name is not itself.
fn link_names(dir: &Path, lk: &Lookups, threads: usize) -> KeyMap<Key> {
    let mappings = mapped_csvs(dir);
    let chunks = chunks_of(&mappings);
    let pairs = run(
        threads,
        chunks.len(),
        || (Vec::new(), csv::Scanner),
        |(out, scanner): &mut (Vec<(Key, Key)>, csv::Scanner), t| {
            let (data, s, e) = chunks[t];
            scanner.scan::<2>(data, s, e, |f| {
                if let (Some(a), Some(b)) = (f[0], f[1]) {
                    out.push((lk.ids.key(a), lk.ids.key(b)));
                }
            });
        },
    );

    let mut index: KeyMap<u32> = KeyMap::default();
    let mut keys: Vec<Key> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut id = |k: Key, keys: &mut Vec<Key>, parent: &mut Vec<u32>| {
        *index.entry(k).or_insert_with(|| {
            keys.push(k);
            parent.push(keys.len() as u32 - 1);
            keys.len() as u32 - 1
        })
    };
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let up = parent[parent[x as usize] as usize];
            parent[x as usize] = up;
            x = up;
        }
        x
    }
    for (list, _) in &pairs {
        for &(a, b) in list {
            let (ia, ib) = (id(a, &mut keys, &mut parent), id(b, &mut keys, &mut parent));
            let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
            if ra != rb {
                parent[rb as usize] = ra;
            }
        }
    }
    let mut name: Vec<Key> = vec![0; keys.len()];
    for i in 0..keys.len() {
        let r = find(&mut parent, i as u32) as usize;
        if name[r] == 0 || lk.ids.cmp(keys[i], name[r]).is_lt() {
            name[r] = keys[i];
        }
    }
    let mut names = KeyMap::default();
    for i in 0..keys.len() {
        let n = name[find(&mut parent, i as u32) as usize];
        if n != keys[i] {
            names.insert(keys[i], n);
        }
    }
    names
}

fn parse_event(f: &[Option<&[u8]>; 7], lk: &Lookups, out: &mut [Vec<RawEvent>]) {
    let Some(eid) = f[0] else { return };
    let ingest = f[3].and_then(ts::parse).unwrap_or(i64::MAX);
    let mut event = RawEvent { eid: lk.ids.key(eid), uid: 0, ts: 0, ingest, price: 0, category: NO_CATEGORY, valid: false };
    'valid: {
        let (Some(uid), Some(product)) = (f[1], f[5]) else { break 'valid };
        let Some(ts) = f[2].and_then(ts::parse) else { break 'valid };
        if ts < lk.window_start || ts >= lk.window_end || ingest.saturating_sub(ts) > DAY {
            break 'valid;
        }
        match f[4] {
            Some(b"view") | Some(b"cart") => {}
            Some(b"purchase") => {
                match f[6].and_then(ts::digits) {
                    Some(p) if (1..=10_000_000).contains(&p) => event.price = p as u32,
                    _ => break 'valid,
                }
                event.category = *lk.products.get(&lk.ids.key(product)).unwrap_or(&lk.unknown);
            }
            _ => break 'valid,
        }
        event.uid = lk.ids.key(uid);
        event.ts = ts;
        event.valid = true;
    }
    out[bucket(event.eid)].push(event);
}

fn parse_click(
    f: &[Option<&[u8]>; 4], lk: &Lookups, names: &KeyMap<Key>, aliases: &mut AliasCache, out: &mut [Vec<Click>],
) {
    let (Some(cid), Some(uid), Some(campaign)) = (f[0], f[1], f[2]) else { return };
    let Some(ts) = f[3].and_then(ts::parse) else { return };
    if ts < lk.window_start - REACH || ts >= lk.window_end {
        return;
    }
    let Some(&campaign) = lk.campaigns.get(&lk.ids.key(campaign)) else { return };
    let uid = lk.ids.key(uid);
    let (person, part) = aliases.resolve(uid, names);
    out[part].push(Click { person, cid: lk.ids.key(cid), ts, campaign });
}

/// Task order: biggest first, so one large bucket does not finish last.
fn by_size(sizes: impl Iterator<Item = usize>) -> Vec<usize> {
    let sizes: Vec<usize> = sizes.collect();
    let mut order: Vec<usize> = (0..sizes.len()).collect();
    order.sort_unstable_by_key(|&b| std::cmp::Reverse(sizes[b]));
    order
}

#[derive(Default)]
struct Dedup {
    seen: Vec<u64>,
    repeated: Vec<u64>,
    candidates: KeyMap<RawEvent>,
}

impl Dedup {
    fn winners<'a>(
        &mut self,
        len: usize,
        copies: impl Iterator<Item = &'a RawEvent> + Clone,
        mut keep: impl FnMut(&RawEvent),
    ) {
        // Equal IDs always select the same bit. Unique bits need no lookup.
        // Repeated bits, including hash collisions, use the full ID in the map.
        let words = len.div_ceil(4).clamp(1, 1 << 15).next_power_of_two();
        self.seen.resize(words, 0);
        self.seen.fill(0);
        self.repeated.resize(words, 0);
        self.repeated.fill(0);
        self.candidates.clear();
        let position = |id| {
            let h = hash(id) as usize;
            ((h >> 6) & (words - 1), 1 << (h & 63))
        };
        for e in copies.clone() {
            let (word, bit) = position(e.eid);
            if self.seen[word] & bit != 0 {
                self.repeated[word] |= bit;
            }
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

#[cfg(test)]
mod dedup_tests {
    use super::*;

    #[test]
    fn chooses_earliest_exact_id_through_filter_collisions() {
        let a = 1;
        let b = (2..).find(|&id| hash(id) & 63 == hash(a) & 63).unwrap();
        let row = |eid, ingest, valid| RawEvent {
            eid, uid: 1, ts: 0, ingest, price: 0, category: NO_CATEGORY, valid,
        };
        let copies = [row(a, 2, true), row(b, 3, true), row(a, 1, false)];
        let mut winners = Vec::new();
        Dedup::default().winners(copies.len(), copies.iter(), |e| {
            winners.push((e.eid, e.ingest, e.valid));
        });
        winners.sort_unstable();
        assert_eq!(winners, [(a, 1, false), (b, 3, true)]);
    }
}

struct PersonState {
    rows: Vec<UserRow>,
    counts: Vec<Counts>,
    starts: Vec<i64>,
    days: Vec<usize>,
    day_end: Vec<usize>,
    by_campaign: Vec<(u32, i64)>,
    categories: Vec<(u16, u64)>,
}

/// One person's sessions, credit and assists.
fn person(events: &[Event], clicks: &[Click], lk: &Lookups, st: &mut PersonState) {
    let organic = lk.channels.len();
    let person = events[0].person;
    st.starts.clear();
    st.days.clear();
    st.categories.clear();
    let mut row: Option<UserRow> = None;
    let mut i = 0;
    let mut click_at = 0;
    let mut credit = 0;
    while i < events.len() {
        let start = events[i].ts;
        let mut j = i + 1;
        while j < events.len() && events[j].ts - events[j - 1].ts <= GAP {
            j += 1;
        }
        let session = &events[i..j];
        let day_abs = start.div_euclid(DAY);
        let day = (day_abs - lk.first_day) as usize;
        if row.as_ref().map_or(true, |r| r.day as i64 != day_abs) {
            if let Some(r) = row.take() {
                flush(r, &mut st.categories, &mut st.rows);
            }
            row = Some(UserRow { person, day: day_abs as i32, sessions: 0, events: 0, revenue: 0, top: NO_CATEGORY });
        }
        let r = row.as_mut().unwrap();
        r.sessions += 1;
        r.events += session.len() as u32;
        let mut revenue = 0;
        for e in session.iter().filter(|e| e.price > 0) {
            revenue += e.price as u64;
            match st.categories.iter_mut().find(|(c, _)| *c == e.category) {
                Some((_, sum)) => *sum += e.price as u64,
                None => st.categories.push((e.category, e.price as u64)),
            }
        }
        r.revenue += revenue;

        // At each timestamp the first click has the smallest click_id.
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
        let c = &mut st.counts[campaign * lk.days + day];
        c.sessions += 1;
        c.purchasing += (revenue > 0) as u64;
        c.revenue += revenue;

        if !clicks.is_empty() {
            st.starts.push(start);
            st.days.push(day);
        }
        i = j;
    }
    if let Some(r) = row.take() {
        flush(r, &mut st.categories, &mut st.rows);
    }

    if clicks.is_empty() {
        return;
    }
    // Assists: per campaign, merge [click, click + 7 days] intervals; every
    // session starting inside one is assisted once, counted per day.
    let n = st.starts.len();
    st.day_end.clear();
    st.day_end.resize(n, n);
    for k in (0..n.saturating_sub(1)).rev() {
        st.day_end[k] = if st.days[k] == st.days[k + 1] { st.day_end[k + 1] } else { k + 1 };
    }
    st.by_campaign.clear();
    st.by_campaign.extend(clicks.iter().map(|c| (c.campaign, c.ts)));
    st.by_campaign.sort_unstable();
    let mut k = 0;
    while k < st.by_campaign.len() {
        let (campaign, ts) = st.by_campaign[k];
        let (lo, mut hi) = (ts, ts + REACH);
        k += 1;
        while k < st.by_campaign.len() && st.by_campaign[k].0 == campaign && st.by_campaign[k].1 <= hi {
            hi = st.by_campaign[k].1 + REACH;
            k += 1;
        }
        let mut a = st.starts.partition_point(|&s| s < lo);
        let b = st.starts.partition_point(|&s| s <= hi);
        while a < b {
            let next = st.day_end[a].min(b);
            st.counts[campaign as usize * lk.days + st.days[a]].assisted += (next - a) as u64;
            a = next;
        }
    }
}

fn flush(mut r: UserRow, categories: &mut Vec<(u16, u64)>, rows: &mut Vec<UserRow>) {
    // Highest sum; categories are indexed in byte order, so ties go to the lowest index.
    r.top = categories.iter().max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0))).map_or(NO_CATEGORY, |c| c.0);
    categories.clear();
    rows.push(r);
}

fn write_tables(out: &Path, results: &[PersonState], counts: &[Counts], lk: &Lookups, threads: usize) {
    let user_dir = out.join("user_daily");
    let campaign_dir = out.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&campaign_dir).unwrap();

    let total: usize = results.iter().map(|r| r.rows.len()).sum();
    let per = total.div_ceil(threads * 2).max(1);
    let pieces: Vec<&[UserRow]> = if total == 0 {
        vec![&[]]
    } else {
        results.iter().flat_map(|r| r.rows.chunks(per)).collect()
    };
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
        let top: Vec<Option<&[u8]>> = piece
            .iter()
            .map(|r| (r.top != NO_CATEGORY).then(|| lk.categories[r.top as usize].as_slice()))
            .collect();
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

/// The general pipeline: used when the fast path's limits do not hold.
pub fn run_legacy(data: PathBuf, out: PathBuf, threads: usize) {
    let clock = Instant::now();
    let lap = |what: &str| eprintln!("{:>8.3}s legacy {what}", clock.elapsed().as_secs_f64());

    let lk = lookups(&data);

    // Link resolution shares the parsing pool. Its worker can take event
    // chunks once the component names are ready.
    let event_files = mapped_csvs(&data.join("events"));
    let event_chunks = chunks_of(&event_files);
    let names = Mutex::new(None);
    let parsed = run(
        threads,
        event_chunks.len() + 1,
        || (vec![Vec::<RawEvent>::new(); BUCKETS], csv::Scanner),
        |(out, scanner), t| {
            if t == 0 {
                *names.lock().unwrap() = Some(link_names(&data.join("links"), &lk, 1));
            } else {
                let (data, s, e) = event_chunks[t - 1];
                scanner.scan::<7>(data, s, e, |f| parse_event(f, &lk, out));
            }
        },
    );
    drop(event_chunks);
    drop(event_files);
    let names = names.into_inner().unwrap().unwrap();
    lap("links + parse events");

    let click_files = mapped_csvs(&data.join("clicks"));
    let click_chunks = chunks_of(&click_files);
    let clicks_parsed = run(
        threads,
        click_chunks.len(),
        || (vec![Vec::<Click>::new(); BUCKETS], csv::Scanner, AliasCache::new()),
        |(out, scanner, aliases), t| {
            let (data, s, e) = click_chunks[t];
            scanner.scan::<4>(data, s, e, |f| parse_click(f, &lk, &names, aliases, out));
        },
    );
    drop(click_chunks);
    drop(click_files);
    lap("parse clicks");

    // Moving the per-worker buffers into their buckets lets dedup release
    // each buffer as soon as its winning events have been routed.
    let mut by_bucket = vec![Vec::new(); BUCKETS];
    for (worker, _) in parsed {
        for (bucket, records) in by_bucket.iter_mut().zip(worker) {
            bucket.push(records);
        }
    }
    let bucket_sizes: Vec<usize> = by_bucket
        .iter()
        .map(|bucket| bucket.iter().map(Vec::len).sum())
        .collect();
    let by_bucket: Vec<_> = by_bucket.into_iter().map(Mutex::new).collect();
    let order_of_buckets = by_size(bucket_sizes.iter().copied());
    let deduped = run(
        threads,
        BUCKETS,
        || (vec![Vec::<Event>::new(); BUCKETS], Dedup::default(), AliasCache::new()),
        |(out, dedup, aliases), t| {
            let b = order_of_buckets[t];
            let copies = std::mem::take(&mut *by_bucket[b].lock().unwrap());
            dedup.winners(bucket_sizes[b], copies.iter().flatten(), |e| {
                if e.valid {
                    let (person, part) = aliases.resolve(e.uid, &names);
                    out[part].push(Event { person, ts: e.ts, price: e.price, category: e.category });
                }
            });
        },
    );
    lap("dedup");

    let slots = (lk.channels.len() + 1) * lk.days;
    let order = by_size((0..BUCKETS).map(|b| deduped.iter().map(|d| d.0[b].len()).sum()));
    let results = run(
        threads,
        BUCKETS,
        || PersonState {
            rows: Vec::new(),
            counts: vec![Counts::default(); slots],
            starts: Vec::new(),
            days: Vec::new(),
            day_end: Vec::new(),
            by_campaign: Vec::new(),
            categories: Vec::new(),
        },
        |st, t| {
            let b = order[t];
            let mut events: Vec<Event> = Vec::with_capacity(deduped.iter().map(|d| d.0[b].len()).sum());
            for d in &deduped {
                events.extend_from_slice(&d.0[b]);
            }
            events.sort_unstable_by_key(|e| (e.person, e.ts));
            let mut clicks: Vec<Click> = Vec::new();
            for p in &clicks_parsed {
                clicks.extend_from_slice(&p.0[b]);
            }
            clicks.sort_unstable_by(|a, b| {
                a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| lk.ids.cmp(a.cid, b.cid))
            });
            // Copies of a click are identical, so they sit next to each other.
            clicks.dedup_by(|a, b| a.cid == b.cid && a.person == b.person);

            let mut c = 0;
            let mut i = 0;
            while i < events.len() {
                let p = events[i].person;
                let mut j = i + 1;
                while j < events.len() && events[j].person == p {
                    j += 1;
                }
                while c < clicks.len() && clicks[c].person < p {
                    c += 1;
                }
                let mut d = c;
                while d < clicks.len() && clicks[d].person == p {
                    d += 1;
                }
                person(&events[i..j], &clicks[c..d], &lk, st);
                c = d;
                i = j;
            }
        },
    );
    drop(clicks_parsed);
    drop(deduped);
    lap("sessions");

    let mut counts = vec![Counts::default(); slots];
    for r in &results {
        for (total, c) in counts.iter_mut().zip(&r.counts) {
            total.sessions += c.sessions;
            total.purchasing += c.purchasing;
            total.revenue += c.revenue;
            total.assisted += c.assisted;
        }
    }
    write_tables(&out, &results, &counts, &lk, threads);
    lap("write");
}
