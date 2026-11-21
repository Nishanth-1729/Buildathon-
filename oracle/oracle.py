#!/usr/bin/env python3
"""
Reference implementation of rules.md in plain Python, written for clarity, not
speed. Used only to cross-check the Rust pipeline on generated seeds.

Usage: oracle.py DATA_DIR OUTPUT_DIR   (writes user_daily/ and campaign_daily/ as Parquet)
"""
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

import polars as pl

DAY_MS = 86_400_000
GAP_MS = 1_800_000
REACH_MS = 7 * DAY_MS
DIGITS = b"0123456789"


def read_lines(path):
    """Every line after the header. Only \\n ends a line."""
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()  # the text after the final \n is not a line
    return lines[1:]


def parse_line(line, n):
    """The line's fields (None for empty), or None if the line is malformed."""
    if line == b"":
        return None
    fields = line.split(b",")
    if len(fields) != n:
        return None
    out = []
    for f in fields:
        if b'"' in f:
            if len(f) >= 2 and f[0] == 34 and f[-1] == 34 and b'"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f if f != b"" else None)
    return out


def rows(folder, n):
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        for line in read_lines(path):
            fields = parse_line(line, n)
            if fields is not None:
                yield fields


def is_digits(s):
    return len(s) > 0 and all(c in DIGITS for c in s)


def days_from_civil(y, m, d):
    return (date(y, m, d) - date(1970, 1, 1)).days if y >= 1 else None


def parse_ts(s):
    """Epoch milliseconds, or None for a bad timestamp."""
    if s is None:
        return None
    if is_digits(s):
        return int(s)
    if len(s) == 20 and s[19:20] == b"Z":
        offset = 0
    elif len(s) == 25 and s[19:20] in (b"+", b"-") and s[22:23] == b":":
        oh, om = s[20:22], s[23:25]
        if not (is_digits(oh) and is_digits(om)):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        offset = (oh * 60 + om) * 60_000 * (1 if s[19:20] == b"+" else -1)
    else:
        return None
    if s[4:5] != b"-" or s[7:8] != b"-" or s[10:11] != b"T" or s[13:14] != b":" or s[16:17] != b":":
        return None
    parts = [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19]]
    if not all(is_digits(p) for p in parts):
        return None
    y, mo, d, h, mi, se = (int(p) for p in parts)
    if not (1 <= mo <= 12 and h <= 23 and mi <= 59 and se <= 59 and d >= 1):
        return None
    try:
        days = days_from_civil(y, mo, d)
    except ValueError:
        return None
    if days is None:
        return None
    return (days * 86400 + h * 3600 + mi * 60 + se) * 1000 - offset


def main():
    data, out = sys.argv[1], sys.argv[2]
    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    w_start = parse_ts(manifest["window_start"].encode())
    w_end = parse_ts(manifest["window_end"].encode())

    category = {}
    for path in [os.path.join(data, "products.csv")]:
        for line in read_lines(path):
            f = parse_line(line, 2)
            if f and f[0] is not None:
                category[f[0]] = f[1] or b""
    channel = {}
    for line in read_lines(os.path.join(data, "campaigns.csv")):
        f = parse_line(line, 2)
        if f and f[0] is not None:
            channel[f[0]] = f[1] or b""

    # Linked IDs: union-find, each root the smallest ID of its set.
    parent = {}

    def find(x):
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in rows(os.path.join(data, "links"), 2):
        if a is None or b is None:
            continue
        ra, rb = find(a), find(b)
        if ra != rb:
            if rb < ra:
                ra, rb = rb, ra
            parent[rb] = ra

    # Events: dedup (smallest ingest_ts), then validate, then late.
    kept = {}
    for f in rows(os.path.join(data, "events"), 7):
        eid = f[0]
        if eid is None:
            continue
        ingest = parse_ts(f[3])
        if ingest is None:
            ingest = float("inf")
        prev = kept.get(eid)
        if prev is None or ingest < prev[0]:
            kept[eid] = (ingest, f)
    events_by_person = defaultdict(list)
    for ingest, f in kept.values():
        _, user, ts_s, _, etype, product, price_s = f
        ts = parse_ts(ts_s)
        if user is None or ts is None or not (w_start <= ts < w_end):
            continue
        if etype not in (b"view", b"cart", b"purchase") or product is None:
            continue
        price = 0
        if etype == b"purchase":
            if price_s is None or not is_digits(price_s):
                continue
            price = int(price_s)
            if not 1 <= price <= 10_000_000:
                continue
        if ingest - ts > DAY_MS:
            continue
        events_by_person[find(user)].append((ts, etype == b"purchase", price, category.get(product, b"unknown")))

    # Clicks: dedup, validate.
    clicks = {}
    for f in rows(os.path.join(data, "clicks"), 4):
        if f[0] is not None and f[0] not in clicks:
            clicks[f[0]] = f
    clicks_by_person = defaultdict(list)
    for cid, user, campaign, ts_s in clicks.values():
        ts = parse_ts(ts_s)
        if user is None or ts is None or not (w_start - REACH_MS <= ts < w_end):
            continue
        if campaign not in channel:
            continue
        clicks_by_person[find(user)].append((ts, cid, campaign))

    user_rows = {}
    camp_rows = defaultdict(lambda: [0, 0, 0, 0])  # sessions, purchasing, revenue, assisted
    for person, evs in events_by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= GAP_MS:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cl = clicks_by_person.get(person, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY_MS
            revenue = sum(e[2] for e in s if e[1])
            purchasing = any(e[1] for e in s)
            cats = defaultdict(int)
            for e in s:
                if e[1]:
                    cats[e[3]] += e[2]
            row = user_rows.setdefault((person, day), [0, 0, 0, defaultdict(int)])
            row[0] += 1
            row[1] += len(s)
            row[2] += revenue
            for c, v in cats.items():
                row[3][c] += v
            qualifying = [c for c in cl if start - REACH_MS <= c[0] <= start]
            if qualifying:
                latest = max(c[0] for c in qualifying)
                credited = min(c for c in qualifying if c[0] == latest)[2]
            else:
                credited = b"organic"
            cr = camp_rows[(credited, day)]
            cr[0] += 1
            cr[1] += int(purchasing)
            cr[2] += revenue
            for campaign in {c[2] for c in qualifying}:
                camp_rows[(campaign, day)][3] += 1

    epoch = date(1970, 1, 1)
    ud = {"user_id": [], "day": [], "sessions": [], "events": [], "revenue_cents": [], "top_category": []}
    for (person, day), (n, ev, rev, cats) in user_rows.items():
        top = None
        if cats:
            top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
        ud["user_id"].append(person.decode())
        ud["day"].append(epoch + timedelta(days=day))
        ud["sessions"].append(n)
        ud["events"].append(ev)
        ud["revenue_cents"].append(rev)
        ud["top_category"].append(top)
    cd = {"campaign_id": [], "channel": [], "day": [], "sessions": [], "purchasing_sessions": [],
          "revenue_cents": [], "assisted_sessions": []}
    for (campaign, day), (n, p, rev, a) in camp_rows.items():
        cd["campaign_id"].append(campaign.decode())
        cd["channel"].append("organic" if campaign == b"organic" else channel[campaign].decode())
        cd["day"].append(epoch + timedelta(days=day))
        cd["sessions"].append(n)
        cd["purchasing_sessions"].append(p)
        cd["revenue_cents"].append(rev)
        cd["assisted_sessions"].append(a)

    for name, cols, schema in (
        ("user_daily", ud, {"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64, "events": pl.Int64,
                            "revenue_cents": pl.Int64, "top_category": pl.String}),
        ("campaign_daily", cd, {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
                                "sessions": pl.Int64, "purchasing_sessions": pl.Int64,
                                "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64}),
    ):
        os.makedirs(os.path.join(out, name), exist_ok=True)
        pl.DataFrame(cols, schema=schema).write_parquet(os.path.join(out, name, "part-0.parquet"))


if __name__ == "__main__":
    main()
