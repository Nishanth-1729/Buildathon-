//! Produce a synthetic workload for compiler profiling during the image build.

use std::fs::{create_dir_all, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

const START: u64 = 1_798_761_600_000; // 2027-01-01T00:00:00Z
const DAY: u32 = 86_400;

struct Random(u64);

impl Random {
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 32) as u32
    }
}

fn utc(sec: u32) -> String {
    format!(
        "2027-01-{:02}T{:02}:{:02}:{:02}Z",
        1 + sec / DAY, sec / 3600 % 24, sec / 60 % 60, sec % 60
    )
}

fn timestamp(sec: u32, r: u32) -> String {
    match r % 10 {
        0 => {
            let mut iso = utc(sec);
            iso.pop();
            iso.push_str("+01:00");
            iso
        }
        1..=3 => (START + sec as u64 * 1000 + (r % 1000) as u64).to_string(),
        _ => utc(sec),
    }
}

fn csv_row(out: &mut impl Write, fields: &[&str], rng: &mut Random) -> io::Result<()> {
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            out.write_all(b",")?;
        }
        if rng.next() % 5 == 0 {
            write!(out, "\"{field}\"")?;
        } else {
            out.write_all(field.as_bytes())?;
        }
    }
    out.write_all(b"\n")
}

fn open(path: &Path, header: &str) -> io::Result<BufWriter<File>> {
    let mut out = BufWriter::new(File::create(path)?);
    writeln!(out, "{header}")?;
    Ok(out)
}

fn user(r: u32) -> String {
    let id = if r % 12 == 0 { (r >> 8) % 16 } else { (r >> 7) % 18_000 };
    format!("u{id:08x}")
}

fn main() -> io::Result<()> {
    let root = PathBuf::from(std::env::args_os().nth(1).expect("training data directory"));
    for dir in ["events", "clicks", "links"] {
        create_dir_all(root.join(dir))?;
    }
    std::fs::write(
        root.join("manifest.json"),
        r#"{"window_start":"2027-01-01T00:00:00Z","window_end":"2027-01-15T00:00:00Z"}"#,
    )?;
    let mut products = open(&root.join("products.csv"), "product_id,category")?;
    for i in 0..1000 {
        writeln!(products, "p_{i},cat_{}", i % 20)?;
    }
    products.flush()?;
    let mut campaigns = open(&root.join("campaigns.csv"), "campaign_id,channel")?;
    for i in 0..256 {
        writeln!(campaigns, "c_{i},channel_{}", i % 5)?;
    }
    campaigns.flush()?;
    let mut links = open(&root.join("links/0.csv"), "user_id_a,user_id_b")?;
    for i in (32..18_000).step_by(5) {
        writeln!(links, "u{i:08x},u{:08x}", i - 1)?;
    }
    links.flush()?;
    let mut rng = Random(0xd087_89d4_2b74_a5b1);
    for part in 0..8 {
        let mut events = open(
            &root.join(format!("events/{part}.csv")),
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
        )?;
        for i in 0..60_000 {
            let r = rng.next();
            let sec = rng.next() % (14 * DAY);
            let id = format!("e{:08x}", part * 60_000 + i);
            let uid = user(r);
            let time = timestamp(sec, r >> 5);
            let ingest = utc(sec + r % 400);
            let kind = match r % 100 {
                0..=2 => "purchase",
                3..=17 => "cart",
                _ => "view",
            };
            let product = format!("p_{}", r % 1005);
            let price = if kind == "purchase" { (1 + r % 5000).to_string() } else { String::new() };
            let fields = [id.as_str(), &uid, &time, &ingest, kind, &product, &price];
            csv_row(&mut events, &fields, &mut rng)?;
            if r % 31 == 0 {
                csv_row(&mut events, &fields, &mut rng)?;
            }
        }
        events.flush()?;
        let mut clicks = open(&root.join(format!("clicks/{part}.csv")), "click_id,user_id,campaign_id,ts")?;
        for i in 0..15_000 {
            let r = rng.next();
            let id = format!("k{:08x}", part * 15_000 + i);
            let uid = user(r);
            let campaign = format!("c_{}", r % 260);
            let when = timestamp(rng.next() % (14 * DAY), r >> 5);
            csv_row(&mut clicks, &[&id, &uid, &campaign, &when], &mut rng)?;
        }
        clicks.flush()?;
    }
    Ok(())
}
