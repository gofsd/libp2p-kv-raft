//! Rust port of `pkg/logrecord` (`key.go` + `record.go` combined -- small
//! enough here not to warrant a second file the way `pkg/shmevent`'s
//! event/system/catalog split does): a generic, append-only structured
//! record store on top of the raft-replicated kv store, keyed by
//! `(kind, unit_id, timestamp, rand)` so "every record of this kind and
//! unit, in chronological order" is a plain byte-wise range scan. See
//! `pkg/logrecord/key.go`'s doc comment for the full design rationale.
//!
//! Deliberately has no "current time" source of its own -- `build_key`/
//! `Record::new` take an explicit `OffsetDateTime`, so this module stays
//! plain, natively `cargo test`-able data plumbing; the wasm-only "now"
//! (`js_sys::Date::now()`, since `wasm32-unknown-unknown` has no
//! `SystemTime`) lives in `client.rs`, the one layer up that actually
//! needs it.

use crate::shmevent::Error;
use time::OffsetDateTime;

/// Marks a store key as belonging to this module -- reserved the same way
/// `shmevent::system::SYSTEM_KEY_PREFIX` (`0x00`) reserves its own
/// namespace, but as its own sibling top-level byte. Matches
/// `pkg/logrecord.LogKeyPrefix`.
pub const LOG_KEY_PREFIX: u8 = 0x01;

/// Width of [`build_key`]'s rand tiebreaker field, in bytes. Matches
/// `pkg/logrecord.RandSize`.
pub const RAND_SIZE: usize = 8;

/// Generates a fresh random [`RAND_SIZE`]-byte tiebreaker for
/// [`build_key`], sourced from the platform CSPRNG (`getrandom`, already a
/// wasm dependency here). Matches `pkg/logrecord.NewRand`.
pub fn new_rand() -> Result<[u8; RAND_SIZE], Error> {
    let mut r = [0u8; RAND_SIZE];
    getrandom::fill(&mut r).map_err(|e| Error(format!("new_rand: {e}")))?;
    Ok(r)
}

/// Packs `kind`, `unit_id`, `ts`, and `rnd` into a single store key:
/// `[LOG_KEY_PREFIX][kindLen 2BE][kind][unitIDLen 2BE][unitID][tsNano 8BE][rand]`.
/// Both `kind` and `unit_id` need their own length prefix (unlike
/// `shmevent::system::system_key`, whose one variable-length field is
/// last and needs none) since neither is the last field here. Matches
/// `pkg/logrecord.BuildKey`.
pub fn build_key(
    kind: &str,
    unit_id: &str,
    ts: OffsetDateTime,
    rnd: [u8; RAND_SIZE],
) -> Result<Vec<u8>, Error> {
    let kind_bytes = kind.as_bytes();
    let unit_bytes = unit_id.as_bytes();
    if kind_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "logrecord: kind too long: {} bytes",
            kind_bytes.len()
        )));
    }
    if unit_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "logrecord: unitID too long: {} bytes",
            unit_bytes.len()
        )));
    }
    let mut buf =
        Vec::with_capacity(1 + 2 + kind_bytes.len() + 2 + unit_bytes.len() + 8 + RAND_SIZE);
    buf.push(LOG_KEY_PREFIX);
    buf.push((kind_bytes.len() >> 8) as u8);
    buf.push(kind_bytes.len() as u8);
    buf.extend_from_slice(kind_bytes);
    buf.push((unit_bytes.len() >> 8) as u8);
    buf.push(unit_bytes.len() as u8);
    buf.extend_from_slice(unit_bytes);
    buf.extend_from_slice(&unix_nanos(ts).to_be_bytes());
    buf.extend_from_slice(&rnd);
    Ok(buf)
}

/// Inverse of [`build_key`] (the timestamp only -- `rand` has no meaning
/// once parsed back out, matching `pkg/logrecord.ParseKey`'s own return
/// shape).
pub fn parse_key(key: &[u8]) -> Result<(String, String, OffsetDateTime), Error> {
    if key.is_empty() || key[0] != LOG_KEY_PREFIX {
        return Err(Error("logrecord: key missing LOG_KEY_PREFIX".to_string()));
    }
    let mut off = 1;
    if off + 2 > key.len() {
        return Err(Error(
            "logrecord: key truncated before kind length".to_string(),
        ));
    }
    let kind_len = ((key[off] as usize) << 8) | key[off + 1] as usize;
    off += 2;
    if off + kind_len > key.len() {
        return Err(Error("logrecord: key truncated in kind".to_string()));
    }
    let kind = String::from_utf8_lossy(&key[off..off + kind_len]).to_string();
    off += kind_len;
    if off + 2 > key.len() {
        return Err(Error(
            "logrecord: key truncated before unitID length".to_string(),
        ));
    }
    let unit_len = ((key[off] as usize) << 8) | key[off + 1] as usize;
    off += 2;
    if off + unit_len > key.len() {
        return Err(Error("logrecord: key truncated in unitID".to_string()));
    }
    let unit_id = String::from_utf8_lossy(&key[off..off + unit_len]).to_string();
    off += unit_len;
    if off + 8 > key.len() {
        return Err(Error(
            "logrecord: key truncated before timestamp".to_string(),
        ));
    }
    let nanos = i64::from_be_bytes(key[off..off + 8].try_into().unwrap());
    Ok((kind, unit_id, from_unix_nanos(nanos)))
}

/// Fixed key prefix shared by every record of `kind`/`unit_id`, up to (not
/// including) the timestamp field -- the shared building block behind
/// [`kind_prefix`] and [`scan_bounds`]. Matches
/// `pkg/logrecord.kindUnitPrefix`.
fn kind_unit_prefix(kind: &str, unit_id: &str) -> Vec<u8> {
    let kind_bytes = kind.as_bytes();
    let unit_bytes = unit_id.as_bytes();
    let mut buf = Vec::with_capacity(1 + 2 + kind_bytes.len() + 2 + unit_bytes.len());
    buf.push(LOG_KEY_PREFIX);
    buf.push((kind_bytes.len() >> 8) as u8);
    buf.push(kind_bytes.len() as u8);
    buf.extend_from_slice(kind_bytes);
    buf.push((unit_bytes.len() >> 8) as u8);
    buf.push(unit_bytes.len() as u8);
    buf.extend_from_slice(unit_bytes);
    buf
}

/// Inclusive `[lo, hi]` key range covering every record of `kind`/
/// `unit_id` with a timestamp in `[start, end]`. `hi` pads with a run of
/// `0xFF` bytes past the timestamp field so it sorts after any rand
/// suffix a record with timestamp == `end` could have. Matches
/// `pkg/logrecord.ScanBounds`.
pub fn scan_bounds(
    kind: &str,
    unit_id: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> (Vec<u8>, Vec<u8>) {
    let prefix = kind_unit_prefix(kind, unit_id);

    let mut lo = prefix.clone();
    lo.extend_from_slice(&unix_nanos(start).to_be_bytes());

    let mut hi = prefix;
    hi.extend_from_slice(&unix_nanos(end).to_be_bytes());
    hi.extend(std::iter::repeat(0xFFu8).take(RAND_SIZE));
    (lo, hi)
}

/// Key prefix shared by every record of `kind`, across every `unit_id`.
/// Matches `pkg/logrecord.KindPrefix`.
pub fn kind_prefix(kind: &str) -> Vec<u8> {
    let kind_bytes = kind.as_bytes();
    let mut buf = Vec::with_capacity(1 + 2 + kind_bytes.len());
    buf.push(LOG_KEY_PREFIX);
    buf.push((kind_bytes.len() >> 8) as u8);
    buf.push(kind_bytes.len() as u8);
    buf.extend_from_slice(kind_bytes);
    buf
}

fn unix_nanos(ts: OffsetDateTime) -> i64 {
    (ts - OffsetDateTime::UNIX_EPOCH).whole_nanoseconds() as i64
}

fn from_unix_nanos(nanos: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + time::Duration::nanoseconds(nanos)
}

/// The generic envelope every logged entry is stored as (JSON-encoded).
/// `Kind`/`Fields`/`Narrative` are entirely caller-defined -- this module
/// makes no claim to any particular report format. Matches
/// `pkg/logrecord.Record`'s JSON shape exactly (field names/omitempty).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Record {
    pub kind: String,
    pub unit_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub author_peer_id: String,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub fields: std::collections::HashMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub narrative: String,
}

impl Record {
    /// Marshals `self` to JSON -- the wire/storage form `LogAppend`
    /// writes as a record's store value. Matches `pkg/logrecord.Record.Encode`.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|e| Error(e.to_string()))
    }

    /// Inverse of [`Record::encode`]. Matches `pkg/logrecord.Decode`.
    pub fn decode(data: &[u8]) -> Result<Record, Error> {
        serde_json::from_slice(data).map_err(|e| Error(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn build_key_parse_key_round_trip() {
        let ts = datetime!(2026-07-23 18:30:00 UTC);
        let key = build_key("cmdlog", "instance-1", ts, [1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let (kind, unit_id, got_ts) = parse_key(&key).unwrap();
        assert_eq!(kind, "cmdlog");
        assert_eq!(unit_id, "instance-1");
        assert_eq!(got_ts, ts);
    }

    #[test]
    fn parse_key_rejects_missing_prefix() {
        assert!(parse_key(&[0xFF, 0x00]).is_err());
        assert!(parse_key(&[]).is_err());
    }

    #[test]
    fn kind_prefix_is_a_prefix_of_build_key() {
        let ts = datetime!(2026-01-01 00:00:00 UTC);
        let key = build_key("sitrep", "unit-a", ts, [0u8; RAND_SIZE]).unwrap();
        assert!(key.starts_with(&kind_prefix("sitrep")));
    }

    #[test]
    fn scan_bounds_contains_records_in_range() {
        let start = datetime!(2026-01-01 00:00:00 UTC);
        let mid = datetime!(2026-06-01 00:00:00 UTC);
        let end = datetime!(2026-12-31 23:59:59 UTC);
        let (lo, hi) = scan_bounds("sitrep", "unit-a", start, end);
        let key = build_key("sitrep", "unit-a", mid, [9u8; RAND_SIZE]).unwrap();
        assert!(lo.as_slice() <= key.as_slice());
        assert!(key.as_slice() <= hi.as_slice());
    }

    #[test]
    fn record_json_shape_matches_go() {
        let rec = Record {
            kind: "cmdlog".to_string(),
            unit_id: "instance-1".to_string(),
            timestamp: datetime!(2026-07-23 18:30:00 UTC),
            author_peer_id: "12D3KooWabc".to_string(),
            fields: std::collections::HashMap::new(),
            narrative: "".to_string(),
        };
        let json = serde_json::to_string(&rec).unwrap();
        // fields/narrative both empty -> omitted, matching Go's
        // `omitempty` tags.
        assert!(!json.contains("fields"));
        assert!(!json.contains("narrative"));
        assert!(json.contains(r#""kind":"cmdlog""#));
        assert!(json.contains(r#""unit_id":"instance-1""#));
        assert!(json.contains(r#""author_peer_id":"12D3KooWabc""#));
        assert!(json.contains(r#""timestamp":"2026-07-23T18:30:00Z""#));
    }

    #[test]
    fn record_encode_decode_round_trip() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("command_id".to_string(), "cmd-1".to_string());
        let rec = Record {
            kind: "cmdreq:cmd-1".to_string(),
            unit_id: "abcd1234".to_string(),
            timestamp: datetime!(2026-07-23 18:30:00.123456789 UTC),
            author_peer_id: "12D3KooWabc".to_string(),
            fields,
            narrative: "a narrative".to_string(),
        };
        let encoded = rec.encode().unwrap();
        let decoded = Record::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
    }
}
