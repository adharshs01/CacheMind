# Data contracts and data-quality rules

## Canonical request (Silver)

| Column | Type | Rule |
|---|---|---|
| `event_id` | string | required; `^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`; generated as UUIDv5(`dataset_id/seq`) so it is stable across retries and replays |
| `dataset_id` | string | required; lower-cased; `^[a-z0-9][a-z0-9_-]{0,63}$` (partition column) |
| `seq` | long | required; integer ≥ 0; unique within a dataset (replay order) |
| `event_time_ms`, `event_time` | long, timestamp | required; ISO-8601 **with** offset (or `Z`), normalised to UTC; 2000-01-01 ≤ t < 2100-01-01; naive timestamps are rejected |
| `event_date`, `event_hour` | date, int | derived (UTC) |
| `user_id`, `item_id` | string | required; trimmed + lower-cased; `^[a-z0-9][a-z0-9_-]{0,63}$` |
| `region` | string | required; one of `us-east, us-west, eu-west, ap-south, ap-east`; legacy codes `USE/USW/EUW/APS/APE` mapped |
| `item_size_bytes` | long | required; 0 < size ≤ 10 GiB |
| `category` | string | from item metadata; normalised (`"  Video "`, `vid` → `video`); unknown/missing → `unknown` |
| `metadata_status` | string | `ok` / `missing` (no metadata row) / `conflict` (contradictory metadata rows → category `unknown`) |
| `user_group` | string | from user metadata; missing → `unknown` |
| `source`, `source_file`, `source_line`, `kafka_topic`, `kafka_partition`, `kafka_offset` | lineage | exactly one of (file, line) or (topic, partition, offset) is set |
| `payload_hash` | string | sha256 of the canonical payload (identity for duplicate checks) |
| `ingest_run_id`, `silver_run_id` | string | run lineage |

## Sources

| Source | Shape | Mapping |
|---|---|---|
| `legacy_csv` | `request_id,seq,ts_epoch_ms,user,item,region_code,bytes`; unquoted, exactly 7 fields; upper-case ids; epoch ms | ids lower-cased, region codes mapped, epoch ms → ISO UTC |
| `api_json` | nested `{v,id,seq,dataset,timestamp,user{id},item{id,bytes},geo{region}}`; mixed UTC offsets | fields flattened; a non-object where an object is required is malformed |
| `kafka` | flat canonical wire JSON (`CanonicalEvent.to_wire`) | identity |

## Validation (first failing rule wins, identical in Python and Spark)

1. malformed record (`malformed_json` / `malformed_csv`)
2. `schema_version` ≠ 1 → `unsupported_schema_version`
3. missing/blank required field → `missing_field:<name>` (in field order)
4. `invalid_event_id`, 5. `invalid_dataset_id`, 6. `invalid_seq`,
7. `invalid_timestamp`, 8. `invalid_item_id`, 9. `invalid_user_id`, 10. `invalid_region`,
11. `non_positive_size`, 12. `invalid_size`

The two implementations (`contracts.validate_wire_record`, `etl.validate_wire`) are
checked against each other on a 27-record fixture covering every rule
(`test_spark_validation_matches_python_reference`).

## Duplicates (deterministic, never silent)

| Case | Policy | Quarantine reason |
|---|---|---|
| same `event_id`, same payload | keep the first by lineage order (source, file, line, partition, offset); quarantine the other copies | `exact_duplicate` |
| same `event_id`, different payloads | quarantine **all** versions — no version is chosen | `conflicting_duplicate` |
| different `event_id`s sharing `(dataset_id, seq)` | quarantine all (replay order would be ambiguous) | `sequence_conflict` |

The same instant written in two time zones is an *exact* duplicate (payloads compare
after normalisation).

## Quarantine record

`raw_payload, dq_reason, source, source_file, source_line, kafka_topic,
kafka_partition, kafka_offset, event_id (if parseable), ingest_run_id,
processing_ts, silver_run_id` — the original payload, the failure reason, the source
reference and the processing timestamp are always present (tested).

## Late and out-of-order data

* **Out-of-order**: the canonical order is `(seq, event_id)`, never arrival order. Files
  contain locally shuffled rows; Kafka gives no cross-partition order.
* **Late (streaming)**: an event is *late* if its event time is more than the watermark
  delay (2 minutes) behind the maximum event time seen in earlier micro-batches. Late
  events are valid: they are kept in Silver with `is_late = true`. The streaming window
  aggregation (watermark 2 min) may drop them — Spark counts these as
  `numRowsDroppedByWatermark` — and the batch Gold recomputation from Silver includes them.
  A watermark is a lower bound on what the windowed query waits for, not a guarantee that
  every late event is aggregated.
* **Event time is per timeline.** `event_time` is the synthetic time of the request in its
  dataset's own timeline (every dataset starts 2026-01-05 00:00 UTC). Lateness and the
  window watermark are defined per streaming namespace (topic + checkpoints), which must
  therefore carry one timeline; independent datasets/runs use separate namespaces
  (`STREAMING_SEMANTICS.md`).
* Malformed records are never "late": they are quarantined.

## Verification against ground truth

The generator writes `expected.json` while writing raw files: per-source row counts,
injected anomalies, the expected quarantine count per reason, the expected Silver row
count and an order-independent checksum (Σ of the first 60 bits of
`sha256(event_id | payload_hash)`). `build_silver` recomputes all of these from its output
and reports `verification.all_ok`, plus the row accounting identity
`bronze rows = silver rows + quarantined rows`.

## Before / after example

Raw legacy CSV line (Bronze keeps it verbatim):
```
69703a9b-4484-59cb-a2ec-e7afc12308fa,0,1767571200085,U0000037,I0000292,EUW,6327
```
Raw API JSON line (another event, rendered at UTC−08:00):
```json
{"v":1,"id":"7dc8d425-…","seq":2000,"dataset":"zipf-n5000-s7","timestamp":"2026-01-04T16:01:09.847-08:00",
 "user":{"id":"u0000063"},"item":{"id":"i0000140","bytes":20238605},"geo":{"region":"us-west"}}
```
Silver (canonical) rows:
```
event_id=69703a9b-… seq=0    event_time=2026-01-05T00:00:00.085Z user_id=u0000037 item_id=i0000292 region=eu-west  item_size_bytes=6327     category=…
event_id=7dc8d425-… seq=2000 event_time=2026-01-05T00:01:09.847Z user_id=u0000063 item_id=i0000140 region=us-west  item_size_bytes=20238605 category=…
```
Rejected examples: `"2026-02-30T10:00:00Z"` → `invalid_timestamp`; a CSV row with an empty
item → `missing_field:item_id`; `"bytes": -42` → `non_positive_size`; a truncated JSON line →
`malformed_json`.
