# Streaming

`pg-cdc start` connects to the replication slot and streams WAL changes into delta Parquet files.

## What Happens

```
pg-cdc start
  1. Read manifest.json for init LSN and active table list
  2. Connect to replication slot at init LSN
  3. Decode pgoutput v2 WAL messages (INSERT/UPDATE/DELETE)
  4. Buffer events in memory
  5. Flush to delta Parquet on interval or row count
  6. Update manifest.json (bump latest_delta_epoch)
  7. Send standby status to PostgreSQL (prevent WAL accumulation)
```

## Usage

```bash
pg-cdc start --config pg-cdc.yml
```

Runs as a long-lived daemon. Stop with SIGINT (Ctrl+C) or SIGTERM.

## Delta Parquet Format

Each delta file contains CDC events with two metadata columns:

| Column | Type | Values |
|--------|------|--------|
| `__op` | UTF8 | `I` (insert), `U` (update), `D` (delete) |
| `__epoch` | INT64 | Epoch number |
| *(table columns)* | typed | PostgreSQL values (NULL for D ops except PK) |

## Epochs

An epoch is an atomic batch of changes. Each flush creates one epoch file:

```
public.orders/deltas/epoch=000001.parquet   ← first batch
public.orders/deltas/epoch=000002.parquet   ← second batch
```

Epochs are monotonically increasing. pg-warehouse uses epoch numbers as watermarks to track what it has already pulled.

## Flush Behavior

```yaml
flush:
  interval_sec: 10    # flush every 10 seconds
  max_rows: 1000      # or every 1000 rows
```

Whichever threshold is hit first triggers a flush. On graceful shutdown, remaining buffered events are flushed.

## Type Fidelity and Blocked Batches

`numeric`/`decimal` columns are written as exact Parquet `DECIMAL(p, s)`
(16-byte fixed encoding, up to precision 38) — never as lossy doubles. The
`(p, s)` comes from the column type recorded in the manifest schema version
(e.g. `numeric(10,2)`); plain `numeric` without a typmod defaults to
`DECIMAL(38,18)`.

Conversion is strict. When a record does not fit the column type of the
current manifest schema version — classically after the source widens a
column (`ALTER COLUMN ... TYPE numeric(38,10)`) while the manifest still
declares the old type — the whole epoch is **blocked**:

- no delta file of that epoch is published and the manifest does not advance
- the LSN is not confirmed, so Postgres retains the WAL (no data loss)
- the batch is retried every flush tick; events stay buffered in order
- `pg-cdc status` shows the blocked epoch with the exact table, column,
  value, and reason

To recover, widen the column type in the manifest schema (or fix the source
value). The next retried batch then lands and the block clears. NaN and
`Infinity` numerics have no exact decimal representation and also block the
batch by design.

## Governance During Streaming

The WAL stream is scoped by the PostgreSQL publication (created during init). Only tables included at init time will appear in the stream. As a safety net, the streaming loop also checks each event's table against `manifest.ActiveTables()` and skips unknown tables.

## Monitoring

While streaming, check health with:

```bash
pg-cdc status --config pg-cdc.yml
```
