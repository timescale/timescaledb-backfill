# Backfill Internals

> Design record migrated from Slab (2026-07); audited against this repo's source at migration time: **implemented as described**, with one detail that has moved on since - the shipped tool still suspends invalidation around each copy, but how it does so now depends on the target's TimescaleDB version (see below), streaming `COPY` directly between source and target connections (the named-pipe `psql` flow below was the exploration of that technique). Companion to the [Backfill tool design document](backfill-tool.md).

## Backfilling Continuous Aggregates

```
                             ←time
mt:    ☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐
ht: ☐☐☐☐☐☐☐☐☐☐☐☐☐☐☐
    <a><-----b----><------c------>
```

Consult the illustration above. Imagine a continuous aggregate over a hypertable named `ht`. The continuous aggregate is backed by another hypertable `mt` that stores the materialized part of the view. `ht` has a retention job defined. The chunks in time frame `c` have been dropped. `ht` has a compression job. All chunks in time frame `b` are already compressed. In time frame `b` both `ht` and `mt` have chunks with data. In time frame `a` only `ht` has chunks because the refresh policy has not yet materialized the cagg. Assume this is a lot of data — multiple TB. Assume time frame `c` is large — 2 years.

We don't want to lose the data in `mt` in time frame `c`. We cannot recompute it from the underlying hypertable `ht` because the relevant chunks have been dropped.

We don't want to recalculate the chunks in `mt` in time frame `b`. We want to migrate these chunks as-is. Recomputing will be slow and costly.

When we backfill the chunks from `ht` in time frame `b`, there is a trigger that will invalidate the chunks in `mt` in time frame `b`. That will cause the cagg to refresh all the chunks in time frame `b`. It will also slow down the backfill into `ht`.

Thus, when we backfill chunks for a hypertable that has one or more continuous aggregates defined on it, we need to suspend invalidation tracking before copying into the chunk and restore it when we finish.

How that is done depends on the target's TimescaleDB version (`suspend_cagg_invalidation` in `src/execute.rs`):

- **< 2.23**: drop `ts_cagg_invalidation_trigger` from the target chunk before the copy and recreate it afterwards. `ALTER TABLE ... DISABLE TRIGGER` is not an option, TimescaleDB intercepts it and prohibits it.
- **>= 2.23, < 2.28**: 2.23 removed the trigger and moved invalidation into the executor, which leaves no way to opt out. The copy records invalidation entries, and the cagg re-materializes ranges that the backfill already brought over.
- **>= 2.28**: `SET LOCAL timescaledb.skip_cagg_invalidation = on` for the duration of the copy ([timescaledb#9842](https://github.com/timescale/timescaledb/pull/9842)). It is a `PGC_USERSET` GUC, so `tsdbadmin` can set it. `SET LOCAL` matters: it must not leak into the cagg refresh at the end of a `copy` run, where a cagg built on another cagg needs its invalidations recorded to be refreshed in turn.

## Streaming Copy with psql

Goals:

- move a chunk without using intermediate storage
- copy the data out of the source quickly
- copy the data into the target quickly
- use a format that is efficient
- use a format that does not lose floating-point precision

Create a named pipe.

`mkfifo pipe`

Copy the contents of the source chunk to the named pipe using the binary format.

```bash
psql -d 'source' -v ON_ERROR_STOP=1 -Xqb -f - <<EOF
begin;
set session statement_timeout to 0;
\copy chunk to 'pipe' with (format binary)
rollback;
\echo tx done
\q
EOF
```

Copy from the named pipe into the target chunk.

```bash
psql -d 'target' -v ON_ERROR_STOP=1 -Xqb -f - <<EOF
begin;
set session statement_timeout to 0;
\copy chunk from 'pipe' with (format binary)
commit;
analyze verbose chunk;
\echo rx done
\q
EOF
```
