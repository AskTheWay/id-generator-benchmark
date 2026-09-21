# ID Generator Benchmark

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115%2B-009688.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Interactive benchmark and comparison of 8 mainstream unique ID generation strategies — Snowflake, UUIDv4, UUIDv7, ULID, NanoID, Redis INCR, Redis Leaf-style segment mode, PostgreSQL sequence — with a live latency / throughput dashboard.**

[English](README.md) | [简体中文](README_zh-CN.md)

---

## Why

Choosing how to generate unique IDs in a distributed system is one of those decisions
too often made from hearsay ("just use UUID", "Snowflake is complicated"). It deserves
data, not folklore. **id-generator-benchmark** runs 8 mainstream ID generation strategies
through the *same* harness on *your* machine and puts their latency quantiles (P50/P90/P99),
throughput (QPS) and duplicate rate side by side in a live web dashboard.

It is built for:

- **Backend engineers** choosing an ID strategy for a real service — see the actual cost of
  each network round trip, and whether "trend-increasing" IDs actually help your database.
- **Interview preparation** — every strategy has a deep-dive section with its bit layout,
  trade-offs and classic pitfalls (Snowflake clock drift, Redis key expiry races, segment
  prefetch watermarks), so you can explain *why*, not just *what*.
- **Teaching distributed systems** — one repo, eight textbook coordination/entropy trade-offs.

## The 8 Strategies at a Glance

| # | Strategy | Layout (bit composition) | Monotonic | Dependency | Theoretical limit | Best for |
|---|----------|---------------------------|-----------|------------|-------------------|----------|
| 1 | UUIDv4 | 128 bit = 122 random + 6 fixed (version/variant) | No | None | 2^122 ≈ 5.3×10^36 values | Zero-dependency IDs when order doesn't matter |
| 2 | UUIDv7 | 48 bit unix_ts_ms \| 4 bit ver \| 12 bit rand_a \| 2 bit variant \| 62 bit rand_b | Trend (ms) | None | 2^74 IDs/ms; 48-bit ms timestamp spans ~8,900 years | Time-ordered UUIDs that stay index-friendly |
| 3 | ULID | 48 bit ms timestamp \| 80 bit randomness (26-char Crockford Base32) | Trend (ms) | None | 2^80 IDs/ms randomness | Sortable, URL-safe string IDs |
| 4 | NanoID | 21 chars × 64-char alphabet ≈ 126 bit entropy | No | None | ~2^126 ≈ 8.5×10^37 values | Compact URL-safe IDs, custom alphabets |
| 5 | Snowflake | 0 \| 41 bit timestamp \| 10 bit machine ID \| 12 bit sequence | Yes (trend) | None | 4096 IDs/ms/machine × 1024 machines; ~69 years from epoch | High-QPS 64-bit integer IDs, DB-index friendly |
| 6 | Redis INCR | 64-bit atomic counter key | Yes (strict) | Redis | Single-node Redis throughput (~100k QPS, network-bound) | Simplest strictly-increasing ID when Redis already exists |
| 7 | Redis segment (Leaf-style) | Pre-allocated ranges, step 1000 | Yes (strict) | Redis | Amortized 1 Redis op per 1000 IDs; ID issue rate ≈ local counter | Very high QPS + strict trend without clock discipline |
| 8 | PostgreSQL sequence | 64-bit bigint sequence (one value per `nextval`) | Yes (strict, gaps on rollback) | PostgreSQL | One DB round trip per ID; pool-bound (typically ~10k QPS) | Centralized, durable ordering in an RDBMS-centric stack |

## Quick Start

Requirements: **Python 3.11+** (developed on 3.13). Docker is optional — local strategies
(UUIDv4/v7, ULID, NanoID, Snowflake) need nothing else; Redis/PostgreSQL strategies light up
as soon as their service is reachable.

```bash
# 1) (optional) start dependencies: PostgreSQL 16 on :15432, Redis 7 on :6380
docker compose up -d

#    Using the containerized Redis? Point the app at port 6380:
#      Windows (PowerShell):   $env:REDIS_PORT="6380"
#      Linux / macOS:          export REDIS_PORT=6380
#    (If you already run Redis on localhost:6379, no env var is needed.)

# 2) install
pip install -r requirements.txt

# 3) run
python -m uvicorn app.main:app --reload

# 4) open the dashboard
#    http://localhost:8000
```

### Dashboard Screenshots

| Strategy cards — live availability, batch sampling (collapsible) | Benchmark results — QPS & latency quantile charts |
|---|---|
| ![Strategy cards with live availability badges and collapsible batch sampling](demo_imgs/Snipaste_2026-09-22_05-16-45.png) | ![Benchmark result charts: QPS comparison and P50/P90/P99 latency](demo_imgs/Snipaste_2026-09-22_05-17-07.png) |
| Sharding × ID — four scenario cards with tunable params | Offline sneakernet — site cards, ferry import & collision demo |
| ![Sharding scenario cards with customizable parameters](demo_imgs/Snipaste_2026-09-22_05-17-36.png) | ![Offline sneakernet: site cards, ferry import and collision anti-pattern](demo_imgs/Snipaste_2026-09-22_05-17-46.png) |

### Configuration

All settings are environment variables with sensible defaults (see `app/config.py`):

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `localhost` / `6379` / `0` | Redis address (compose maps it to **6380**) |
| `PG_HOST` / `PG_PORT` | `localhost` / `15432` | PostgreSQL address (matches docker-compose) |
| `PG_USER` / `PG_PASSWORD` / `PG_DATABASE` | `postgres` / `postgres` / `idbench` | PostgreSQL credentials |
| `SEGMENT_STEP` | `1000` | Segment size (IDs per Redis `INCRBY`) |
| `SNOWFLAKE_MACHINE_ID` | `1` | 10-bit machine ID (0–1023) |
| `SNOWFLAKE_EPOCH_MS` | `1704067200000` | Custom epoch = 2024-01-01 00:00:00 UTC |

## Benchmark Methodology

Measuring an ID generator is easy to get wrong in ways that flatter or slander it.
This section documents exactly what the harness does — and where it *itself* becomes
the bottleneck.

### 1. Connection-pool warm-up before timing

The first request a client makes pays not just for the operation, but for the TCP
connect + AUTH + handshake. Under concurrency, an unwarmed pool serializes a burst of
connection setups, and those one-off stalls land squarely in the tail — poisoning P99
for the *whole* run even though no steady-state request is that slow.

Measured in this project (Redis INCR, `total=5000`, `concurrency=50`):

| | P50 | P99 |
|---|---|---|
| No warm-up (cold pool) | 1.1 ms | **2056 ms** |
| After `warmup()` (full pool) | 0.4 ms | **19 ms** |

That is a ~100× distortion of P99 from a purely methodological artifact. The harness
therefore calls `gen.warmup(concurrency)` *before* the clock starts, filling the
connection pool to the expected concurrency so timed requests only measure `generate()`.

### 2. Per-operation latency via `time.perf_counter_ns()`

Each individual `generate()` call is bracketed with the nanosecond monotonic clock.
Reported quantiles are per-ID latency, not per-batch averages — averages hide exactly
the tail behavior you are choosing an ID strategy for.

### 3. Duplicate detection

Every generated ID is inserted into one global `set` guarded by a lock, shared across
all worker threads. The result reports the duplicate count alongside success/error
counts — an ID generator that ever produces a duplicate has failed, whatever its QPS.

### 4. Quantiles with linear interpolation

P50/P90/P99 are computed by linear interpolation over the sorted latency list (the
standard "inclusive" method used by numpy's default), so they behave stably at any
sample size instead of jumping between discrete order statistics.

### 5. Honest limitation: the harness can be the bottleneck

This is a Python benchmark using a threaded client (`ThreadPoolExecutor`). Python's GIL
plus lock-protected bookkeeping mean that for **local generators** (UUIDv4, UUIDv7, ULID,
NanoID, Snowflake) you are substantially measuring the harness — function call, lock
acquisition, GIL scheduling — rather than the generator itself.

Treat local-generator numbers as a **lower bound** on what a production system achieves.
What remains meaningful is the *relative* comparison under identical overhead, because
every strategy pays the same harness tax. Network-bound generators (Redis, PostgreSQL)
are much less affected: worker threads block on socket I/O and release the GIL, so the
measured latency is dominated by the true service round trip.

### 6. Environment note: native-Windows Redis has spiky tail latency

If you point `REDIS_HOST` at a native-Windows port of redis-server, expect occasional
~2 s single-request stalls even with a fully warmed connection pool — the Windows port's
event loop occasionally delays handling of *new* connections. This is an artifact of the
server build, not of the ID strategies. For representative Redis latency percentiles,
run Redis under Docker or WSL2 instead.

## Strategy Deep Dives

### 1. UUIDv4 — pure randomness, zero dependencies

```
128 bits:  [ 122 random bits ................ | ver=4 | var | rnd ]
            31d3449a-9f2e-4c2b-b8f1-6e0a5c9d8e7f
                                            ^ 4 = version    ^ 8,9,a,b = variant
hex form:  xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx   (y ∈ {8,9,a,b})
```

UUIDv4 fills 122 of its 128 bits from a CSPRNG; the remaining 6 bits are fixed protocol
overhead (4-bit version field `0100`, 2-bit variant field `10`). Collision probability is
astronomically small: to get a 50% chance of *any* collision you need ~2.7×10^18 IDs.

The price is total disorder. Consecutive UUIDv4 values share no relationship, so a B-tree
index on a UUIDv4 column takes random-position inserts on every write: poor cache locality,
page splits everywhere, and ever-growing fragmentation. In a big table this dominates write
amplification.

- Pros: no coordination, no infrastructure, no clock dependence; collision risk negligible;
  universally understood.
- Cons: 36-char hex string (storage & index size); random inserts fragment B-tree indexes;
  leaks no ordering information for debugging or pagination.

Code: [`app/generators/uuid_v4.py`](app/generators/uuid_v4.py)

### 2. UUIDv7 — time-ordered UUIDs

```
 0                47 48 51 52            63 64 65 66                 127
+-------------------+------+--------------+----+----------------------+
|  unix_ts_ms (48)  | ver=7| rand_a (12)  |var |     rand_b (62)      |
+-------------------+------+--------------+----+----------------------+
   ms since Unix epoch    fixed bits (variant = 10xx)   CSPRNG
```

UUIDv7 (RFC 9562) moves a 48-bit millisecond timestamp into the most significant bits,
keeping the UUID container but making values *roughly* time-ordered. New rows append at
the right edge of the index instead of inserting at random positions — the same property
that made auto-increment keys index-friendly, without a central counter.

Ordering is only millisecond-granular: within the same millisecond, the random tail
scrambles order. That is fine for index locality (all same-ms rows land on the same
index pages) but it is *not* a strict ordering — you cannot use it as a change-feed
cursor or for dedup-by-maximum. The 12-bit `rand_a` field is specified as a
monotonic counter option to lift the same-ms ordering limit, at the cost of per-process
state.

- Pros: drop-in replacement for UUIDv4 (same column type, same tools); trend-increasing →
  good index locality; 74 random bits/ms still makes collisions a non-issue.
- Cons: still 36-char hex; only ms-level ordering; two IDs in the same ms are unordered.

Code: [`app/generators/uuid_v7.py`](app/generators/uuid_v7.py)

### 3. ULID — sortable, URL-safe identifiers

```
 01JMYPK5C2Q0 T7R9SBG6A4H8J2K5M0N7
+-------------+---------------------+
| timestamp   |     randomness      |
| 48 bit ms   |     80 bit          |
| 10 chars    |     16 chars        |
+-------------+---------------------+
  Crockford Base32 (0-9 A-H J K M N P-T V-Z), 26 chars, case-insensitive, no I/L/U/O
```

A ULID packs the same 48-bit millisecond timestamp + randomness idea as UUIDv7 into a
26-character Crockford Base32 string. Because the encoding is order-preserving (timestamp
occupies the most significant positions and Base32 digits sort lexicographically), **string
sort order == creation order** across milliseconds. Compare that to UUID hex, where even a
time-ordered UUIDv7 sorts correctly only when decoded, never as a naive string.

The 80-bit random tail gives 1.2×10^24 IDs per millisecond before collision risk becomes
nonzero; same-ms ordering is random. Practical benefits: 26 chars vs 36, no hyphens, URL-
and filename-safe, case-insensitive, and visually distinct from UUIDs in logs.

- Pros: lexicographically sortable as a plain string; shorter and URL-safe; timestamp
  readable from the ID itself; index-friendly inserts.
- Cons: not a UUID (ecosystem tooling assumes UUID sometimes); same-ms values unordered;
  128 bits stored as a 26-char string is wider than a bigint.

Code: [`app/generators/ulid.py`](app/generators/ulid.py)

### 4. NanoID — compact and customizable

```
 21 chars from a 64-char alphabet  [A-Za-z0-9_-]  →  21 × log2(64) ≈ 126 bits of entropy
 example:  V1StGXR8_Z5jdHi6B-myT_
```

NanoID is UUIDv4's answer to "why 36 characters?". Using a 64-symbol alphabet
(URL-safe `A-Za-z0-9_-`), 21 characters carry ~126 bits of entropy — slightly more than
UUIDv4's 122 — at 58% of the string length. The alphabet is a first-class parameter:
drop lookalike symbols (`0`, `O`, `1`, `l`) for human transcription, or shrink the alphabet
to fix a length budget. Compared to UUIDv4's `random`-based hex, NanoID uses a
crypto-secure random source and a bias-free modular mapping.

It inherits all of UUIDv4's ordering problems — fully random, no timestamp, nothing. Its
niche is exactly where UUIDv4 already fit, with a smaller and friendlier string.

- Pros: shortest common random ID; URL/file safe with no escaping; tunable alphabet &
  length; no coordination.
- Cons: random inserts (same index fragmentation as UUIDv4); less standardized than UUID;
  ordering meaningless.

Code: [`app/generators/nanoid.py`](app/generators/nanoid.py)

### 5. Snowflake — 64-bit trend-increasing integers

```
 1 bit        41 bits                         10 bits          12 bits
+-----+-----------------------------------+----------------+----------------+
|  0  |   ms since custom epoch           |   machine ID   |   sequence     |
+-----+-----------------------------------+----------------+----------------+
  sign     1704067200000 = 2024-01-01 UTC    0-1023            0-4095 per ms
```

Twitter's Snowflake fits a trend-increasing ID in a single signed 64-bit integer: a 41-bit
millisecond timestamp (relative to a custom epoch — here `SNOWFLAKE_EPOCH_MS =
1704067200000`, i.e. 2024-01-01 UTC, good until ~2093), a 10-bit machine ID (1024 nodes),
and a 12-bit per-millisecond sequence (4096 IDs/ms/machine, ~4M QPS per node). Each node
generates IDs independently with **zero network calls**: the theoretical limit is
"4096 IDs per ms per machine", and in practice you are bounded by the machine's ability
to call a function.

Because the high bits are the timestamp, Snowflake IDs sort in creation order (modulo
multi-node skew), giving B-tree indexes the append-mostly behavior of auto-increment —
while spanning 1024 machines with no coordination. The machine ID must be **unique per
node**; two nodes sharing an ID silently produce duplicates, which is why real
deployments allocate it via ZooKeeper/Consul/static config.

**Clock drift (clock rollback) handling.** Snowflake's Achilles' heel is the system clock.
If NTP steps the clock *backwards*, a naive implementation re-enters a millisecond it has
already issued IDs from — same timestamp, same machine ID, sequence restarts from 0 →
**duplicates**. This implementation handles drift explicitly:

- Drift ≤ **5 ms** (tolerance threshold): the generator *spin-waits* until real time
  catches back up to the last-seen timestamp, then continues — correctness preserved,
  latency temporarily inflated.
- Drift > 5 ms: it **raises an error instead of emitting an ID**. Silently duplicating IDs
  is the worst possible failure mode; failing loudly lets the operator fix the clock.
- Sequence exhaustion within one millisecond (>4096 IDs): also spin-waits to the next ms
  rather than overflowing the sequence bits.

- Pros: no network dependency, no single point of failure; 4096/ms per machine; 64-bit int
  (8 bytes) — smallest storage & fastest index of all trend-increasing options here.
- Cons: requires unique machine IDs and clock discipline (NTP with slew, no steps);
  leaks timing + machine info; drift beyond threshold halts generation.

Code: [`app/generators/snowflake.py`](app/generators/snowflake.py)

### 6. Redis INCR — atomic counter, one round trip per ID

```
   key "id:{name}"  ──►  [ 64-bit integer ]
   INCR  →  1, 2, 3, ...  strictly increasing, atomic under any concurrency
```

Redis executes commands single-threaded, so `INCR` is atomic by construction: every
client, on every connection, gets a unique, strictly increasing number. There is no
coordination logic to write and no theoretical ceiling below Redis's own throughput —
in practice a single Redis node serves ~100k simple ops/sec, and each ID costs exactly
**one network round trip**.

One operational trap: a bare counter key never expires. Benchmark runs and short-lived
environments would leak keys forever. This generator instead uses a small **Lua script
executed atomically in Redis** — increment first, and if the result is 1 (first use of
the key) set an expiry on it. Doing INCR and EXPIRE *inside one Lua call* is the point:
two separate commands from the client could race (crash between them → immortal key) and
Lua guarantees atomic, no-timing-window execution on the server.

- Pros: dead simple; strictly increasing; instant global uniqueness across all services;
  ID issuance rate bounded only by Redis throughput.
- Cons: Redis is a hard dependency and a single point of failure; one RTT per ID makes
  latency = network RTT (the dominant term in the benchmark); disaster recovery must
  plan for counter loss (persistence config) — a restart with empty data means re-issuing
  old IDs unless persistence/replication is set up.

Code: [`app/generators/redis_incr.py`](app/generators/redis_incr.py)

### 7. Redis Segment Mode (Leaf-style) — amortize the round trip

```
   Redis:  INCRBY id:{name} 1000  ──►  returns max of the new range
                                     e.g. 3000  →  owns (2001..3000]
   App:    local atomic allocation from the in-memory range

   current buffer        next buffer (prefetched at 50% watermark)
   [2001 ......... 3000] [3001 .............. 4000]
              ^ remaining < 50% of step → start async prefetch
```

Meituan's Leaf popularized segment mode: instead of talking to Redis per ID, a service
fetches a *range* of IDs at once. Here `INCRBY key 1000` (`SEGMENT_STEP = 1000`) atomically
reserves the next 1000-wide range, and ID generation becomes a local atomic counter
bumping through it — so the per-ID cost collapses from one network round trip to 1/1000th
of one, and the achievable QPS approaches that of a pure local generator while keeping
strictly increasing, Redis-coordinated order.

The catch is refill: when a range runs out, the naive implementation *blocks* on a Redis
round trip, and that stall lands in the tail latency. Leaf's answer is the **double
buffer**: while the current segment is still serving IDs, a *second* buffer is prepared
in the background. This generator prefetches when current-segment usage crosses 80%
(remaining drops below the **50% watermark**), so by the time the current range is
exhausted, the next one is already in memory and allocation switches over without
blocking. If traffic is so fast that the prefetch hasn't finished at exhaustion time,
allocation waits for it — degraded but still correct.

- Pros: near-local QPS with strict trend ordering; Redis load divided by the step size;
  no clock or machine-ID discipline needed; brief Redis outages are survivable while the
  current buffer lasts.
- Cons: IDs consume a fixed 1000-wide hole per fetch even if the process dies mid-range
  (gaps, never duplicates); 50%-watermark prefetch leaves ample headroom under bursty
  strictly increasing *per key* — multiple app instances share one sequence only if they
  share the key.

Code: [`app/generators/redis_segment.py`](app/generators/redis_segment.py)

### 8. PostgreSQL Sequence — ordering anchored in the database

```
   CREATE SEQUENCE id_seq AS bigint;        -- 64-bit, max 2^63 - 1
   SELECT nextval('id_seq');                -- 1, 2, 3, ...  one round trip per ID
```

The database sequence is the original centralized ID service. PostgreSQL hands out each
value under its locking and persistence guarantees, so every `nextval()` returns a unique,
strictly increasing 64-bit integer — and because it lives *in* the database, the ID's
ordering is consistent with the transactions that use it: no extra infrastructure, and
crash-safe by the same WAL that protects your data.

Two properties worth knowing precisely. First, sequences are **non-transactional**: a
`nextval()` inside a rolled-back transaction is *not returned to the pool*, so sequences
are strictly increasing but **not gap-free** — rollbacks and caching leave holes. For
generated IDs that is almost always acceptable; for invoice numbers mandated sequential
by law, it is not. Second, each ID costs a database round trip and competes for
connections with real queries, which caps throughput at roughly 10k QPS per pool — and
makes the whole scheme share fate with your primary database.

- Pros: true strict ordering anchored to durable storage; zero new infrastructure for a
  DB-centric stack; bigint storage; understandable failure modes.
- Cons: one RTT per ID and DB-bound throughput; the primary DB becomes the ID bottleneck
  & single point of failure; gaps on rollback; sharding a sequence is awkward.

Code: [`app/generators/db_sequence.py`](app/generators/db_sequence.py)

## Sharding × ID: Where IDs Live After You Split the Database

Sharding is *the* reason distributed ID generation exists: once rows are split across
shards, `AUTO_INCREMENT` is only unique *within* one shard. The dashboard's
**Sharding × ID** section demonstrates this live against PostgreSQL (N schemas act as
N shards). Every scenario is self-contained (re-runnable, auto-resets) and its
parameters are customizable — change shard count / row count / user count / hotspot
ratio right in the card and re-run. Each scenario deliberately uses only the ID
strategy that fits it; this is a teaching demo, not a forced tournament.

| # | Scenario | What you see | Teaching point | Tunable params |
|---|----------|--------------|----------------|----------------|
| 1 | Independent auto-increment (anti-pattern) | The same `L00000001` exists in **every** shard — ~100% of IDs collide across shards | Why database auto-increment stops working the moment you shard; this is the origin story of distributed IDs | shards (1–16), rows per shard |
| 2 | Step-based auto-increment | shard0 issues 1,5,9…; shard1 issues 2,6,10… — interleaved yet disjoint, globally unique with **zero coordination** | Static partitioning trades runtime coordination for planning: resharding 4→8 means migrating half the data with IDs frozen. Try running shards=4 then shards=8 and watch the ranges | shards, rows per shard |
| 3 | Gene method (Snowflake-based) | `order_id = (snowflake << 6) \| (user_id % 64)`; extracting the low 6 bits routes you straight to the shard holding that order. Same user ⇒ same shard, 100% | IDs can *carry routing info*, eliminating broadcast queries and mapping tables. Cost: gene bits cap the shard count (divisors of 64) | shards, users, orders per user |
| 4 | Range sharding + monotonic ID | With month-range shards the newest shard takes 80%+ of writes (hotspot bar turns amber); hash sharding is perfectly even but range queries scatter-gather | "Monotonic" flips from virtue to vice in distributed storage — exactly why TiDB added `auto_random` to shuffle auto-increment keys | shards, total rows, hotspot % |

**Reading the bars**: amber = hot shard, blue = even distribution, red = collision.

## Air-Gapped Sneakernet: Issuing IDs Where the Network Never Reaches

Some label printers live in prisons, defense plants, or remote substations — machines
that will *never* see your network. Data comes back on discs or USB sticks, ferried by
a human, on a daily/weekly cadence (sneakernet / air-gapped deployment). Collision
detection goes from milliseconds (a Redis round trip) to *weeks* (next ferry), so
uniqueness must be guaranteed **by construction, not by detection**.

The dashboard's **Offline Sneakernet** section simulates this end-to-end. Site state
lives in local JSON files (simulating each terminal's local disk) and touches the
database **only** when a ferry import happens — same topology as the real thing.

**The three defensive layers** (each one's failure mode is covered by the next):

| Layer | Prevents | How it works |
|-------|----------|--------------|
| 1. Site ID embedded in the ID | Cross-site collisions | Every terminal gets a globally-registered site number at install time; IDs look like `LB260922S0100000042` (prefix·date·site·seq) |
| 2. Locally persisted watermark | Re-issuing after restart / clock chaos | The sequence number comes from a monotonic watermark on local disk that *never* moves backwards. The date field is display-only — try the "wrong clock" input (`2020-01-01`) and watch the date go wrong while IDs stay unique |
| 3. Center-preallocated quotas | Watermark loss after reinstall | Each site draws from a quota range allocated in advance (e.g. S01: 1–10,000, S02: 10,001–20,000). When exhausted, the center appends a new range that travels back on the *next* ferry — the offline extreme of segment mode: segment size = one ferry cycle of demand |

**Final backstop**: ferry imports hit a `UNIQUE` index in the central database. With
the three layers intact it should *never* fire. The demo's anti-pattern card runs two
"naked" terminals (no site ID, each counting from 1) — watch the unique index reject
their collisions weeks "after the labels were printed", which is exactly why
after-the-fact detection is too late.

**Clocks in air-gapped environments**: each terminal's ID timestamp comes from *its
own* system clock (RTC hardware → system time → timestamp), and no two machines drift
alike. Within one site, ordering holds; across sites, never compare timestamps — use
ferry batch numbers. Real deployments calibrate clocks opportunistically: the ferry
media carries the authoritative time, or hardware GPS/radio clocks keep drift near
zero without any network.

Code: [`app/sharding.py`](app/sharding.py), [`app/offline.py`](app/offline.py) — demo
data is deliberately tiny (≤ 20k rows per scenario, well under any size pressure).

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Serves the dashboard (`web/index.html`) |
| `GET` | `/api/health` | Dependency availability: `{"redis": bool, "postgres": bool}` |
| `GET` | `/api/generators` | All 8 strategies with metadata, live availability and a freshly generated sample |
| `POST` | `/api/sample/{name}` | Generate one ID: `{"name": ..., "id": ...}` — `404` unknown strategy, `503` dependency unavailable |
| `POST` | `/api/benchmark` | Run the benchmark (body below) and return per-strategy `BenchmarkResult`s |
| `POST` | `/api/sharding/init` / `reset` | Create shard schemas / clear demo data |
| `POST` | `/api/sharding/scenario/{independent\|step\|gene\|hotspot}` | Run a sharding scenario; query params `shards`/`rows`/`users`/`orders`/`total`/`pct` are clamped server-side |
| `GET` | `/api/offline/sites` | Offline sites overview + central import stats |
| `POST` | `/api/offline/issue` | Offline issuance (local watermark only; optional `fake_date` simulates a broken terminal clock) |
| `POST` | `/api/offline/allocate` | Center appends a new quota range for a site |
| `POST` | `/api/offline/register` | Register a new offline site |
| `POST` | `/api/offline/import/{site_id}` | Ferry import with unique-index conflict detection |
| `POST` | `/api/offline/demo-collision` | Anti-pattern: two site-ID-less terminals colliding |

`POST /api/benchmark` request body:

```json
{
  "generators": ["snowflake", "uuid_v7", "redis_segment"],
  "total": 5000,
  "concurrency": 50
}
```

`generators` omitted or `null` = benchmark **all available** strategies; unavailable ones
are reported in `"skipped"`. `total` is clamped to ≤ 200000 and `concurrency` to ≤ 512.
Example:

```bash
curl -X POST http://localhost:8000/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{"generators": ["snowflake", "redis_incr", "redis_segment"], "total": 10000, "concurrency": 64}'
```

## Comparison Verdict

A short decision path — full reasoning in the sections above:

```
 Q1: Do IDs need to be time-ordered / database-index friendly?
 |__ NO  --> Q2: Any reason to leave the app process (coordination)?
 |           |__ NO   --> UUIDv4 (ubiquitous)  or  NanoID (shorter, URL-safe)
 |           |__ YES  --> you only need uniqueness; any strategy works,
 |                        so pick the cheapest: UUIDv4
 |
 |__ YES --> Q3: Can you add infrastructure (Redis / PostgreSQL)?
             |__ NO (in-process only)
             |        --> Q4: Need strict per-ms ordering + max QPS?
             |                  |__ YES --> Snowflake (needs unique machine ID,
             |                  |          clock discipline; ints only)
             |                  |__ NO  --> UUIDv7 / ULID (ms-order, no coordination)
             |
             |__ YES --> Q5: How strict must ordering be?
                        |__ Strictly increasing, gaps tolerable
                        |     |__ Redis already present?
                        |     |     |__ QPS < ~50k   --> Redis INCR (simplest)
                        |     |     |__ QPS higher   --> Redis segment (Leaf-style)
                        |     |__ PostgreSQL-centric, QPS modest
                        |           --> PostgreSQL sequence
                        |__ Trend-increasing is enough
                              --> Snowflake (int) or UUIDv7 (UUID-shaped)
```

Rules of thumb distilled from the benchmark:

- **Default for new services: UUIDv7** — index-friendly, zero dependencies, one-line answer.
- **Need 64-bit integers at high QPS: Snowflake** — but budget for machine-ID allocation
  and clock discipline, and understand the 5 ms drift behavior.
- **Strict order and you already run Redis: segment mode** — near-local QPS, amortized
  round trips, double-buffer hides refill latency.
- **Redis INCR** when simplicity beats throughput: one atomic op, one RTT, done.
- **PostgreSQL sequence** when the database *is* the coordination point and QPS is modest —
  never bolt Redis onto a stack purely for IDs without measuring.

## Contributing

Contributions welcome. Adding a strategy is intentionally cheap:

1. Subclass `BaseIDGenerator` in `app/generators/`, implement `generate()`
   (plus `check_ready()`/`warmup()`/`close()` if the strategy has dependencies).
2. Register the class in `REGISTRY` in `app/generators/__init__.py`.

The dashboard and every API route pick it up automatically. Bug reports on the
methodology (especially measurement artifacts) are just as valuable as new generators.

## License

Released under the [MIT License](LICENSE).

## 中文文档

This README is also available in Chinese: [README_zh-CN.md](README_zh-CN.md).
