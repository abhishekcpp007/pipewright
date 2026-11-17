# pipewright

An incremental warehouse build over a deliberately messy source system. Change feed in,
Parquet lake, merged silver tables, type 2 dimensions, a fact that joins to the customer as
they were at order time, and a quality gate that stops a bad batch before it reaches anything
somebody reports from.

The pipeline half is ordinary. The half worth reading is what the source system does to it. A
generator builds the OLTP database and writes down every defect it injected, so the tests can
assert that the pipeline caught exactly that many and no more. Duplicate payment rows, refunds
booked as negative captures, orders backdated three days by an offline mobile client, a column
that appears in the source on day eight, customer updates arriving from a lagged replica with
timestamps that go backwards. All of it is counted in `data/source/injected.json` before the
pipeline ever runs.

Four results came out the other way from what I expected, and they are the first things to
read. Polling the source costs you 69 percent of your history and not one rupee of revenue.
Point in time attribution protects a third of a percent of the money, not the tenth everybody
implies. Partitioning the lake made the one day query slower. Threading the extract made the
whole thing slower.

```bash
make install      # uv venv, editable install
make gen          # the source OLTP database, defects and all
make replay       # 14 daily runs in order, which is the only honest way to test incremental
make dq           # 33 expectations against the warehouse, non-zero if any error check fails
make test         # 105 tests
make report       # read the gold tables
make state        # watermarks, partitions, what each materialization decided
make bench        # the four measurements below
make docker-test  # the same lint and suite inside the image, network switched off
```

A run looks like this:

```
$ pw run --date 2026-10-14 -v
run 20261117T185148-9237fa | batch 2026-10-14 | 20754 rows extracted | 16 models |
  33 checks, 0 failed, 1 warned | success
  warn  silver_customers: every email parses -> 9
  silver_orders       incremental  rows=41721   rows_inserted=9467 rows_rejected_as_stale=0
  silver_payments     incremental  rows=35461   rows_inserted=2600 rows_rejected_as_stale=0
  dim_customer        scd2         rows=3467    new_keys=89 new_versions=35 unchanged=4
                                               late_versions_dropped=1
  fct_orders          incremental  rows=41721   rows_inserted=9467 rows_rejected_as_stale=0
  agg_daily_city_revenue  table    rows=176
```

## The shape of it

```
bronze (5 views over Parquet)
  -> silver (5 merged tables + quarantine)
    -> dim_customer, dim_product (type 2)
      -> fct_orders
        -> agg_daily_city_revenue, agg_customer_ltv
```

Sixteen models in five layers, resolved from the `ref()` calls inside the SQL rather than from a
list somebody has to remember to update. `pw lineage --format mermaid` prints the graph, and
`pw lineage` prints it as text with the materialization of each node.

| layer | models | materialization |
|---|---|---|
| bronze | 5 | views over hive partitioned Parquet, `union_by_name` so a new column unions with the files written before it |
| silver | 6 | incremental merge on a key, plus the quarantine table |
| dim | 2 | type 2 with a validity window per version |
| gold | 3 | one incremental fact, two rebuilt aggregates |

Fourteen daily runs over the whole source take 5.2 seconds end to end, of which the extract is
0.81s. The lake is 2.83 MB across 70 Parquet files. The fact ends at 41,721 orders and
6.56 crore rupees of revenue.

## The source system breaks things on purpose

`pw gen` writes a SQLite database and a JSON file saying what it did to it.

| what it did | how many | what has to happen |
|---|---|---|
| duplicate payment rows, byte for byte | 42 | collapse to one row, same one every time |
| duplicate payment ids with different amounts | 28 | collapse deterministically, last write wins |
| refunds booked as negative captures | 28 | quarantined with a reason, out of revenue |
| items with a quantity of zero | 92 | quarantined, out of the fact |
| items whose order does not exist | 28 | quarantined, but only after a grace window |
| orders backdated by an offline client | 451 | land in the right day, flagged late arriving |
| customer updates with timestamps going backwards | 31 | never overwrite a newer version |
| customers with no city | 7 | filled as unknown, not dropped |
| customers with an unparseable email | 7 | warn and keep, not an error |
| a column the source grows on day 8 | 1 | the early Parquet files still read |

After the replay, every one of those lands where it should. The quarantine holds 158 rows:

| reason | rows | injected |
|---|---|---|
| non_positive_qty | 92 | 92 |
| negative_amount | 28 | 28 |
| orphan_order_id | 24 | 28, four still inside the two day grace window |
| zero_amount | 7 | not injected, these are payments for orders whose only items had zero quantity |
| unparseable_email | 7 | 7 |

The 451 backdated orders are flagged in the fact, and exactly 451 rows carry the flag. That
number is asserted in CI, because a late arriving order quietly booked to today is the kind of
bug that only shows up as a weird looking Tuesday.

## What polling the source costs you

Most tutorial pipelines poll: read the source table, compare `updated_at` against a watermark,
take what is newer. The problem is that `updated_at` holds one timestamp. An order that went
from placed to paid to shipped between two runs only has the last of those in the column, and
the two before it are gone for good.

pipewright supports both. The source writes a change feed as well as the live table, and
`pw run --mode poll` reads the live table instead. Same database, same fourteen daily runs,
same models.

| mode | rows extracted | order versions landed | distinct order states seen | orders in the fact | revenue |
|---|---|---|---|---|---|
| cdc | 278,120 | 134,665 | 134,652 | 41,721 | 6.56 crore |
| poll | 184,419 | 41,734 | 41,721 | 41,721 | 6.56 crore |

Polling missed 92,931 order state changes, which is 69 percent of them.

The part I had wrong: it does not cost you a rupee of revenue. Both modes converge on the same
fact, the same delivered count and the same total, because the final state of an order is the
same whichever way you got there.

Polling is fine right up to the first question about the path instead of the destination. How
long an order sits in paid before it ships, which orders reached refunded from delivered, what
the cancel rate looked like at 6pm on the Friday, whether the shipped to delivered gap grew
after a carrier change. A table that only remembers where things ended up cannot answer any of
those, and no modelling downstream puts the history back once it is gone. That is the real bill
for polling, and it arrives months later when somebody asks.

`bench/cdc_vs_poll.py` builds both warehouses and prints the table above.

## What point in time attribution is actually worth

`fct_orders` joins to `dim_customer` with an ASOF join on the order timestamp, so a row carries
the city and tier the customer had when they ordered. Join to `is_current` instead and last
month's revenue by city changes every time somebody moves house.

That is the textbook argument and it is correct. It is also smaller than I assumed:

- 1,750 orders out of 41,721, so 4.2 percent, land in a different city depending on which join you use
- 0.34 percent of revenue moves between cities
- the largest single city shift is Kochi, 2.1 percent of its revenue

So the honest version is: the type 2 dimension is not protecting a tenth of your revenue, it is
protecting about a third of a percent of it, and the 4.2 percent of orders that would be
misfiled is the number to quote instead. Which matters a lot if you pay regional commission off
that table and barely at all if you are reading a trend line. The reason to build it this way
anyway is that the 0.34 percent is not stable: it grows every month the dimension lives, and
once a fact is built on the current join there is no way to recover what the right answer was.

81 orders, 0.19 percent, fall back to the current version because they were backdated to before
their customer's first version existed. The fact records that in a column rather than hiding it,
and a warn level expectation watches the count.

### The surrogate key had to stop being a sequence

The first version handed out surrogate keys from a counter. Then the test that rebuilds the
whole warehouse and compares it against the incremental build started failing on one column:
every row matched except `customer_sk`, because a full rebuild inserts the versions in a
different order and the counter hands out different numbers. Any fact built before the rebuild
now points at keys that mean something else.

The keys are a hash of the business key and the moment the version opened. Eight bytes wider
than they need to be, and the dimension rebuilds to the same keys on any machine in any order.

## The hygiene check that deleted 92 percent of the revenue

This one was a real bug, found by the benchmark rather than the test suite.

`silver_order_items` used to drop an item whose order was not in the warehouse. That reads as
obvious hygiene and it is wrong, because the parent and the child do not arrive together. Items
land in the batch where the order was placed. The order's own row lands in the batch where its
status last changed, which can be five days later. A referential filter at ingest time throws
away a legitimate item, and nothing ever reconsiders it.

Under the change feed the damage was small, because an insert and its items share a batch. Under
polling it deleted 92 percent of the revenue, and the first benchmark run reported 50 lakh
rupees against 6.56 crore. Both numbers were computed by a pipeline whose 33 expectations all
passed, which is the part worth sitting with.

What it does now: keep the item, record `order_known` as it was at ingest time, and let the
quarantine decide. An item whose order has not shown up is pending, not orphaned, and it only
becomes an orphan after a two day grace window. The quarantine model is rebuilt whole every run
rather than merged, because that is a verdict that has to be allowed to change its mind.

## Incremental, and what makes it safe to repeat

Four materializations: `view`, `table`, `incremental`, `scd2`. The incremental merge is the one
that does the work.

- the extract's cursor is inclusive on the low side, so the rows sitting exactly on the last
  watermark get read twice. That is deliberate. A writer committing a row with a timestamp equal
  to one already read would be lost by an exclusive cursor, silently. The duplicates cost
  nothing because every silver model merges on a key
- the file write goes to a temp name and is renamed into place, so a reader never sees a half
  written partition and a crashed run leaves nothing to clean up
- the manifest hashes the data, not the file, so the same batch extracted twice hashes the same
  even when Parquet encodes it differently, and the second copy is refused
- a model can declare a watermark column, and then the merge only gives up a stored row if the
  incoming one is at least as new. It is what keeps a lagged replica from walking an order back
  from delivered to placed

On this dataset that last guard never fires, because the change feed delivers versions in order.
It is insurance, and `tests/test_warehouse.py` is where it gets proven, not the fixture.

Three tests say the incremental path is safe, and the third is the one I would want if I only got
one: run the same batch twice and nothing moves, backfill five days already processed and nothing
moves, then rebuild the entire warehouse from scratch and compare it against the incremental
build row by row. If those ever diverge, every number in every report is quietly wrong and no
other test in the suite would say so.

```
one batch, incremental :  0.208s     31,979 rows touched
everything, full refresh:  1.531s    234,045 rows touched
incremental is 7.4x faster on 7.3x fewer rows
```

Which is the unexciting result, and the right one: the speedup tracks the row count almost
exactly. Incremental is not clever, it just does less.

## Quality gates

33 expectations in `pw/expectations.yml`, twelve types, declared per model and run as SQL right
after the model is built. The full replay evaluates 462 of them.

| type | what it catches |
|---|---|
| `not_null`, `unique`, `accepted_values`, `range` | the ordinary column level things |
| `relationship` | a child pointing at a parent that is not there |
| `row_count_min` | an empty batch dressed up as a successful one |
| `freshness` | the pipeline ran, the data did not arrive |
| `volume_anomaly` | today's row count against the median of the last seven runs |
| `scd2_one_current`, `scd2_no_overlap`, `scd2_closed_history` | a dimension whose history stopped making sense |
| `custom_sql` | anything else, with the model substituted in |

Severity decides the consequence. A warn is written to the metadata store and the run carries on.
An error opens the circuit: the model stays built so it can be looked at, and everything
downstream of it is skipped for that batch. So a bad silver table does not get averaged into a
gold aggregate, and the gold aggregate keeps yesterday's numbers rather than becoming a wrong
version of today's.

`scd2_one_current` is the one that earned its place. The dimension builder closed the previous
version with a SQL update, which quietly did nothing when a customer's second and third version
arrived inside the same batch, because the row it was trying to close had not been inserted yet.
Two rows per customer flagged current, every downstream join doubled, and the test caught it on
the first run.

The only expectation that fails on the shipped dataset is the email format check, 7 rows, warn,
and those 7 are the 7 the generator broke.

## The benchmarks

`bench/` has four, and they write their JSON into `bench/results/` so the numbers in this README
can be regenerated rather than remembered.

**Parquet against CSV, and partition pruning against not bothering.** `order_items`, 103,606 rows
across 14 daily partitions.

| | size | one day of it |
|---|---|---|
| Parquet, partitioned by ingest date | 0.85 MB | 1.39 ms |
| Parquet, one file | 0.47 MB | 0.56 ms |
| CSV | 5.62 MB | 44.83 ms |

Parquet is worth it: 6.6 times smaller than CSV and 32 times faster to answer a one day question.
The partitioning is not, at this size. Opening 14 files costs more than scanning one small one,
and the single file compresses better because it has one dictionary instead of fourteen. At a
hundred million rows the answer flips, which is exactly why the number is in a script instead of
in my head.

**The textbook SQL rewrite.** Three correlated subqueries rewritten as window functions, with the
rows compared before either time is reported, because a faster query that answers a different
question is not a rewrite.

| query | correlated | window | speedup |
|---|---|---|---|
| hours since the customer's previous order | 36.3 ms | 14.9 ms | 2.4x |
| each customer's largest order | 5.0 ms | 4.5 ms | 1.1x |
| running revenue by city | 1.7 ms | 1.3 ms | 1.3x |

2.4x, on the query everybody uses as the example of a hundred times win. DuckDB decorrelates the
subquery, so the rewrite is mostly cosmetic. The same pair on SQLite, same 41,721 rows:

| | correlated | window | speedup |
|---|---|---|---|
| no index | 49.11 s | 0.05 s | 1029x |
| with the right index | 0.04 s | 0.03 s | 1.1x |

So the rewrite is worth three orders of magnitude on a row store with no index, and nothing at
all once the index exists. The lesson I took out of this is not about window functions. It is
that "I rewrote the slow query" is a weaker claim than "I looked at what the engine was doing",
and the two answers here differ by a factor of a thousand.

**Threading the extract, which I got wrong.** Five source tables, one thread each at most.

| threads | 14 daily extracts |
|---|---|
| 1 | 0.765 s |
| 2 | 0.820 s |
| 5 | 1.083 s |

0.71x at five threads. The metadata store is one DuckDB file with one writer, so every watermark
read and manifest commit queues behind the same lock while the actual work is a few milliseconds
of SQLite reads. The default is one thread now. The knob stays, because a wider source or object
storage instead of a local disk moves the answer, and the point of having the number is knowing
which way.

## What the tests cover

105 tests, no network, 61 seconds.

| file | what it holds down |
|---|---|
| `test_dag.py` | config parsing, refs from the query, topological order, cycles, selectors |
| `test_source.py` | the generator is deterministic, its defect counters match the tables, the feed holds more than the live table, the column appears on day 8 |
| `test_extract.py` | watermarks move, the window bound holds, no temp files survive, a committed batch is refused by its checksum, a new column unions with old files |
| `test_warehouse.py` | all four materializations, the watermark guard, and nine cases of the type 2 builder including the two-versions-in-one-batch bug |
| `test_quality.py` | every expectation type against a fixture with known violations, severity, tolerances, a broken expectation query failing loudly |
| `test_pipeline.py` | the end to end invariants: the fact loses no orders, the quarantine reconciles against the injected counts, a repeat run changes nothing, a backfill changes nothing, a full rebuild matches the incremental build, retries work, a failed model stops its descendants, a quality failure does the same |
| `test_metadata.py` | watermarks, the manifest, what a resume would pick up, surrogate key stability |
| `test_cli.py` | every verb, on a warehouse built inside the test |

CI runs the suite, then builds the warehouse from scratch and asserts the gold numbers against
`injected.json`, then runs the same batch twice and compares the aggregate row by row. The docker
job runs lint and the suite inside the image with `--network none`, so a pass means the tests
really do not reach for a network.

## What this is not

- not a scheduler. There is a DAG runner with retries, a resume path and per task state, and it
  is a few hundred lines. Airflow and Dagster exist and do far more
- not distributed. DuckDB on one machine, which is the right answer for a lot more workloads
  than people admit, and the wrong answer past a few hundred gigabytes
- the source system is generated. That is the point, because the defects are known and counted,
  but it does mean nothing here has met a real upstream team's creativity
- no streaming. Daily batches with a change feed, which is where most of this work actually lives

## Layout

```
pw/
  source.py       the OLTP generator and the change feed, with its defect counters
  extract.py      incremental extract, inclusive cursor, cdc or poll
  lake.py         atomic Parquet writes, content checksums, hive partitions
  warehouse.py    the four materializations, including the type 2 builder
  dag.py          model parsing, refs, topological sort, selectors
  quality.py      the expectation engine, twelve types
  runner.py       the orchestrator: layers, retries, the quality gate, backfill
  metadata.py     watermarks, manifest, run state, dq results, model stats
  models/         16 SQL models
  expectations.yml
bench/            four measurements, results committed as JSON
tests/            105 tests
```
