# Spark Performance Optimization

A hands-on repo demonstrating the core PySpark performance techniques that come up constantly in data engineering interviews — each one shown as a runnable "naive vs. optimized" pair, with an explanation of *why* it's faster, not just that it is.

Every optimized version is unit-tested to produce identical output to the naive version — these techniques change **performance**, never **correctness**.

---

## Techniques Covered

| # | Technique | File |
|---|---|---|
| 1 | Broadcast Join | `src/01_broadcast_join.py` |
| 2 | Repartition vs. Coalesce | `src/02_repartition_vs_coalesce.py` |
| 3 | Partition Pruning | `src/03_partition_pruning.py` |
| 4 | Caching / Persistence | `src/04_caching.py` |
| 5 | Bucketing | `src/05_bucketing.py` |
| 6 | Adaptive Query Execution (AQE) | `src/06_adaptive_query_execution.py` |
| 7 | Skew Handling (salting) | `src/07_skew_handling.py` |
| 8 | Shuffle Optimization | `src/08_shuffle_optimization.py` |

---

## Repo Structure

```
spark-performance-optimization/
├── data/
│   └── generate_datasets.py     # Builds the fact/dimension/skewed datasets used across examples
├── src/
│   ├── 01_broadcast_join.py
│   ├── 02_repartition_vs_coalesce.py
│   ├── 03_partition_pruning.py
│   ├── 04_caching.py
│   ├── 05_bucketing.py
│   ├── 06_adaptive_query_execution.py
│   ├── 07_skew_handling.py
│   └── 08_shuffle_optimization.py
├── benchmarks/
│   └── run_benchmarks.py        # Runs every naive/optimized pair and prints timing comparisons
├── tests/
│   └── test_optimizations.py    # Verifies optimized output == naive output
├── .github/workflows/ci.yml
└── docs/
    └── interview_talking_points.md
```

---

## Running

```bash
pip install -r requirements.txt

# Generate the datasets every script depends on
python data/generate_datasets.py

# Run any individual technique
python src/01_broadcast_join.py

# Run all naive-vs-optimized comparisons with timing
python benchmarks/run_benchmarks.py

# Verify optimized output matches naive output for every technique
pytest tests/
```

---

## 1. Broadcast Join

**Problem:** joining a large fact table against a small dimension table triggers a full shuffle of both sides (`SortMergeJoin`), even though the dimension table would fit in memory on every executor.

**Fix:** `broadcast(dim_df)` hint (or let `spark.sql.autoBroadcastJoinThreshold` auto-detect it) ships the small table to every executor once, avoiding the shuffle entirely — turns a `SortMergeJoin` into a `BroadcastHashJoin`.

**When it doesn't help:** if the "small" table is actually larger than the broadcast threshold (default 10MB, tunable), forcing a broadcast can blow up executor memory instead of helping.

---

## 2. Repartition vs. Coalesce

**`repartition(n)`** — full shuffle, can increase or decrease partition count, results in evenly balanced partitions. Use when you need to increase parallelism or fix skew.

**`coalesce(n)`** — no shuffle, only decreases partition count, merges existing partitions without redistributing data. Use when reducing partitions before a write (fewer output files) and even distribution doesn't matter.

Using `coalesce` to increase partitions silently does nothing useful; using `repartition` to reduce partitions when you didn't need even distribution wastes a shuffle you didn't need.

---

## 3. Partition Pruning

Writing data partitioned by a frequently-filtered column (e.g., `sale_date`) lets Spark skip reading partitions entirely when a query filters on that column — visible in `explain()` as `PartitionFilters` in the physical plan instead of a full-table scan.

**Requirement:** the filter must be on the actual partition column, and use a static/foldable value — a filter on a derived expression won't prune.

---

## 4. Caching / Persistence

`.cache()` (alias for `.persist(StorageLevel.MEMORY_AND_DISK)`) avoids recomputing a DataFrame's lineage every time it's reused across multiple actions. Only worth it when a DataFrame is:
- reused more than once, **and**
- expensive enough to recompute that the memory/disk cost is worth it

Always `.unpersist()` when done — an uncleared cache is a common cause of executor memory pressure in long-running jobs.

---

## 5. Bucketing

Pre-shuffles and sorts data into a fixed number of buckets **at write time** based on a join/group key. If both tables in a join are bucketed identically (same column, same bucket count), Spark can join them without a shuffle at query time — the expensive part happened once, at write time, instead of on every query.

**Requirement:** both sides must be bucketed on the join key with the same bucket count, and this only works with tables registered in a metastore (not ad hoc DataFrames).

---

## 6. Adaptive Query Execution (AQE)

Spark 3.x's AQE (`spark.sql.adaptive.enabled`, on by default in modern Spark) re-optimizes the query plan **during** execution using actual runtime statistics instead of relying solely on static estimates:
- **Coalescing shuffle partitions** — merges small post-shuffle partitions automatically, no manual `spark.sql.shuffle.partitions` tuning needed for every job
- **Dynamically switching join strategies** — can convert a `SortMergeJoin` to a `BroadcastHashJoin` mid-plan if runtime stats show one side is actually small
- **Skew join optimization** — automatically splits skewed partitions (see #7)

---

## 7. Skew Handling (Salting)

When one join key has disproportionately more rows than others (e.g., one `customer_id` with a million transactions vs. hundreds for everyone else), a single task ends up handling that entire partition while every other task finishes instantly.

**Fix (manual — salting):** append a random suffix ("salt") to the skewed key on both sides of the join, spreading that key's rows across multiple partitions instead of one.

**Fix (automatic — AQE):** `spark.sql.adaptive.skewJoin.enabled` detects skewed partitions at runtime and splits them automatically, no code changes required in modern Spark. Worth knowing both — salting is what you'd have done on Spark 2.x, and understanding *why* AQE's version works shows you understand the underlying problem, not just the config flag.

---

## 8. Shuffle Optimization

General shuffle-reduction principles demonstrated together:
- Prefer `reduceByKey`-style combiner-side aggregation over `groupByKey`-style shuffle-everything-then-aggregate (in DataFrame terms: aggregate before a wide transformation where possible)
- Tune `spark.sql.shuffle.partitions` — the default (200) is frequently wrong for both very small and very large datasets
- Filter and select columns **before** a join/shuffle, not after, to reduce the volume of data actually being shuffled

See [`docs/interview_talking_points.md`](docs/interview_talking_points.md) for how to answer follow-up questions on each of these.
