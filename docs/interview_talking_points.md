# Interview Talking Points

This repo exists to answer the single most common category of Spark interview question: *"How would you optimize a slow Spark job?"* Here's how to talk through each technique.

## 30-Second Summary

"I put together a repo demonstrating the core Spark performance techniques — broadcast joins, repartition vs. coalesce, partition pruning, caching, bucketing, AQE, skew handling, and shuffle tuning — each as a runnable naive-vs-optimized comparison with a correctness test proving the optimization only changes performance, never the output."

## Q&A by Technique

**"When would you use a broadcast join, and what can go wrong with it?"**
When one side of a join is small enough to fit comfortably in executor memory — avoids shuffling the large side entirely. Goes wrong when you force-broadcast a table that's actually larger than expected (e.g., after an upstream data volume increase); executors can OOM trying to hold it. `spark.sql.autoBroadcastJoinThreshold` auto-detects this within a size limit, but an explicit `broadcast()` hint overrides that safety check.

**"What's the difference between repartition and coalesce?"**
`repartition` does a full shuffle and can go up or down in partition count, producing evenly balanced partitions. `coalesce` avoids a shuffle by merging existing partitions, but can only reduce count, and distribution can be uneven since it doesn't redistribute rows. Use `coalesce` before a write to reduce file count cheaply; use `repartition` when you actually need balanced parallelism or are keying by a column before a groupBy/join.

**"How does partition pruning work, and why doesn't it always kick in?"**
Data physically laid out by a column (e.g., `partitionBy("sale_date")` at write time) lets Spark skip reading entire files/directories when a query filters on that column — visible as `PartitionFilters` in `explain()`. It doesn't kick in if the filter is on a derived/computed expression rather than the raw partition column, since Spark can't statically evaluate which partitions that maps to.

**"When is caching actually worth it?"**
Only when a DataFrame is reused across multiple actions AND expensive enough to recompute that the memory/disk cost is worth paying. Caching something used once is pure overhead. Always `unpersist()` when done — I've seen uncleared caches cause executor memory pressure in long-running jobs.

**"What's bucketing, and why doesn't everyone use it?"**
Pre-sorts and pre-partitions data into a fixed number of buckets by a key at write time, so joins on that key at query time skip the shuffle entirely. The catch: both tables must be bucketed on the same column with the same bucket count, and it only works through the metastore (`saveAsTable`), not ad hoc file reads — so it's a deliberate upfront investment, not something you retrofit onto an existing ad hoc pipeline easily.

**"What does AQE actually do, and is it always on?"**
Adaptive Query Execution re-optimizes the plan mid-execution using real runtime statistics instead of only static estimates — it coalesces small post-shuffle partitions automatically and can flip a SortMergeJoin to a BroadcastHashJoin if actual data turns out smaller than the optimizer guessed. It's been on by default since Spark 3.2, but I still set it explicitly in code so the behavior isn't silently dependent on cluster defaults.

**"How do you handle data skew?"**
Two answers, and I'd give both: manually via salting — append a random suffix to the skewed key on both sides of a join to spread it across partitions — or automatically via `spark.sql.adaptive.skewJoin.enabled`, which detects and splits skewed partitions at runtime without code changes in modern Spark. Knowing salting matters even with AQE on, because it's the fallback when AQE's skew detection doesn't catch a particular case, and because it demonstrates you understand *why* the skew problem happens, not just which flag fixes it.

**"How do you reduce shuffle overhead in general?"**
Filter and select columns before a wide transformation, not after — shuffle only what you'll actually use. Aggregate at the coarsest grain you actually need rather than grouping fine then re-aggregating. And tune `spark.sql.shuffle.partitions` deliberately — the default of 200 is frequently wrong in both directions: too many partitions for a small job wastes overhead on task scheduling, too few for a large job creates huge, slow partitions.

## What Ties It Together

If asked to summarize your overall approach to Spark performance: **look at the `explain()` plan first, always** — Exchange (shuffle) operators, join strategy (BroadcastHashJoin vs. SortMergeJoin), and whether partition filters are actually being applied tell you where the cost really is, rather than guessing and applying optimizations that don't address the actual bottleneck.
