"""Node 3 - PageRank over the cleaned link graph (search.links), written with RDDs.

    spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/pagerank.py --iterations 10

The algorithm (d = 0.85, N = number of docs, every doc starts at 1/N):

    PR(p) = (1 - d) / N  +  d * ( sum over q -> p of PR(q) / outdeg(q)  +  dangling / N )

`dangling` is the total rank of docs with no out-links. Without it that rank would leak out of the
graph every iteration (500 of our 1,000 docs have no out-link), so we spread it evenly over all
docs. Then the ranks always add up to 1, which the export job checks.

Why RDDs: each iteration is a join + reduceByKey that builds on the previous iteration's RDD, so
the lineage graph grows by one step per iteration. If a worker dies mid-job, Spark recomputes the
lost partitions from that lineage - that is the fault-tolerance demo (--slow-seconds makes each
task last long enough to kill a worker while it runs).
"""
import argparse
import hashlib
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402
from lake import t  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=10)
    ap.add_argument("--damping", type=float, default=0.85)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--slow-seconds", type=float, default=0.0,
                    help="sleep this long inside every task (fault-tolerance demo only)")
    ap.add_argument("--phase", type=int, default=-1, help="only used for the run log")
    args = ap.parse_args()
    if args.iterations < 10:
        sys.exit("the spec asks for at least 10 iterations")

    spark = lake.get_spark("pagerank")
    sc = spark.sparkContext
    d, P, slow = args.damping, args.partitions, args.slow_seconds
    t0 = time.time()

    snap = lake.current_snapshot(spark, "docs")
    doc_ids = spark.table(t("docs")).select("doc_id").rdd.map(lambda r: r[0])
    N = doc_ids.count()
    edges = spark.table(t("links")).select("src", "dst").rdd.map(lambda r: (r[0], r[1]))

    # adjacency list (src -> [dst, ...]); partitioned once and cached, reused by every iteration
    links = edges.groupByKey().mapValues(list).partitionBy(P).cache()
    n_edges = links.map(lambda kv: len(kv[1])).sum()
    n_with_out = links.count()
    ranks = doc_ids.map(lambda x: (x, 1.0 / N)).partitionBy(P).cache()
    all_ids = ranks.mapValues(lambda _: 0.0).cache()   # every doc, so docs with no in-links still get a rank

    def slow_down(it):
        if slow:
            time.sleep(slow)
        return it

    print(f"[pagerank] N={N} docs, {n_edges} links, {n_with_out} docs have out-links, "
          f"{N - n_with_out} are dangling; d={d}, {args.iterations} iterations")
    for i in range(1, args.iterations + 1):
        ti = time.time()
        joined = links.join(ranks)                                       # (src, ([dst..], rank))
        contribs = (joined.mapPartitions(slow_down)
                          .flatMap(lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]]))
        linked_mass = joined.map(lambda kv: kv[1][1]).sum()             # rank held by docs WITH out-links
        dangling = max(0.0, 1.0 - linked_mass)
        base = (1.0 - d) / N + d * dangling / N
        new_ranks = (all_ids.leftOuterJoin(contribs.reduceByKey(lambda a, b: a + b, P))
                            .mapValues(lambda v: base + d * (v[1] or 0.0))
                            .cache())
        delta = new_ranks.join(ranks).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum()
        total = new_ranks.values().sum()
        ranks.unpersist()
        ranks = new_ranks
        print(f"[pagerank] iter {i:2d}: sum={total:.6f}  L1 change={delta:.3e}  "
              f"dangling mass={dangling:.4f}  ({time.time() - ti:.1f}s)", flush=True)

    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    pr = spark.createDataFrame(ranks, StructType([StructField("doc_id", StringType()),
                                                  StructField("rank", DoubleType())]))
    pr = pr.withColumn("iterations", F.lit(args.iterations)).withColumn("docs_snapshot_id", F.lit(snap).cast("long"))
    if lake.table_exists(spark, "pagerank"):
        pr.writeTo(t("pagerank")).overwrite(F.lit(True))
    else:
        pr.writeTo(t("pagerank")).using("iceberg").create()

    res = spark.table(t("pagerank"))
    titles = spark.table(t("docs")).select("doc_id", "title")
    top = res.join(titles, "doc_id").orderBy(F.desc("rank"), "doc_id").limit(10).collect()
    st = res.agg(F.sum("rank").alias("s"), F.max("rank").alias("mx"), F.min("rank").alias("mn")).collect()[0]
    n_min = res.where(F.abs(F.col("rank") - st["mn"]) < 1e-12).count()
    # fingerprint of the result: identical with and without a worker failure
    fp = hashlib.md5("".join(f"{r[0]}:{r[1]:.10f};" for r in
                             sorted(res.select("doc_id", "rank").collect())).encode()).hexdigest()[:12]
    lake.banner("PAGERANK", [
        ("docs / links", f"{N} / {n_edges}"), ("iterations / damping", f"{args.iterations} / {d}"),
        ("sum of ranks", f"{st['s']:.6f}"),
        ("max / min rank", f"{st['mx']:.6f} / {st['mn']:.6f}  (max/min = {st['mx'] / st['mn']:.2f})"),
        ("docs tied at the minimum", n_min), ("result fingerprint", fp),
        ("seconds", round(time.time() - t0, 1))] +
        [(f"top {k + 1:2d}", f"{r['rank']:.6f}  {r['title']} ({r['doc_id']})") for k, r in enumerate(top)])
    lake.log_run(spark, "pagerank", args.phase, snap, {
        "iterations": args.iterations, "damping": d, "docs": N, "links": n_edges,
        "fingerprint": fp, "links_snapshot": lake.current_snapshot(spark, "links")})
    spark.stop()


if __name__ == "__main__":
    main()
