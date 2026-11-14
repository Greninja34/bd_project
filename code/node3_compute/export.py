"""Node 3 - validate everything, then export plain Parquet for the search service on Node 4.

    spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/export.py \
        --version v0 --export-root hdfs://node2:9000/search/exports

Writes, in HDFS:
    <root>/<version>/docs/      Parquet: doc_id, url, title, length, pagerank, snippet
    <root>/<version>/postings/  Parquet: term, doc_id, tf, df
    <root>/<version>/manifest.json   counts, snapshot ids, ranking parameters, every check's result
    <root>/<version>/_SUCCESS        written LAST: "this export is complete and valid"

How a half-written or invalid export can never be served:
  * everything is written into <root>/_tmp_<version>_<time> first;
  * if any check fails, that folder is renamed to _failed_<version>_<time> and the job exits 1;
  * only when every check passes do we write manifest + _SUCCESS and rename the folder to
    <root>/<version> (an HDFS rename is atomic: Node 4 sees either nothing or the whole export).
Node 4 only switches to a version whose _SUCCESS exists and whose manifest says status = OK.

--simulate-failure deliberately breaks the data (for the demo "keep the old version if export fails").
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402
from lake import t  # noqa: E402

K1, B, W_BM25, W_PR = 1.2, 0.75, 0.7, 0.3


class HFS:
    """Tiny wrapper over Hadoop's FileSystem API (works for hdfs:// and file:// paths)."""

    def __init__(self, spark, root):
        self.jvm = spark._jvm
        self.fs = self.jvm.org.apache.hadoop.fs.Path(root).getFileSystem(spark._jsc.hadoopConfiguration())

    def p(self, path):
        return self.jvm.org.apache.hadoop.fs.Path(path)

    def exists(self, path):
        return self.fs.exists(self.p(path))

    def write_text(self, path, text):
        out = self.fs.create(self.p(path), True)
        out.write(bytearray(text.encode("utf-8")))
        out.close()

    def rename(self, src, dst):
        if not self.fs.rename(self.p(src), self.p(dst)):
            raise RuntimeError(f"rename {src} -> {dst} failed")

    def delete(self, path):
        self.fs.delete(self.p(path), True)

    def files(self, path):
        it = self.fs.listFiles(self.p(path), True)
        out = []
        while it.hasNext():
            st = it.next()
            out.append((str(st.getPath()), int(st.getLen())))
        return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, help="e.g. v0 for phase 0, v1 for phase 1")
    ap.add_argument("--export-root", required=True, help="e.g. hdfs://node2:9000/search/exports")
    ap.add_argument("--overwrite", action="store_true", help="replace an existing export of this version")
    ap.add_argument("--simulate-failure", action="store_true", help="demo: corrupt the export on purpose")
    ap.add_argument("--phase", type=int, default=-1)
    args = ap.parse_args()
    spark = lake.get_spark(f"export-{args.version}")
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    t0 = time.time()
    root = args.export_root.rstrip("/")
    final = f"{root}/{args.version}"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tmp = f"{root}/_tmp_{args.version}_{stamp}"
    fs = HFS(spark, root)
    if fs.exists(final) and not args.overwrite:
        sys.exit(f"{final} already exists (pass --overwrite to replace it)")

    # ---- gather the inputs: all from the CURRENT Iceberg snapshots ----------------------------
    snap = lake.current_snapshot(spark, "docs")
    idx_run, pr_run = lake.last_run(spark, "index"), lake.last_run(spark, "pagerank")
    docs = spark.table(t("docs")).select("doc_id", "url", "title")
    stats = spark.table(t("doc_stats"))
    pr = spark.table(t("pagerank")).select("doc_id", F.col("rank").alias("pagerank"))
    out_docs = docs.join(stats, "doc_id", "left").join(pr, "doc_id", "left").cache()
    w = Window.partitionBy("term")
    out_post = (spark.table(t("postings"))
                .withColumn("df", F.count(F.lit(1)).over(w).cast("int"))
                .select("term", "doc_id", "tf", "df").cache())
    if args.simulate_failure:
        print("[export] --simulate-failure: dropping PageRank for 5 docs and adding a posting for a missing doc")
        victims = [r[0] for r in out_docs.orderBy("doc_id").limit(5).collect()]
        out_docs = out_docs.withColumn("pagerank", F.when(F.col("doc_id").isin(victims), None)
                                                     .otherwise(F.col("pagerank"))).cache()
        out_post = out_post.unionByName(spark.createDataFrame(
            [("ghost", "no-such-doc", 1, 1)], "term string, doc_id string, tf int, df int")).cache()

    # ---- the checks ----------------------------------------------------------------------------
    n_docs_tbl = docs.count()
    n_docs = out_docs.count()
    n_post = out_post.count()
    pr_sum = out_docs.agg(F.sum("pagerank")).collect()[0][0] or 0.0
    checks = []

    def check(name, ok, detail, level="error"):
        checks.append({"check": name, "ok": bool(ok), "detail": str(detail), "level": level})

    check("docs exported = docs in Iceberg table", n_docs == n_docs_tbl, f"{n_docs} vs {n_docs_tbl}")
    check("at least 1 doc and 1 posting", n_docs > 0 and n_post > 0, f"{n_docs} docs, {n_post} postings")
    n_bad_id = out_docs.where(F.col("doc_id").isNull() | (F.trim("doc_id") == "")).count()
    check("no empty doc_id", n_bad_id == 0, n_bad_id)
    n_dup = n_docs - out_docs.select("doc_id").distinct().count()
    check("doc_id unique", n_dup == 0, f"{n_dup} duplicates")
    n_no_url = out_docs.where(F.col("url").isNull() | F.col("title").isNull()).count()
    check("every doc has url and title", n_no_url == 0, n_no_url)
    n_no_len = out_docs.where(F.col("length").isNull()).count()
    check("every doc is in the index (has a length)", n_no_len == 0, f"{n_no_len} missing")
    n_no_pr = out_docs.where(F.col("pagerank").isNull()).count()
    check("every doc has a PageRank", n_no_pr == 0, f"{n_no_pr} missing")
    check("PageRank sums to 1", abs(pr_sum - 1.0) < 1e-3, f"{pr_sum:.6f}")
    n_orphan = out_post.join(out_docs.select("doc_id"), "doc_id", "left_anti").count()
    check("every posting points at an exported doc", n_orphan == 0, f"{n_orphan} orphan postings")
    n_bad_tf = out_post.where((F.col("tf") <= 0) | F.col("tf").isNull()).count()
    check("tf > 0", n_bad_tf == 0, n_bad_tf)
    check("index built from the current docs snapshot",
          idx_run is not None and idx_run["docs_snapshot_id"] == snap,
          f"index used {idx_run and idx_run['docs_snapshot_id']}, current is {snap}")
    check("PageRank computed from the current docs snapshot",
          pr_run is not None and pr_run["docs_snapshot_id"] == snap,
          f"pagerank used {pr_run and pr_run['docs_snapshot_id']}, current is {snap}")
    pr_iters = json.loads(pr_run["details"]).get("iterations", 0) if pr_run else 0
    check("PageRank ran >= 10 iterations", pr_iters >= 10, pr_iters)
    ok = all(c["ok"] for c in checks if c["level"] == "error")

    # ---- write -----------------------------------------------------------------------------------
    out_docs.select("doc_id", "url", "title", "length", "pagerank", "snippet") \
            .coalesce(1).write.mode("overwrite").parquet(f"{tmp}/docs")
    out_post.orderBy("term", "doc_id").coalesce(1).write.mode("overwrite").parquet(f"{tmp}/postings")
    avgdl = out_docs.agg(F.avg("length")).collect()[0][0]
    files = [{"path": "/".join(p.rsplit("/", 2)[-2:]), "bytes": n}     # e.g. docs/part-00000-....parquet
             for p, n in fs.files(tmp) if p.endswith(".parquet")]
    manifest = {
        "version": args.version, "status": "OK" if ok else "FAILED", "created_at": stamp,
        "phase": args.phase, "docs_snapshot_id": snap,
        "links_snapshot_id": lake.current_snapshot(spark, "links"),
        "counts": {"docs": n_docs, "postings": n_post,
                   "terms": out_post.select("term").distinct().count()},
        "avgdl": avgdl, "pagerank_sum": pr_sum,
        "ranking": {"k1": K1, "b": B, "w_bm25": W_BM25, "w_pagerank": W_PR,
                    "damping": json.loads(pr_run["details"]).get("damping") if pr_run else None,
                    "pagerank_iterations": pr_iters},
        "checks": checks, "files": files}
    fs.write_text(f"{tmp}/manifest.json", json.dumps(manifest, indent=2))

    rows = [(("PASS " if c["ok"] else ("WARN " if c["level"] == "warn" else "FAIL ")) + c["check"], c["detail"])
            for c in checks]
    if not ok:
        failed = f"{root}/_failed_{args.version}_{stamp}"
        fs.rename(tmp, failed)
        lake.banner(f"EXPORT {args.version}: VALIDATION FAILED - nothing published", rows + [("left in", failed)])
        lake.log_run(spark, "export", args.phase, snap, {"version": args.version, "status": "FAILED"})
        spark.stop()
        sys.exit(1)

    fs.write_text(f"{tmp}/_SUCCESS", "")
    if fs.exists(final):
        fs.rename(final, f"{root}/_replaced_{args.version}_{stamp}")
    fs.rename(tmp, final)                       # atomic publish
    lake.banner(f"EXPORT {args.version}: OK", rows + [
        ("published to", final), ("docs / postings / terms",
                                  f"{n_docs} / {n_post} / {manifest['counts']['terms']}"),
        ("seconds", round(time.time() - t0, 1))])
    lake.log_run(spark, "export", args.phase, snap, {"version": args.version, "status": "OK", "path": final})
    spark.stop()


if __name__ == "__main__":
    main()
