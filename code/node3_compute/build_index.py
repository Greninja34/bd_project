"""Node 3 - build / update the distributed inverted index from the Iceberg docs table.

    spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/build_index.py

Output tables:
    search.postings       (term, doc_id, tf)      the inverted index; df is computed at export time
    search.doc_stats      (doc_id, length, snippet)
                          length = number of indexed terms (BM25's len(D))

Modes (chosen automatically, or forced with --full):
    full         first run, or --full: tokenize every doc, replace both tables
    incremental  later runs: ask Iceberg which docs changed since the snapshot we last indexed
                 (time-travel diff), delete their old postings, add postings for ADDED/UPDATED docs
    up-to-date   the docs snapshot is the one we already indexed: nothing to do (idempotent)

The heavy part (tokenizing ~10 MB of wiki markup, then grouping by (doc_id, term)) runs as
Spark tasks on the workers of Nodes 2 and 3; the groupBy is the shuffle you can see in the UI.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402
from lake import t  # noqa: E402


def build(spark, docs):
    """docs (doc_id, title, text) -> (doc_stats, postings) DataFrames."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType

    def _terms(title, text):
        import textproc                      # shipped to the executors by lake.get_spark()
        return textproc.doc_terms(title, text)

    def _snippet(text):
        import textproc
        return textproc.make_snippet(text)

    terms_udf = F.udf(_terms, ArrayType(StringType()))
    snippet_udf = F.udf(_snippet, StringType())
    tok = (docs.select("doc_id", terms_udf("title", "text").alias("terms"),
                       snippet_udf("text").alias("snippet"))
               .cache())
    stats = tok.select("doc_id", F.size("terms").alias("length"), "snippet")
    postings = (tok.select("doc_id", F.explode("terms").alias("term"))
                   .groupBy("term", "doc_id").agg(F.count(F.lit(1)).cast("int").alias("tf")))
    return stats, postings


def write_all(stats, postings):
    write_replace(stats, "doc_stats")
    write_replace(postings, "postings", ["term"])


def write_replace(df, name, sort_cols=None):
    from pyspark.sql import functions as F
    if sort_cols:
        df = df.sortWithinPartitions(*sort_cols)
    if lake.table_exists(df.sparkSession, name):
        df.writeTo(t(name)).overwrite(F.lit(True))
    else:
        df.writeTo(t(name)).using("iceberg").create()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, default=-1, help="only used for the run log")
    ap.add_argument("--full", action="store_true", help="rebuild everything from scratch")
    args = ap.parse_args()
    spark = lake.get_spark("build-index")
    from pyspark.sql import functions as F
    t0 = time.time()

    cur = lake.current_snapshot(spark, "docs")
    last = lake.last_run(spark, "index")
    have_tables = all(lake.table_exists(spark, n) for n in ("postings", "doc_stats"))
    docs = spark.table(t("docs")).select("doc_id", "title", "text")

    mode, changed = "full", None
    if not args.full and have_tables and last and last["docs_snapshot_id"] is not None:
        if last["docs_snapshot_id"] == cur:
            mode = "up-to-date"
        else:
            mode = "incremental"
            changed = lake.snapshot_diff(spark, last["docs_snapshot_id"], cur).cache()

    detail = {"mode": mode}
    if mode == "full":
        write_all(*build(spark, docs))
    elif mode == "incremental":
        by_type = {r["change_type"]: r["count"] for r in changed.groupBy("change_type").count().collect()}
        ids = [r["doc_id"] for r in changed.select("doc_id").collect()]
        detail.update({"added": by_type.get("ADDED", 0), "updated": by_type.get("UPDATED", 0),
                       "deleted": by_type.get("DELETED", 0)})
        print(f"[index] Iceberg says {len(ids)} docs changed since snapshot "
              f"{last['docs_snapshot_id']}: {by_type}")
        if ids:
            id_list = ", ".join("'" + i.replace("'", "''") + "'" for i in ids)
            # 1. remove every posting / stat of a changed doc (covers UPDATED and DELETED) ...
            for name in ("postings", "doc_stats"):
                spark.sql(f"DELETE FROM {t(name)} WHERE doc_id IN ({id_list})")
            # 2. ... then add fresh ones for the docs that still exist (ADDED and UPDATED)
            live = changed.where(F.col("change_type") != "DELETED").select("doc_id")
            stats, postings = build(spark, docs.join(live, "doc_id", "left_semi"))
            stats.writeTo(t("doc_stats")).append()
            postings.sortWithinPartitions("term").writeTo(t("postings")).append()
        # safety net: the incremental result must cover exactly the docs in the table
        st_ids = spark.table(t("doc_stats")).select("doc_id")
        all_ids = docs.select("doc_id")
        if st_ids.count() != all_ids.count() or st_ids.subtract(all_ids).count() or all_ids.subtract(st_ids).count():
            print("[index] incremental result inconsistent -> falling back to a full rebuild")
            write_all(*build(spark, docs))
            detail["mode"] = mode = "incremental->full"

    st = spark.table(t("doc_stats"))
    po = spark.table(t("postings"))
    n_docs = st.count()
    agg = st.agg(F.avg("length").alias("avg"), F.max("length").alias("mx")).collect()[0]
    n_post = po.count()
    n_terms = po.select("term").distinct().count()
    top = (po.groupBy("term").agg(F.count("*").alias("df")).orderBy(F.desc("df"), "term").limit(8).collect())
    detail.update({"docs": n_docs, "postings": n_post, "terms": n_terms,
                   "postings_snapshot": lake.current_snapshot(spark, "postings")})
    lake.banner("INVERTED INDEX", [
        ("mode", mode), ("docs snapshot indexed", cur), ("docs in index", n_docs),
        ("distinct terms", n_terms), ("postings (term, doc) rows", n_post),
        ("avg / max doc length (terms)", f"{agg['avg']:.1f} / {agg['mx']}"),
        ("most common terms (df)", ", ".join(f"{r['term']}={r['df']}" for r in top)),
        ("seconds", round(time.time() - t0, 1))])
    lake.log_run(spark, "index", args.phase, cur, detail)
    spark.stop()


if __name__ == "__main__":
    main()
