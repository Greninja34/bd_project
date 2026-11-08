"""Node 1 - ingestion + validation job.  Reads one phase's input from HDFS, merges it into Iceberg.

    spark-submit --properties-file ~/spark-config/search.conf \
        code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl

What it does, in order (each step is a block below):
  1. read the file line by line, keeping the line number
  2. parse + validate every line; bad lines get a reason instead of being silently dropped
  3. deduplicate by doc_id: highest `version` wins; with no version, the LAST copy in the file wins
  4. compare with the current Iceberg table (content hash) -> only real changes go into the MERGE
  5. MERGE INTO search.docs  (UPSERT = insert/replace, DELETE = remove)
  6. detect what changed FROM ICEBERG: time-travel diff of the snapshot before vs after the merge
  7. rebuild the link graph: drop links whose target does not exist (also after DELETEs)
  8. store the rejects (per phase) and print + log a summary

Idempotent: running the same phase again finds 0 changes in step 4, so docs / links / rejects /
doc_changes get no new snapshot and their contents stay identical. Only pipeline_runs (the audit
log) gets a new row.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402
from lake import t  # noqa: E402

REQUIRED = ("doc_id", "url", "title", "text")


def parse_line(pair, missing_links_policy):
    """One input line -> one row. `reason` is None for a good record, else why it was rejected.
    Runs on the executors, so it only uses the standard library."""
    import json
    line, idx = pair
    row = {"line_no": idx + 1, "raw": line[:500], "op": None, "doc_id": None, "url": None,
           "title": None, "text": None, "links": None, "links_missing": False,
           "version": None, "reason": None}

    def reject(reason):
        row["reason"] = reason
        return row

    if not line.strip():
        return reject("empty_line")
    try:
        rec = json.loads(line)
    except ValueError:
        return reject("unparseable_json")
    if not isinstance(rec, dict):
        return reject("not_a_json_object")

    op = rec.get("op")
    op = "UPSERT" if op is None else str(op).strip().upper()
    if op not in ("UPSERT", "DELETE"):
        return reject("unknown_op")
    row["op"] = op

    doc_id = rec.get("doc_id")
    if isinstance(doc_id, bool) or not isinstance(doc_id, (str, int)):
        return reject("missing_or_empty:doc_id")
    doc_id = str(doc_id).strip()
    if not doc_id:
        return reject("missing_or_empty:doc_id")
    row["doc_id"] = doc_id

    if op == "DELETE":           # a DELETE only needs the id; any body it carries is ignored
        return row

    for f in ("url", "title", "text"):
        v = rec.get(f)
        if not isinstance(v, str) or not v.strip():
            return reject("missing_or_empty:" + f)
        row[f] = v.strip() if f != "text" else v
    if not row["url"].lower().startswith(("http://", "https://")):
        return reject("bad_url")

    links = rec.get("links")
    if links is None:
        if missing_links_policy == "reject":
            return reject("missing_links")
        links, row["links_missing"] = [], True
    if not isinstance(links, list):
        return reject("bad_type:links")
    clean, seen = [], set()
    for target in links:
        if isinstance(target, bool) or not isinstance(target, (str, int)):
            continue
        target = str(target).strip()
        if target and target != doc_id and target not in seen:   # no empties, self links, repeats
            seen.add(target)
            clean.append(target)
    row["links"] = clean

    v = rec.get("version")
    try:
        row["version"] = None if v is None or isinstance(v, bool) else int(v)
    except (TypeError, ValueError):
        row["version"] = None
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="HDFS path of pages.jsonl or changes.jsonl")
    ap.add_argument("--phase", type=int, required=True)
    ap.add_argument("--missing-links", choices=["empty", "reject"], default="empty",
                    help="record with no 'links' key: keep it with links=[] (default) or reject it")
    args = ap.parse_args()

    spark = lake.get_spark(f"ingest-phase{args.phase}")
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import (ArrayType, BooleanType, IntegerType, LongType, StringType,
                                   StructField, StructType)
    source = os.path.basename(args.input)

    # ---- 1 + 2. read with line numbers, parse, validate --------------------------------------
    schema = StructType([
        StructField("line_no", LongType()), StructField("raw", StringType()),
        StructField("op", StringType()), StructField("doc_id", StringType()),
        StructField("url", StringType()), StructField("title", StringType()),
        StructField("text", StringType()), StructField("links", ArrayType(StringType())),
        StructField("links_missing", BooleanType()), StructField("version", LongType()),
        StructField("reason", StringType())])
    policy = args.missing_links
    lines = spark.sparkContext.textFile(args.input, minPartitions=4).zipWithIndex()
    parsed = spark.createDataFrame(lines.map(lambda p: parse_line(p, policy)), schema).cache()
    n_lines = parsed.count()
    bad = parsed.where(F.col("reason").isNotNull())
    good = parsed.where(F.col("reason").isNull())

    # ---- 3. deduplicate by doc_id --------------------------------------------------------------
    w = Window.partitionBy("doc_id").orderBy(F.col("version").desc_nulls_last(), F.col("line_no").desc())
    ranked = good.withColumn("rn", F.row_number().over(w))
    winners = ranked.where("rn = 1").drop("rn")
    losers = ranked.where("rn > 1").drop("rn")
    hash_cols = F.concat_ws("\u0001", "op", "url", "title", "text", F.array_join("links", ","))
    keep = winners.select("doc_id", F.col("line_no").alias("kept_line"), hash_cols.alias("kept_h"))
    dup_rejects = (losers.join(keep, "doc_id")
                   .withColumn("reason", F.lit("duplicate_superseded"))
                   .withColumn("detail", F.concat(
                       F.lit("kept line "), F.col("kept_line").cast("string"),
                       F.when(hash_cols == F.col("kept_h"), F.lit(" (identical copy)"))
                        .otherwise(F.lit(" (different content)")))))

    staged = (winners
              .withColumn("content_hash", F.when(
                  F.col("op") == "UPSERT",
                  F.sha2(F.concat_ws("\u0001", "url", "title", "text",
                                     F.array_join("links", "\u0002")), 256)))
              .cache())

    # ---- 4. compare with the current table: keep only real changes ----------------------------
    if not lake.table_exists(spark, "docs"):
        spark.sql(f"""CREATE TABLE {t('docs')} (
              doc_id STRING, url STRING, title STRING, text STRING, out_links ARRAY<STRING>,
              content_hash STRING, phase INT, ingested_at TIMESTAMP)
            USING iceberg TBLPROPERTIES ('format-version'='2')""")
    snap_before = lake.current_snapshot(spark, "docs")
    current = spark.table(t("docs")).select("doc_id", F.col("content_hash").alias("cur_h"))
    docs_before = current.count()
    joined = staged.join(current, "doc_id", "left")
    upserts = joined.where((F.col("op") == "UPSERT") &
                           (F.col("cur_h").isNull() | (F.col("cur_h") != F.col("content_hash"))))
    deletes = joined.where((F.col("op") == "DELETE") & F.col("cur_h").isNotNull())
    n_unchanged = joined.where((F.col("op") == "UPSERT") & (F.col("cur_h") == F.col("content_hash"))).count()
    n_delete_absent = joined.where((F.col("op") == "DELETE") & F.col("cur_h").isNull()).count()
    changes = (upserts.unionByName(deletes)
               .select("doc_id", "op", "url", "title", "text", F.col("links").alias("out_links"),
                       "content_hash")
               .withColumn("phase", F.lit(args.phase))
               .withColumn("ingested_at", F.current_timestamp())
               .cache())
    n_changes = changes.count()

    # ---- 5. MERGE INTO the Iceberg table ------------------------------------------------------
    if n_changes:
        changes.createOrReplaceTempView("incoming_changes")
        spark.sql(f"""
            MERGE INTO {t('docs')} AS tgt
            USING incoming_changes AS src
            ON tgt.doc_id = src.doc_id
            WHEN MATCHED AND src.op = 'DELETE' THEN DELETE
            WHEN MATCHED THEN UPDATE SET
                tgt.url = src.url, tgt.title = src.title, tgt.text = src.text,
                tgt.out_links = src.out_links, tgt.content_hash = src.content_hash,
                tgt.phase = src.phase, tgt.ingested_at = src.ingested_at
            WHEN NOT MATCHED AND src.op = 'UPSERT' THEN INSERT
                (doc_id, url, title, text, out_links, content_hash, phase, ingested_at)
                VALUES (src.doc_id, src.url, src.title, src.text, src.out_links,
                        src.content_hash, src.phase, src.ingested_at)
        """)
    snap_after = lake.current_snapshot(spark, "docs")

    # ---- 6. detect the changes from Iceberg (time travel: before vs after) --------------------
    diff = lake.snapshot_diff(spark, snap_before, snap_after).cache()
    counts = {r["change_type"]: r["count"] for r in diff.groupBy("change_type").count().collect()}
    if n_changes:
        out = (diff.withColumn("phase", F.lit(args.phase))
                   .withColumn("from_snapshot", F.lit(snap_before).cast("long"))
                   .withColumn("to_snapshot", F.lit(snap_after).cast("long")))
        if lake.table_exists(spark, "doc_changes"):
            out.writeTo(t("doc_changes")).append()
        else:
            out.writeTo(t("doc_changes")).using("iceberg").create()

    # ---- 7. link graph: only links whose source AND target exist -------------------------------
    docs_now = spark.table(t("docs"))
    docs_after = docs_now.count()
    all_links = docs_now.select(F.col("doc_id").alias("src"), F.explode("out_links").alias("dst"))
    ids = docs_now.select(F.col("doc_id").alias("dst"))
    valid_links = all_links.join(ids, "dst", "left_semi").select("src", "dst").distinct().cache()
    dangling = all_links.join(ids, "dst", "left_anti")
    n_links = valid_links.count()
    links_snapshot_changed = False
    if lake.table_exists(spark, "links"):
        old_links = spark.table(t("links"))
        if (valid_links.subtract(old_links).count() or old_links.subtract(valid_links).count()):
            valid_links.writeTo(t("links")).overwrite(F.lit(True))
            links_snapshot_changed = True
    else:
        valid_links.writeTo(t("links")).using("iceberg").create()
        links_snapshot_changed = True

    # ---- 8. rejects for this phase (replaced as a whole partition, so a re-run is identical) ---
    rej = (bad.select(F.col("line_no"), "doc_id", "reason", F.lit(None).cast("string").alias("detail"), "raw")
           .unionByName(dup_rejects.select("line_no", "doc_id", "reason", "detail", "raw"))
           .unionByName(dangling.select(F.lit(None).cast("long").alias("line_no"),
                                        F.col("src").alias("doc_id"),
                                        F.lit("dangling_link_dropped").alias("reason"),
                                        F.concat(F.lit("-> "), F.col("dst")).alias("detail"),
                                        F.lit(None).cast("string").alias("raw")))
           .withColumn("phase", F.lit(args.phase).cast(IntegerType()))
           .withColumn("source", F.when(F.col("reason") == "dangling_link_dropped", F.lit("links"))
                                  .otherwise(F.lit(source)))
           .select("phase", "source", "line_no", "doc_id", "reason", "detail", "raw")
           .cache())
    if not lake.table_exists(spark, "rejects"):
        rej.writeTo(t("rejects")).using("iceberg").partitionedBy(F.col("phase")).create()
    else:
        old = spark.table(t("rejects")).where(F.col("phase") == args.phase)
        if rej.subtract(old).count() or old.subtract(rej).count():
            rej.writeTo(t("rejects")).overwritePartitions()

    # ---- summary -------------------------------------------------------------------------------
    reasons = sorted((r["reason"], r["count"]) for r in rej.groupBy("reason").count().collect())
    n_missing_links = winners.where("links_missing").count()
    rows = [("input", args.input), ("lines read", n_lines),
            ("rejected (invalid) lines", bad.count())]
    rows += [(f"   reason {k}", v) for k, v in reasons
             if k not in ("dangling_link_dropped", "duplicate_superseded")]
    rows += [("duplicate copies superseded (same doc_id)", dict(reasons).get("duplicate_superseded", 0)),
             ("accepted records (unique doc_id)", staged.count()),
             ("   of which had no 'links' key (kept, links=[])" if policy == "empty"
              else "   no 'links' key", n_missing_links),
             ("   of which DELETE", staged.where("op = 'DELETE'").count()),
             ("unchanged (same content already in table)", n_unchanged),
             ("DELETE of a doc that is not in the table", n_delete_absent),
             ("changes merged", n_changes),
             ("detected from Iceberg: ADDED / UPDATED / DELETED",
              f"{counts.get('ADDED', 0)} / {counts.get('UPDATED', 0)} / {counts.get('DELETED', 0)}"),
             ("docs in table before -> after", f"{docs_before} -> {docs_after}"),
             ("links kept (both ends exist)", n_links),
             ("dangling links dropped", dict(reasons).get("dangling_link_dropped", 0)),
             ("links table rewritten", links_snapshot_changed),
             ("docs snapshot before -> after", f"{snap_before} -> {snap_after}")]
    lake.banner(f"INGESTION phase {args.phase}", rows)
    lake.log_run(spark, "ingest", args.phase, snap_after, {
        "input": args.input, "lines": n_lines, "rejected": bad.count(), "changes": n_changes,
        "added": counts.get("ADDED", 0), "updated": counts.get("UPDATED", 0),
        "deleted": counts.get("DELETED", 0), "docs": docs_after, "links": n_links,
        "snapshot_before": snap_before, "snapshot_after": snap_after,
        "links_snapshot": lake.current_snapshot(spark, "links")})
    spark.stop()


if __name__ == "__main__":
    main()
