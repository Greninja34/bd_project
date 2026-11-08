"""Small helpers shared by every Spark job: table names, snapshot ids, the run log.

All tables live in the Iceberg catalog `iceberg_catalog` (the name the course guide uses),
namespace `search`, warehouse on HDFS (see ~/spark-config/search.conf).

    search.docs          one row per live document (doc_id, url, title, text, out_links, content_hash, ...)
    search.links         cleaned link graph (src, dst): both ends exist, no self links, no duplicates
    search.rejects       every rejected / dropped input, with a reason (partitioned by phase)
    search.doc_changes   what each ingestion run changed (ADDED / UPDATED / DELETED), from Iceberg snapshots
    search.doc_stats     per-doc length + snippet for BM25 and the UI          (written by the index job)
    search.postings      the inverted index: (term, doc_id, tf)                 (written by the index job)
    search.pagerank      (doc_id, rank)                                         (written by the PageRank job)
    search.pipeline_runs audit log: one row per stage run, with the snapshot ids it used/produced
"""
import json
import os
import sys
from datetime import datetime, timezone

CATALOG = "iceberg_catalog"
NS = "search"
HERE = os.path.dirname(os.path.abspath(__file__))


def t(name):
    """Fully qualified table name, e.g. t('docs') -> iceberg_catalog.search.docs"""
    return f"{CATALOG}.{NS}.{name}"


def get_spark(app_name):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Ship the shared modules to the executors (Nodes 2 and 3) so UDFs can import them.
    for f in ("textproc.py", "lake.py"):
        spark.sparkContext.addPyFile(os.path.join(HERE, f))
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}")
    return spark


def table_exists(spark, name):
    try:
        spark.table(t(name)).schema
        return True
    except Exception:
        return False


def current_snapshot(spark, name):
    """Snapshot id the table currently points at (None if the table has no snapshot yet)."""
    if not table_exists(spark, name):
        return None
    # .history has one row each time the table's pointer moved (commits AND rollbacks);
    # the newest row that is still an ancestor of the current state is the current snapshot.
    rows = spark.sql(
        f"SELECT snapshot_id FROM {t(name)}.history WHERE is_current_ancestor "
        f"ORDER BY made_current_at DESC LIMIT 1").collect()
    return int(rows[0][0]) if rows else None


def read_at(spark, name, snapshot_id):
    """Time travel: the table as it was at `snapshot_id` (None -> an empty read of the current schema)."""
    if snapshot_id is None:
        return spark.table(t(name)).limit(0)
    return spark.read.option("snapshot-id", int(snapshot_id)).table(t(name))


def snapshot_diff(spark, old_snap, new_snap):
    """Which docs changed between two snapshots of search.docs, using Iceberg time travel.

    Returns a DataFrame (doc_id, change_type) with change_type in ADDED / UPDATED / DELETED.
    content_hash tells an UPDATE apart from an untouched row.
    """
    from pyspark.sql import functions as F
    old = read_at(spark, "docs", old_snap).select("doc_id", F.col("content_hash").alias("old_h"))
    new = read_at(spark, "docs", new_snap).select("doc_id", F.col("content_hash").alias("new_h"))
    j = old.join(new, "doc_id", "full_outer")
    return (j.withColumn("change_type",
                         F.when(F.col("old_h").isNull(), "ADDED")
                          .when(F.col("new_h").isNull(), "DELETED")
                          .when(F.col("old_h") != F.col("new_h"), "UPDATED"))
             .where(F.col("change_type").isNotNull())
             .select("doc_id", "change_type"))


def last_run(spark, stage, phase=None):
    """Most recent pipeline_runs row for a stage (as a dict), or None."""
    if not table_exists(spark, "pipeline_runs"):
        return None
    q = f"SELECT * FROM {t('pipeline_runs')} WHERE stage = '{stage}'"
    if phase is not None:
        q += f" AND phase = {int(phase)}"
    rows = spark.sql(q + " ORDER BY run_ts DESC LIMIT 1").collect()
    return rows[0].asDict() if rows else None


def log_run(spark, stage, phase, docs_snapshot, details):
    """Append one row to the audit log. This is the only table that grows on every run."""
    from pyspark.sql.types import (LongType, IntegerType, StringType, StructField,
                                   StructType, TimestampType)
    schema = StructType([
        StructField("run_ts", TimestampType()), StructField("stage", StringType()),
        StructField("phase", IntegerType()), StructField("docs_snapshot_id", LongType()),
        StructField("details", StringType())])
    row = (datetime.now(timezone.utc).replace(tzinfo=None), stage, int(phase),
           None if docs_snapshot is None else int(docs_snapshot), json.dumps(details, sort_keys=True))
    df = spark.createDataFrame([row], schema)
    if table_exists(spark, "pipeline_runs"):
        df.writeTo(t("pipeline_runs")).append()
    else:
        df.writeTo(t("pipeline_runs")).using("iceberg").create()


def banner(title, rows):
    """Print a readable summary block (this is what you screenshot)."""
    width = max([len(title)] + [len(f"{k}: {v}") for k, v in rows]) + 4
    print("\n" + "=" * width, file=sys.stdout)
    print(f"  {title}")
    print("-" * width)
    for k, v in rows:
        print(f"  {k}: {v}")
    print("=" * width + "\n", flush=True)
