"""Node 2 - one-screen report of the lakehouse: every table, its row count and snapshots.

    spark-submit --properties-file ~/spark-config/search.conf code/node2_storage/lake_report.py

Read-only. Good for screenshots after each phase, and for checking idempotency
(run it, re-run an ingestion, run it again: docs/links/rejects must show the same snapshot ids).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402
from lake import t  # noqa: E402

TABLES = ["docs", "links", "rejects", "doc_changes", "doc_stats", "postings",
          "pagerank", "pipeline_runs"]


def main():
    spark = lake.get_spark("lake-report")
    rows = []
    for name in TABLES:
        if not lake.table_exists(spark, name):
            rows.append((name, "(not created yet)"))
            continue
        n = spark.table(t(name)).count()
        snaps = spark.sql(f"SELECT count(*) FROM {t(name)}.snapshots").collect()[0][0]
        rows.append((f"{name:<14}", f"{n:>7} rows | {snaps:>2} snapshots | current {lake.current_snapshot(spark, name)}"))
    lake.banner("LAKEHOUSE  (iceberg_catalog.search)", rows)

    if lake.table_exists(spark, "docs"):
        print("search.docs history (newest last):")
        spark.sql(f"""SELECT h.made_current_at, h.snapshot_id, h.is_current_ancestor AS current_line,
                             s.operation, s.summary['added-records'] AS added,
                             s.summary['deleted-records'] AS deleted, s.summary['total-records'] AS total
                      FROM {t('docs')}.history h JOIN {t('docs')}.snapshots s USING (snapshot_id)
                      ORDER BY h.made_current_at""").show(50, False)
        refs = spark.sql(f"SELECT name, type, snapshot_id FROM {t('docs')}.refs ORDER BY name")
        print("tags / branches on search.docs:")
        refs.show(20, False)
    if lake.table_exists(spark, "rejects"):
        print("rejects by phase and reason:")
        spark.sql(f"SELECT phase, reason, count(*) AS n FROM {t('rejects')} GROUP BY phase, reason "
                  f"ORDER BY phase, reason").show(50, False)
    if lake.table_exists(spark, "pipeline_runs"):
        print("pipeline runs (audit log):")
        spark.sql(f"SELECT run_ts, stage, phase, docs_snapshot_id, substr(details, 1, 90) AS details "
                  f"FROM {t('pipeline_runs')} ORDER BY run_ts").show(100, False)
    spark.stop()


if __name__ == "__main__":
    main()
