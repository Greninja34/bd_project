"""Cluster smoke test. Run it from Node 1 AND from Node 3 once HDFS + Spark are up:

    spark-submit --properties-file ~/spark-config/search.conf ~/project_search_engine/code/tools/cluster_check.py

It proves, in ~30 seconds, the four things the real jobs need:
  1. tasks run on BOTH workers (node2 and node3), not just one
  2. the executors use Python 3.10 (Spark 3.3.4 breaks on 3.11+)
  3. the executors can import our shared code (shipped with addPyFile)
  4. Iceberg works on the HDFS warehouse, and a table written from one driver is visible from
     another (run it on node1, then node3: the row count goes up by one each time)
Clean up afterwards with --drop.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402


def where(_):
    import platform
    import textproc                                  # proves addPyFile reached this executor
    return (os.environ.get("SPARK_LOCAL_HOSTNAME") or socket.gethostname(),
            platform.python_version(), textproc.tokenize("Hello World")[0])


def main():
    spark = lake.get_spark("cluster-check")
    sc = spark.sparkContext
    me = os.environ.get("SPARK_LOCAL_HOSTNAME") or socket.gethostname()
    if "--drop" in sys.argv:
        spark.sql("DROP TABLE IF EXISTS iceberg_catalog.check.hello PURGE")
        print("dropped iceberg_catalog.check.hello")
        return
    print(f"\n[check] driver on {me}, master {sc.master}, default parallelism {sc.defaultParallelism}")
    seen = sorted(set(sc.parallelize(range(32), 8).map(where).collect()))
    for host, py, tok in seen:
        print(f"[check] task ran on {host:<10} python {py}  textproc ok ({tok})")
    hosts = {h for h, _, _ in seen}
    print("[check] workers used:", ", ".join(sorted(hosts)),
          "-> OK" if len(hosts) >= 2 else "-> ONLY ONE WORKER! check the Spark master UI (node3:8080)")
    if any(not py.startswith("3.10") for _, py, _ in seen):
        print("[check] WARNING: an executor is not on Python 3.10 - set spark.pyspark.python")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.check")
    spark.sql("CREATE TABLE IF NOT EXISTS iceberg_catalog.check.hello "
              "(written_by STRING, at TIMESTAMP) USING iceberg")
    spark.sql(f"INSERT INTO iceberg_catalog.check.hello VALUES ('{me}', current_timestamp())")
    print("[check] iceberg_catalog.check.hello now contains:")
    spark.sql("SELECT * FROM iceberg_catalog.check.hello ORDER BY at").show(truncate=False)
    wh = spark.conf.get("spark.sql.catalog.iceberg_catalog.warehouse")
    print(f"[check] warehouse: {wh}   (look: hdfs dfs -ls -R /warehouse/check)")


if __name__ == "__main__":
    main()
