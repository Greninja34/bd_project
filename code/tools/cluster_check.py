"""Cluster smoke test. Run it from node1 AND from node3 once HDFS + Spark are up:

    bd check          (runs: spark-submit code/tools/cluster_check.py)
    bd check --drop   (removes the test table afterwards)

It proves, in about 30 seconds, the four things the real jobs need:
  1. tasks run on BOTH workers (node2 and node3), not just one
  2. the executors use Python 3.10 (Spark 3.3.4 breaks on 3.11+)
  3. the executors can import our shared code (shipped with addPyFile)
  4. Iceberg works on the HDFS warehouse, and a table written from one driver is visible from
     another (run it on node1, then node3: the row count goes up by one each time)

If an executor cannot connect back to this driver (firewall, WSL2 NAT, wrong address) the job
sees only one worker. Then this script says which worker is missing and exits with 1.
"""
import os
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402

WORKERS = ("node2", "node3")                         # the two nodes that run a Spark worker


def name_of(host, ips):
    """Executors report IPs; show the team name when we know it."""
    return ips.get(host, host)


def where(_):
    import platform
    import textproc                                  # proves addPyFile reached this executor
    time.sleep(0.3)                                  # keep tasks alive so they spread over all executors
    try:                                             # /etc/hosts (bd setup) maps the hostname to the team IP
        host = socket.gethostbyname(socket.gethostname())
    except OSError:
        host = socket.gethostname()
    return (host, platform.python_version(), textproc.tokenize("Hello World")[0])


def executor_hosts(sc):
    """Hosts of the live executors (the driver is in the map too: remove it)."""
    it = sc._jsc.sc().getExecutorMemoryStatus().keysIterator()      # Scala iterator of "host:port"
    keys = []
    while it.hasNext():
        keys.append(str(it.next()))
    driver = sc.getConf().get("spark.driver.host")
    # Only the driver's own block manager entry goes. On node3 an executor of the node3 worker has the
    # same IP as the driver but another port: it is a real executor and must be counted.
    drv_key = f"{driver}:{sc.getConf().get('spark.driver.blockManager.port', '')}"
    return driver, sorted(k.rsplit(":", 1)[0] for k in keys if k != drv_key)


def main():
    spark = lake.get_spark("cluster-check")
    sc = spark.sparkContext
    me_ip = os.environ.get("MY_IP", "?")
    me = os.environ.get("BD_NODE", socket.gethostname())
    ips = {os.environ[f"NODE{i}_IP"]: f"node{i}" for i in range(1, 5) if os.environ.get(f"NODE{i}_IP")}
    if "--drop" in sys.argv:
        spark.sql("DROP TABLE IF EXISTS iceberg_catalog.check.hello PURGE")
        print("dropped iceberg_catalog.check.hello")
        return
    print(f"\n[check] driver on {me} ({sc.getConf().get('spark.driver.host')}:"
          f"{sc.getConf().get('spark.driver.port', '?')}), master {sc.master}")
    # Executors start one by one; the one next to the driver is usually first and would otherwise
    # finish all the tiny tasks before the other worker's executor has registered.
    driver, hosts = [], []
    for _ in range(120):
        driver, hosts = executor_hosts(sc)
        if len(set(hosts)) >= 2:
            break
        time.sleep(1)
    print("[check] executors registered: " + (", ".join(name_of(h, ips) for h in hosts) or "none"))

    seen = sorted(set(sc.parallelize(range(32), 8).map(where).collect()))
    for host, py, tok in seen:
        print(f"[check] task ran on {name_of(host, ips):<10} python {py}  textproc ok ({tok})")
    used = {name_of(h, ips) for h, _, _ in seen}
    if len(used) < 2:
        missing = [w for w in WORKERS if w not in used]
        print(f"[check] workers used: {', '.join(sorted(used)) or 'none'} -> ONLY ONE WORKER")
        print(f"[check] DIAGNOSIS: the executor on {' / '.join(missing)} never ran a task. It probably never")
        print(f"[check] connected back to this driver at {me_ip}:40000. The Spark master UI (node3:8080)")
        print("[check] may still list both workers: the master is reachable, the driver is not.")
        print("[check] Run `bd netcheck` on all 4 nodes at the same time.")
        print("[check] If this driver is node3 on WSL2, see TROUBLESHOOTING.md#wsl2")
        sys.exit(1)
    print("[check] workers used:", ", ".join(sorted(used)), "-> OK")
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
