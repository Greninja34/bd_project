# Demo day: the steps, one after the other

The cluster is already up before the examiner arrives (RUNBOOK S01-S28 done, gates 0-3 PASSED, the search service from S28 running on node4 showing "no version loaded").
Do this once as a rehearsal 1-2 days before, then reset.

**Terminal opener** (every node, every new terminal):
```bash
source ~/bd_project/venv/bin/activate && cd ~/bd_project
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
```
**Stuck?** `bash scripts/doctor.sh`, paste the output in the chat, then [TROUBLESHOOTING.md](TROUBLESHOOTING.md). Expected numbers for every step: [RUNBOOK.md](RUNBOOK.md) (same step id). Never re-format HDFS during the demo.

Spec items: **(1)** ingestion + dirty records + Iceberg, **(2)** inverted index, **(3)** PageRank, **(4)** fault tolerance, **(5)** BM25, **(6)** search + UI before/after changes.

Tabs open: Spark master `http://172.23.10.141:8080` · history `:18080` · NameNode `http://node2:9870` · UI `http://node4:8000`.

---

## D01 · node2 + node3 · Show the cluster (RUNBOOK S22, S25)
**node2**
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|^Name:"
curl -s http://172.23.10.141:8080/json/ | python3 -c "import sys,json; print([ (w['host'],w['state']) for w in json.load(sys.stdin)['workers']])"
```
Point at: 2 live DataNodes (node2 + node3 IPs), 2 workers ALIVE. Say: node1 and node4 are only clients.

## D02 · node1 · Phase 0 ingestion (1) (S29-S31)
```bash
hdfs dfs -put -f data/phase-0/pages.jsonl /search/input/phase-0/pages.jsonl
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations | grep -E "Live_repl|Average block replication"
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
$SQL -e "SELECT phase, reason, count(*) FROM iceberg_catalog.search.rejects GROUP BY phase, reason" 2> state/logs/sql.log
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
Point at: `Live_repl=2` · 1040 read, 20 rejected, 20 duplicates, **1000 docs**, 499 links · re-run: `changes merged: 0`, same snapshot id (**idempotent**).
**say in chat:** "Phase 0 ingested" + the snapshot id.

## D03 · node3 · Index, PageRank, export v0 (2, 3) (S33-S35)
```bash
spark-submit code/node3_compute/build_index.py --phase 0 2> state/logs/index-p0.log
spark-submit code/node3_compute/pagerank.py --phase 0 2> state/logs/pagerank-p0.log
spark-submit code/node3_compute/export.py --version v0 --phase 0 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v0.log
```
Point at: 37,551 terms / 233,643 postings (the `groupBy` is the shuffle) · PageRank 10 iterations, sum 1.000000, fingerprint `60a6221350e7` · 13 checks PASS, `EXPORT v0: OK`.
**say in chat:** "v0 exported".

## D04 · node4 · Search v0 (5, 6) (S37-S38)
```bash
python code/node4_search/pull_export.py --version v0 --source hdfs://node2:9000/search/exports
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py april
python code/node4_search/q.py ocean
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py zzzqqq
```
Then the UI `http://node4:8000`: search "bubonic plague" and "april"; show the score bars (blue BM25, orange PageRank).
Say: April is 5th because it has the best BM25 but no in-links; the spec's formula `0.7*BM25/max + 0.3*PR/max`.

## D05 · node2 · Tag Phase 0, before Phase 1 (S39)
```bash
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs" 2> state/logs/sql.log
```
Point at: 1 snapshot, `phase0` tag. **say in chat:** "go for Phase 1".

## D06 · node1 · Phase 1 ingestion (1) (S41-S42)
```bash
hdfs dfs -put -f data/phase-1/changes.json /search/input/phase-1/changes.jsonl
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
$SQL -e "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type ORDER BY phase, change_type" 2> state/logs/sql.log
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
Point at: ADDED/UPDATED/DELETED **25/25/50**, docs 1000 -> **975**, 7 dangling links dropped (detected from Iceberg snapshots) · re-run: nothing merged.
**say in chat:** "Phase 1 ingested".

## D07 · node3 · Phase 1 index, PageRank, export v1 (2, 3) (S44-S46)
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1.log
spark-submit code/node3_compute/export.py --version v1 --phase 1 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1.log
```
Point at: "Iceberg says 100 docs changed", `mode: incremental` (only those docs re-indexed) · 975 docs · fingerprint `54ee091e74b5` · 13 PASS, `EXPORT v1: OK`.
**say in chat:** "v1 exported".

## D08 · node4 · v0 is still served, then pull v1 (6) (S48)
```bash
python code/node4_search/q.py "alan turing"
python code/node4_search/pull_export.py --version v1 --source hdfs://node2:9000/search/exports
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py april
python code/node4_search/q.py phase
```
Point at: before the pull Alan Turing is found; after the pull he is gone (DELETED in Phase 1) · "Bubonic plague (Phase 1)" (UPDATED) · "phase" matches 20 -> 69 (the 25 added pages). The service never stopped.

## D09 · node2 · Time travel (1) (S49)
```bash
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase1" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase1" 2> state/logs/sql.log
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs VERSION AS OF 'phase0'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
```
Point at: 1000 vs 975 · history shows `append 1000` then `overwrite` (copy-on-write).

## D10 · node3 + node2 · Spark fault tolerance (4) (S51-S53)
**node3**, terminal 1:
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 --slow-seconds 3 2> state/logs/pagerank-p1-slow.log
```
**say in chat:** "running". When node3's screen shows `iter 2` or `iter 3`, **node2**:
```bash
jps | grep CoarseGrainedExecutorBackend
kill -9 <that pid>
```
After the job finished, **node3**, terminal 2:
```bash
grep -oE "Lost executor|ExecutorLostFailure|No more replicas|FetchFailed" state/logs/pagerank-p1-slow.log | sort | uniq -c
```
Point at: the job survived, the killed iteration is slower, "Lost executor", `FetchFailed`, and **the same fingerprint `54ee091e74b5`**. Say: Spark recomputed the lost partitions from the lineage.

## D11 · node3 + node4 · A broken export never reaches the search (S54-S55)
**node3**
```bash
spark-submit code/node3_compute/export.py --version v1-broken --phase 1 --simulate-failure --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1-broken.log
echo "exit code $?"
```
**say in chat:** "broken export made". **node4**
```bash
python code/node4_search/pull_export.py --version v1-broken --source hdfs://node2:9000/search/exports; echo "exit code $?"
curl -s localhost:8000/api/health; echo
```
Point at: 3 FAIL lines, `VALIDATION FAILED - nothing published` · `REFUSED ... Still serving v1`.

## D12 · node4 · Switch versions (S56)
```bash
curl -s -X POST localhost:8000/api/admin/switch/v0; echo
python code/node4_search/q.py "alan turing"
curl -s -X POST localhost:8000/api/admin/switch/v1; echo
python code/node4_search/q.py "alan turing"
```
Point at: Alan Turing back on v0, gone on v1.

## D13 · node2, node4, node3 · A bad update, then recovery (1) (S57-S60)
**node2**
```bash
$SQL -e "DELETE FROM iceberg_catalog.search.docs WHERE title LIKE 'C%'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
```
**node4** (search unaffected):
```bash
python code/node4_search/q.py "computer science"
```
**node2** (roll back):
```bash
GOOD=$($SQL -e "SELECT snapshot_id FROM iceberg_catalog.search.docs.refs WHERE name = 'phase1'" 2> state/logs/sql.log); echo "GOOD=$GOOD"
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.docs', $GOOD)" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history" 2> state/logs/sql.log
```
**node3**
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
```
Point at: 878 rows -> rollback -> **975** · the bad snapshot has `is_current_ancestor = false` · index says `up-to-date` · search never changed (it serves exported Parquet).

## D14 (optional) · node3 + node2 · A DataNode dies (S61-S64)
**node3**
```bash
hdfs --daemon stop datanode
```
**node2**
```bash
hdfs dfs -cat /search/input/phase-0/pages.jsonl | wc -l
# about 90 seconds later:
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication|Status"
```
**node3**
```bash
hdfs --daemon start datanode
```
**node2**, about 30 s later: same two commands. Point at: reads work with one DataNode down (1040 lines) · `Live 1 / Dead 1`, under-replicated, still HEALTHY · back to `Live 2`, 0 under-replicated, replication 2.0.

## D15 (optional) · node3 · Where the time and traffic go (S65)
```bash
ZT_IF=$(ip -o -4 addr show | awk -v ip=$MY_IP '$4 ~ "^"ip"/" {print $2}')
rx0=$(cat /sys/class/net/$ZT_IF/statistics/rx_bytes); tx0=$(cat /sys/class/net/$ZT_IF/statistics/tx_bytes)
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1-meter.log
echo "received $(( ($(cat /sys/class/net/$ZT_IF/statistics/rx_bytes) - rx0) / 1048576 )) MB, sent $(( ($(cat /sys/class/net/$ZT_IF/statistics/tx_bytes) - tx0) / 1048576 )) MB"
```
Then `http://node3:18080` -> the application -> *Stages*. Say: the data is tiny; PageRank is latency-bound (10 iterations, a shuffle each).

---

Time: D01-D13 about 30 min. Cut D14-D15 first if short, never D02-D13.
Each member must be able to explain their steps: [VIVA.md](VIVA.md) section 7. Likely first question to M4 (D04, D08, D11, D12): BM25 with k1 = 1.2, b = 0.75, the two bugs in the course snippet, the final-score normalisation, and why a version switch can't serve a broken export.
