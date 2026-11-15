# TA verification guide: our version of every command

The TAs' guide (Project 1 Verification Runbook) uses its own names (`vm1`-`vm4`, `iceberg_catalog.lake.pages`, `/serving/v0/docs.parquet`, ...). This file is the same checklist with **our** names, ready to paste. Same section numbers as the TA guide. Rehearse it once after RUNBOOK S40 (section 1), after the Phase 1 steps (sections 2-4).

| TA guide says | Ours |
|---|---|
| vm1 / vm2 / vm3 / vm4 | node1 / node2 / node3 / node4 (node2 = NameNode, node3 = Spark master, node4 = search) |
| `iceberg_catalog.lake.pages` | `iceberg_catalog.search.docs` |
| `iceberg_catalog.lake.pagerank` (`doc_id`, `rank`) | `iceberg_catalog.search.pagerank` (`doc_id`, `rank`) : same columns |
| `iceberg_catalog.lake.inverted_index` | `iceberg_catalog.search.postings` (`term`, `doc_id`, `tf`) |
| `--properties-file ~/spark-config/iceberg.conf` | not needed: the Iceberg catalog is in `state/conf/spark/spark-defaults.conf` (loaded through `SPARK_CONF_DIR`, set by `env.sh`) |
| `/serving/v0/` with `docs.parquet`, `index.parquet`, `_READY` | identical: HDFS `/serving/vN/` = `docs.parquet`, `index.parquet`, `manifest.json`, `_READY` (written last). node4 keeps its pulled copy in `~/bd_project/state/serving/vN/` |
| `http://localhost:8000/search?q=&limit=` and `/health` | work (aliases of `/api/search`, `/api/health`) |
| `{"status":"ok","version":1}` | `/health` returns `{"status":"ok","version":1,"docs":975,...}` (same shape; extra fields). `/api/health` keeps the string `"v1"` used by check.sh and the UI |

**Terminal opener on every node:** `source ~/bd_project/venv/bin/activate && cd ~/bd_project`
**On nodes 1-3:** `SQL="spark-sql --master spark://172.23.10.141:7077"` (cluster, like the TA). `--master local[2] --conf spark.eventLog.enabled=false` also works and is quicker.

---

## 1. After Phase 0

**node2**
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|^Name:"
hdfs dfs -ls -R / | grep -i jsonl
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations
```
Expect: `Live datanodes (2)` (node2 + node3 IPs); `pages.jsonl` non-zero; `Status: HEALTHY`, `Live_repl=2` with both IPs. UI: `http://node2:9870` → Datanodes (2), and Utilities → Browse the file system → `/search/input/phase-0`.

### Baseline (before Phase 1), write these down
**node4** (doc 94 is plain "Beard")
```bash
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool
```
**node3** (PageRank of doc 146; write the number down)
```bash
$SQL -e "SELECT doc_id, rank FROM iceberg_catalog.search.pagerank WHERE doc_id = '146'" 2>/dev/null
```
Baseline for doc 146: __________________

## 2. Live Phase 1 (one job after the other, check each)

### 2.1 Ingest (RUNBOOK S41), then on node1 or node3
```bash
$SQL -e "SELECT snapshot_id, committed_at, operation FROM iceberg_catalog.search.docs.snapshots" 2>/dev/null
```
Expect 2 snapshots (Phase 0 `append`, Phase 1 `overwrite`).

### 2.2 Index (RUNBOOK S44)
```bash
$SQL -e "SELECT term, doc_id FROM iceberg_catalog.search.postings WHERE doc_id = 'phase1-new-001' LIMIT 5" 2>/dev/null
```
Expect rows.

### 2.3 PageRank (RUNBOOK S45)
```bash
$SQL -e "SELECT COUNT(*) AS deleted_doc_ranks FROM iceberg_catalog.search.pagerank WHERE doc_id = '1'" 2>/dev/null
```
Expect 0.

### 2.4 Export (RUNBOOK S46), node2 or node3
```bash
hdfs dfs -ls /serving/v0/
hdfs dfs -ls /serving/v1/
```
Expect in each: `docs.parquet`, `index.parquet` (plain Parquet files), plus our `manifest.json` and the `_READY` marker.

## 3. Search verification

**node4**
```bash
curl -s http://localhost:8000/health
```
**node3**
```bash
hdfs dfs -test -e /serving/v1/_READY && echo "v1 ready"
```
Expect `"status":"ok"`, `"version":1`, `"docs":975`.

### 3.1 Doc 94 updated in place
**node4**: `curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool`
**node3**
```bash
$SQL -e "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id = '94'" 2>/dev/null
```
Expect `94  Beard (Phase 1)`.

### 3.2 Doc 146 (Cooking) gains rank
**node4**: `curl -s "http://localhost:8000/search?q=cooking&limit=5" | python3 -m json.tool`
**node3**
```bash
$SQL -e "SELECT doc_id, rank FROM iceberg_catalog.search.pagerank WHERE doc_id = '146'" 2>/dev/null
```
Expect higher than the baseline (links from doc 98 and phase1-new-011).

### 3.3 Multi-word query
```bash
curl -s "http://localhost:8000/search?q=plant+growth&limit=5"
```
Expect valid JSON, no 500.

## 4. Idempotency

**node1**
```bash
$SQL -e "SELECT COUNT(*) FROM iceberg_catalog.search.docs" 2>/dev/null     # note it (975)
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
$SQL -e "SELECT COUNT(*) FROM iceberg_catalog.search.docs" 2>/dev/null     # same
```
Expect the same count; the ingest prints `changes merged: 0` and the same snapshot id.

---

## Open points to check on the VMs (not yet run)
- Doc 146's links, `phase1-new-001` in `postings`, and doc 1 absent from `pagerank` are expectations from the TA's text; confirm the numbers in rehearsal.
- Our `doc_id` is a STRING, so always quote it (`'146'`).
