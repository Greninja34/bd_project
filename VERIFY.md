# VERIFY: demo day, the TA verification run (10 minutes)

The TAs' guide (Project 1 Verification Runbook) with **our** names and the exact commands that ran on our cluster. Phase 0 is already done and serving when the examiner arrives. Only Phase 1 is run live, then the TA checks. The RUNBOOK's Stage 10 demos (S51-S65) are **not** part of this run.

## 0. Names: TA guide to ours

| TA guide | Ours |
|---|---|
| vm1 / vm2 / vm3 / vm4 | node1 / node2 / node3 / node4 (node2 = NameNode, node3 = Spark master, node4 = search service) |
| `iceberg_catalog.lake.pages` | `iceberg_catalog.search.docs` |
| `iceberg_catalog.lake.inverted_index` | `iceberg_catalog.search.postings` (`term`, `doc_id`, `tf`) |
| `iceberg_catalog.lake.pagerank` | `iceberg_catalog.search.pagerank` (`doc_id`, `rank`) |
| `--properties-file ~/spark-config/iceberg.conf` | not needed: the Iceberg catalog is in `state/conf/spark/spark-defaults.conf` (`env.sh` sets `SPARK_CONF_DIR`) |
| `/serving/vN/` with `docs.parquet`, `index.parquet`, `_READY` | the same, plus our `manifest.json`. `_READY` is written last |
| `http://localhost:8000/search?q=&limit=` and `/health` | work (`/health` returns `"version":1`); `/api/search` and `/api/health` also exist |

`doc_id` is a string, so always quote it (`'146'`).

**Terminal opener on every node:**
```bash
source ~/bd_project/venv/bin/activate && cd ~/bd_project
SQL="spark-sql --master spark://172.23.10.141:7077"      # nodes 1, 2, 3 (cluster, as in the TA guide)
```

---

## A. Phase 0 build: do it tonight, then shut down cleanly

The cluster is at the end of Stage 9 (Phase 1 applied, v1 served). Take it back to the Phase 0 state, with no Phase 1 residue. Do **not** format HDFS again.

**A1. Reset (node2, then node4)**
```bash
# node2
hdfs dfs -rm -r -skipTrash /warehouse/search /warehouse/check '/serving/*' /search/input/phase-1/changes.jsonl
hdfs dfs -ls -R /warehouse /serving /search/input
```
```bash
# node4: Ctrl-C the service in terminal A, then
rm -rf ~/bd_project/state/serving && mkdir -p ~/bd_project/state/serving
# start it again (terminal A, RUNBOOK S28):
cd ~/bd_project/code/node4_search && uvicorn app:app --host 0.0.0.0 --port 8000 2>&1 | tee ~/bd_project/state/logs/serve.log
```
Expect: `/warehouse` empty, `/serving` empty, `/search/input/phase-0/pages.jsonl` still there, `/health` says `no-version`.

**A2. Phase 0 (same commands as RUNBOOK S29-S40, one node after the other)**

| Step | Node | Command | Expect |
|---|---|---|---|
| S29 | node1 | `hdfs dfs -put -f data/phase-0/pages.jsonl /search/input/phase-0/pages.jsonl` then `hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations \| grep -E "Live_repl\|Average block replication"` | `Live_repl=2` (node2 + node3 IPs), `Average block replication: 2.0` |
| S30 | node1 | `spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log` | read 1040, rejected 20, duplicates 20, accepted 1000, links 499, docs 0 → 1000 |
| S31 | node1 | the same command again | changes merged 0, same snapshot before = after |
| S32 | node1, node2 | `bash scripts/check.sh ingest0` | `GATE ingest0 PASSED` |
| S33 | node3 | `spark-submit code/node3_compute/build_index.py --phase 0 2> state/logs/index-p0.log` | `mode: full`, docs 1000, terms 37551, postings 233643 |
| S34 | node3 | `spark-submit code/node3_compute/pagerank.py --phase 0 2> state/logs/pagerank-p0.log` | N=1000, sum 1.000000, fingerprint `60a6221350e7` |
| S35 | node3 | `spark-submit code/node3_compute/export.py --version v0 --phase 0 --export-root hdfs://node2:9000/serving 2> state/logs/export-v0.log` | 13 × PASS, `EXPORT v0: OK`, 1000 / 233643 / 37551 |
| S36 | node3, node2 | `bash scripts/check.sh index` and `bash scripts/check.sh export v0` | both `PASSED` |
| S37 | node4 | `python code/node4_search/pull_export.py --version v0 --source hdfs://node2:9000/serving` | `verified v0: 1000 docs, 233643 postings, 13/13`, `CURRENT: None -> v0`, `switched: True` |
| S39 | node2 | `spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log`, then `$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase0"` and the same for `links` | tag `phase0` on `docs` and `links` |
| S40 | node4 | `bash scripts/check.sh pulled v0` and `bash scripts/check.sh serve v0` | both `PASSED` |

Quick sanity on node4: `python code/node4_search/q.py "bubonic plague"` (first: Bubonic plague), `... "alan turing"` (Alan Turing), `... april` (April is 5th). Then take the two **baselines** in section 1 and write them down.

**A3. Shut down cleanly tonight (RUNBOOK S67-S70, in this order)**
```bash
# node4: Ctrl-C the service in terminal A
# node3
stop-history-server.sh; stop-worker.sh; stop-master.sh; hdfs --daemon stop datanode
# node2 (last: the NameNode)
stop-worker.sh; hdfs --daemon stop datanode; hdfs --daemon stop namenode
# all nodes
bash ~/bd_project/scripts/check.sh down
```
Do not delete `~/bd_project/state` and do not re-format. Leave the VM and WSL settings (Bridged on node4, `generateHosts=false` on node3) as they are.

**A4. Start up tomorrow morning (about 5 min, no format)**
```bash
# node2
hdfs --daemon start namenode; sleep 10; hdfs --daemon start datanode; start-worker.sh spark://172.23.10.141:7077
# node3
hdfs --daemon start datanode; start-master.sh; start-worker.sh spark://172.23.10.141:7077; start-history-server.sh
# all nodes, then check the network first (ping node1-node4) and ZeroTier peers
bash ~/bd_project/scripts/check.sh hdfs      # GATE hdfs PASSED
bash ~/bd_project/scripts/check.sh spark     # GATE spark PASSED
# node4: start the service (S28); it loads CURRENT = v0 by itself
cd ~/bd_project/code/node4_search && uvicorn app:app --host 0.0.0.0 --port 8000 2>&1 | tee ~/bd_project/state/logs/serve.log
# node4
bash ~/bd_project/scripts/check.sh serve v0
```
Wait about 30 s after the NameNode starts for safe mode to turn OFF (`hdfs dfsadmin -safemode get`). After a reboot or ZeroTier restart, the MTU setting (RUNBOOK S07) is lost, so reapply it if bulk transfers stall.

---

## 1. Before the examiner arrives (Phase 0 done, serving, nothing of Phase 1)

Tick every line. If something fails: `bash scripts/doctor.sh`, then TROUBLESHOOTING.md.

- [ ] Daemons up: node2 `NameNode DataNode Worker`; node3 `DataNode Master Worker HistoryServer` (`jps`).
- [ ] Gates passed: `check.sh setup` and `network` (all nodes), `hdfs`, `spark`, `ingest0`, `index`, `export v0` (nodes 1/2/3 as in the RUNBOOK), and on **node4** `check.sh pulled v0` and `check.sh serve v0`.
- [ ] node4: the search service from S28 has been **running since before the demo** (terminal A open, do not restart it) and `curl -s localhost:8000/health` shows version 0, 1000 docs.
- [ ] HDFS has only `/serving/v0` (no `v1`), `/search/input/phase-1` is empty, `doc_changes` has only phase 0.
- [ ] Tags `phase0` exist on `docs` and `links` (S39).
- [ ] Browser tabs open: NameNode `http://node2:9870`, Spark master `http://172.23.10.141:8080`, history server `:18080`, search UI `http://node4:8000`.
- [ ] Baselines written down (run them now, before Phase 1):

**node4**: doc 94 is plain "Beard"
```bash
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool
```
**node3**: PageRank of doc 146
```bash
$SQL -e "SELECT doc_id, rank FROM iceberg_catalog.search.pagerank WHERE doc_id = '146'" 2>/dev/null
```
Baseline rank of doc 146: ____________________

- [ ] Do **not** run S41 or later before the examiner. If you rehearsed past S41, reset first (section 6).

---

## 2. Show Phase 0 is stored on the cluster (TA section 1, about 1 min)

**node2**
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|^Name:"
hdfs dfs -ls -R / | grep -i jsonl
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations
```
Expect:
- `Live datanodes (2)` with the node2 (172.23.194.191) and node3 (172.23.10.141) IPs, `Dead datanodes (0)`.
- `pages.jsonl` listed with non-zero size.
- fsck: `Status: HEALTHY`, `Live_repl=2` with both IPs on every block.

UI: `http://node2:9870` shows 2 DataNodes; Utilities → Browse the file system → `/search/input/phase-0`.

---

## 3. Live Phase 1 (one job after the other; check each before the next)

Say in chat after each job; the next member waits for it.

### 3.1 node1: ingest (RUNBOOK S41)
```bash
hdfs dfs -put -f data/phase-1/changes.json /search/input/phase-1/changes.jsonl
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
Expect: lines read 100, accepted 100 (50 DELETE), ADDED/UPDATED/DELETED **25 / 25 / 50**, docs 1000 → **975**, links kept 467, dangling dropped 7.

TA check (node1 or node3):
```bash
$SQL -e "SELECT snapshot_id, committed_at, operation FROM iceberg_catalog.search.docs.snapshots" 2>/dev/null
```
Expect at least 2 snapshots: Phase 0 (`append`) and Phase 1 (`overwrite`).

### 3.2 node3: incremental index (S44)
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
```
Expect: `Iceberg says 100 docs changed`, `mode: incremental`, docs 975, terms **36521**, postings **220622**.

TA check:
```bash
$SQL -e "SELECT term, doc_id FROM iceberg_catalog.search.postings WHERE doc_id = 'phase1-new-001' LIMIT 5" 2>/dev/null
```
Expect rows.

### 3.3 node3: PageRank (S45)
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1.log
```
Expect: N=975, 467 links, sum of ranks 1.000000, fingerprint **54ee091e74b5**.

TA check:
```bash
$SQL -e "SELECT COUNT(*) AS deleted_doc_ranks FROM iceberg_catalog.search.pagerank WHERE doc_id = '1'" 2>/dev/null
```
Expect **0**.

### 3.4 node3: export v1 (S46)
```bash
spark-submit code/node3_compute/export.py --version v1 --phase 1 --export-root hdfs://node2:9000/serving 2> state/logs/export-v1.log
```
Expect: 13 × `PASS`, `EXPORT v1: OK`, docs / postings / terms = 975 / 220622 / 36521.

TA check (node2 or node3):
```bash
hdfs dfs -ls /serving/v0/
hdfs dfs -ls /serving/v1/
```
Expect in each: `docs.parquet`, `index.parquet` (plain Parquet), and our `manifest.json` and `_READY`.

### 3.5 node4: pull v1, the service switches live (S48)
```bash
python code/node4_search/pull_export.py --version v1 --source hdfs://node2:9000/serving
```
Expect: `verified v1: 975 docs, 220622 postings, 13/13 checks passed`, `CURRENT: v0 -> v1`, `service: {'switched': True, 'version': 'v1'}`.

---

## 4. Search verification (TA section 3)

**node4**
```bash
curl -s http://localhost:8000/health
```
**node3**
```bash
hdfs dfs -test -e /serving/v1/_READY && echo "v1 ready"
```
Expect `"status":"ok"`, `"version":1`, `"docs":975`, and `v1 ready`.

### 4.1 Doc 94 updated in place
**node4**
```bash
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool
```
**node3**
```bash
$SQL -e "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id = '94'" 2>/dev/null
```
Expect: same `doc_id` 94, title now **"Beard (Phase 1)"** (it was "Beard" in the baseline).

### 4.2 Doc 146 (Cooking) gains rank
**node4**
```bash
curl -s "http://localhost:8000/search?q=cooking&limit=5" | python3 -m json.tool
```
**node3**
```bash
$SQL -e "SELECT doc_id, rank FROM iceberg_catalog.search.pagerank WHERE doc_id = '146'" 2>/dev/null
```
Expect: **higher than the baseline** from section 1 (new links from doc 98 "Black" and phase1-new-011).

### 4.3 Multi-word query
**node4**
```bash
curl -s "http://localhost:8000/search?q=plant+growth&limit=5"
```
Expect valid JSON, no 500.

---

## 5. Idempotency (TA section 4, last minute)

**node1**
```bash
$SQL -e "SELECT COUNT(*) FROM iceberg_catalog.search.docs" 2>/dev/null       # note it: 975
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
$SQL -e "SELECT COUNT(*) FROM iceberg_catalog.search.docs" 2>/dev/null       # same: 975
```
Expect the same count. The re-run prints `changes merged: 0` and the same snapshot id before and after.

---

## 6. Reset after a rehearsal (so Phase 1 is really live)

1. **node2**: `hdfs dfs -rm -r -skipTrash /warehouse/search /warehouse/check '/serving/*' /search/input/phase-1/changes.jsonl`, then `hdfs dfs -ls -R /warehouse /serving`.
2. **node4**: Ctrl-C the service, `rm -rf ~/bd_project/state/serving && mkdir -p ~/bd_project/state/serving`, start it again (S28).
3. Redo Phase 0: RUNBOOK **S29-S40** (upload, ingest, re-run, index, PageRank, export v0, pull v0, tag `phase0`, gates). Then redo the baselines in section 1.

Do not re-run S10 on any node: it would add `spark.jars` back (the Iceberg jar is in `$SPARK_HOME/jars`).

---

## 7. If a TA asks why the jobs are slow

The data is tiny (1000 pages), so the time is latency, not compute. Our four machines sit in different houses on a ZeroTier overlay, and some links are relayed (node2 to node3 is the slowest, with high ping and about 1.8 MB/s). Every Spark job and every Iceberg or HDFS metadata call crosses those links to the NameNode on node2. On a normal LAN the same jobs take well under a minute. The results (counts, fingerprints, 13/13 checks) are identical; only the wall-clock differs.

If a job is still running when time is short, keep talking through the log lines it has already printed, and never skip a job or fake its output.
