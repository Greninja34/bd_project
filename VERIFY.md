# VERIFY: demo day, the complete run sheet

Everything needed for tonight, tomorrow morning and the 10-minute TA demo is in this file. No other guide is required.

## Notation

| Mark | Meaning |
|---|---|
| **M1 / M2 / M3 / M4** | the member, always on the node shown next to them |
| **Step id** `A2.3` | Part A, group 2, step 3. Say the id in chat so everyone knows where you are |
| **▶ Run** | paste only this block, on the named node |
| **✅ Expect** | what the screen must show before you hand over |
| **➡ Then** | the handoff: the sentence to post in the team chat, and who goes next |
| **⏱** | a time budget (fill in from the rehearsal) |
| `[ ]` | a tick box for the check-lists |

| Member | Node | ZeroTier IP | Owns |
|---|---|---|---|
| **M1** | node1 | 172.23.59.75 | ingestion (Spark driver), SQL checks |
| **M2** | node2 | 172.23.194.191 | NameNode + DataNode + worker, Iceberg storage report/tags |
| **M3** | node3 | 172.23.10.141 | Spark master + worker + DataNode + history server, index / PageRank / export |
| **M4** | node4 | 172.23.204.87 | search service and UI (HDFS client only) |

**Opener: every member, first thing in every new terminal**
```bash
source ~/bd_project/venv/bin/activate && cd ~/bd_project
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"      # M1, M2, M3 only
```
`$SQL` is exactly what we used in all our earlier runs: it reads the same Iceberg tables in HDFS, but does not start executors on the cluster. Do **not** use `--master spark://...` for these queries: a cluster SQL session takes all 4 cores, so the next Spark job waits behind it, and each query is much slower over ZeroTier. If a TA asks for the cluster form, it is `spark-sql --master spark://172.23.10.141:7077 -e "..."` (same result).

**TA names → ours**

| TA guide | Ours |
|---|---|
| vm1 / vm2 / vm3 / vm4 | node1 / node2 / node3 / node4 |
| `iceberg_catalog.lake.pages` | `iceberg_catalog.search.docs` |
| `iceberg_catalog.lake.inverted_index` | `iceberg_catalog.search.postings` (`term`, `doc_id`, `tf`) |
| `iceberg_catalog.lake.pagerank` | `iceberg_catalog.search.pagerank` (`doc_id`, `rank`) |
| `--properties-file ~/spark-config/iceberg.conf` | not needed: the catalog is in `state/conf/spark/spark-defaults.conf` |
| `/serving/vN/` with `docs.parquet`, `index.parquet`, `_READY` | the same, plus `manifest.json` (`_READY` is written last) |
| `localhost:8000/search?q=&limit=` and `/health` | work (`/health` returns `"version":1`); `/api/search` and `/api/health` also exist |

`doc_id` is a string: always quote it (`'146'`). If anything fails: `bash scripts/doctor.sh` and paste the output in the team chat.

## Order of the day

| When | Part | Who |
|---|---|---|
| Tonight | **A** reset to Phase 0 and rebuild it, then shut down | M4, M2, M1, M3 |
| Tomorrow morning | **B** start the cluster, **C** check-list | all |
| Demo, 0:00 | **D** live Phase 1 and the TA checks | M2, M1, M3, M4 |
| After a rehearsal | **E** reset | Part A |

---

# PART A · Tonight: back to the Phase 0 state, then shut down

Starting point: Phase 1 applied, v1 served, all daemons up. Do **not** format HDFS.

### A1 · Reset

**A1.1 · M4 · node4 · stop the service**
▶ Run: Ctrl-C in terminal A (uvicorn).
➡ Then: "service stopped".

**A1.2 · M2 · node2 · remove everything from Phase 1 and the old exports**
```bash
hdfs dfs -rm -r -skipTrash /warehouse/search /warehouse/check '/serving/*' /search/input/phase-1/changes.jsonl
hdfs dfs -mkdir -p /serving /search/input/phase-1
hdfs dfs -ls -R /warehouse /serving /search/input
```
✅ Expect: `/warehouse` and `/serving` empty; `/search/input/phase-0/pages.jsonl` still present; `/search/input/phase-1` empty. A `No such file or directory` line for `/warehouse/check` is harmless (that table was already dropped); the other paths are still removed.
➡ Then: "lake reset". M4 next.

**A1.3 · M4 · node4 · empty the local serving copy and start the service again**
```bash
rm -rf ~/bd_project/state/serving && mkdir -p ~/bd_project/state/serving
cd ~/bd_project/code/node4_search
uvicorn app:app --host 0.0.0.0 --port 8000 2>&1 | tee ~/bd_project/state/logs/serve.log      # terminal A, stays open
```
Terminal B: `curl -s localhost:8000/health`
✅ Expect: `"status":"no-version"`.
➡ Then: "node4 ready". M1 next.

### A2 · Build Phase 0

**A2.1 · M1 · node1 · upload `pages.jsonl` and look at the replication**
```bash
hdfs dfs -put -f data/phase-0/pages.jsonl /search/input/phase-0/pages.jsonl
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations | grep -E "Live_repl|Average block replication"
```
✅ Expect: `Live_repl=2` with the node2 and node3 IPs; `Average block replication: 2.0`.

**A2.2 · M1 · node1 · ingest Phase 0**
```bash
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
✅ Expect: read 1040 · rejected 20 (`unparseable_json`) · duplicates superseded 20 · accepted 1000 · links kept 499 · ADDED/UPDATED/DELETED 1000/0/0 · docs 0 → 1000.

**A2.3 · M1 · node1 · run it again (idempotence)**
```bash
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
✅ Expect: changes merged 0, links table rewritten False, same snapshot id before and after.

**A2.4 · M1 and M2 · gate**
```bash
bash scripts/check.sh ingest0
```
✅ Expect: `GATE ingest0 PASSED` on node1 and node2.
➡ Then (M1): "Phase 0 ingested". M3 next.

**A2.5 · M3 · node3 · inverted index**
```bash
spark-submit code/node3_compute/build_index.py --phase 0 2> state/logs/index-p0.log
```
✅ Expect: `mode: full`, docs 1000, terms 37551, postings 233643.

**A2.6 · M3 · node3 · PageRank**
```bash
spark-submit code/node3_compute/pagerank.py --phase 0 2> state/logs/pagerank-p0.log
```
✅ Expect: N=1000, 499 links, sum of ranks 1.000000, fingerprint `60a6221350e7`.

**A2.7 · M3 · node3 · validate and export v0**
```bash
spark-submit code/node3_compute/export.py --version v0 --phase 0 --export-root hdfs://node2:9000/serving 2> state/logs/export-v0.log
hdfs dfs -ls -R hdfs://node2:9000/serving/v0
```
✅ Expect: 13 × `PASS`, `EXPORT v0: OK`, docs / postings / terms = 1000 / 233643 / 37551; listing shows `docs.parquet`, `index.parquet`, `manifest.json`, `_READY`.

**A2.8 · M3 and M2 · gates**
```bash
bash scripts/check.sh index
bash scripts/check.sh export v0
```
✅ Expect: both `PASSED` on node3 and node2.
➡ Then (M3): "v0 exported". M4 and M2 go **at the same time**.

**A2.9 · M4 · node4 · pull v0 and search it** (terminal B)
```bash
python code/node4_search/pull_export.py --version v0 --source hdfs://node2:9000/serving
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py april
python code/node4_search/q.py zzzqqq
```
✅ Expect: `verified v0: 1000 docs, 233643 postings, 13/13 checks passed` · `CURRENT: None -> v0` · `switched: True` · Bubonic plague first · Alan Turing first · April **5th** · `zzzqqq` 0 matches. Open `http://node4:8000`, search "bubonic plague".

**A2.10 · M2 · node2 · report and tag Phase 0** (at the same time as A2.9)
```bash
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs" 2> state/logs/sql.log
```
✅ Expect: `docs 1000 rows | 1 snapshots`, links 499, rejects 40, postings 233643; the `phase0 TAG` row on `docs`.

**A2.11 · M4 · node4 · gate**
```bash
bash scripts/check.sh pulled v0
bash scripts/check.sh serve v0
```
✅ Expect: both `PASSED`.
➡ Then (M4): "Phase 0 serving".

**A2.12 · M3 and M4 · write the baselines on paper** (needed in Part C)
- M4: `curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool` → doc 94 title is plain "Beard".
- M3: `$SQL -e "SELECT doc_id, rank FROM iceberg_catalog.search.pagerank WHERE doc_id = '146'" 2>/dev/null` → rank of doc 146: ____________

### A3 · Shut down cleanly (in this order; each waits for the previous one)

**A3.1 · M4 · node4**: Ctrl-C in terminal A. ➡ "node4 stopped".

**A3.2 · M3 · node3**
```bash
stop-history-server.sh
stop-worker.sh
stop-master.sh
hdfs --daemon stop datanode
```
➡ "node3 stopped".

**A3.3 · M2 · node2** (the NameNode last)
```bash
stop-worker.sh
hdfs --daemon stop datanode
hdfs --daemon stop namenode
```

**A3.4 · everyone · check**
```bash
bash scripts/check.sh down
jps
```
✅ Expect: `GATE down PASSED`; `jps` shows only `Jps`.
Do **not** delete `~/bd_project/state` and do **not** format HDFS.

---

# PART B · Tomorrow morning: start the cluster (about 5 min, no format)

**B1 · everyone · network first** (before any daemon)
```bash
sudo zerotier-cli info                          # ... ONLINE
sudo zerotier-cli listnetworks                  # 8286ac0e47d1ebb6 ... OK PRIVATE ... your 172.23.x.x
sudo zerotier-cli peers | grep LEAF             # DIRECT is best; RELAY works but is slow
for n in node1 node2 node3 node4; do ping -c2 -W3 $n >/dev/null && echo "$n ok" || echo "$n FAIL"; done
getent hosts $(hostname)                        # your own ZeroTier IP, never 127.x or fe80::
bash scripts/check.sh setup && bash scripts/check.sh network
```
✅ Expect: 4 × `ok`, both gates `PASSED` on all four nodes. (node3's ping may fail because WSL drops ICMP; trust `check.sh network`.)
If not: M3 checks WSL is running and `ip -4 -br a` shows `172.23.10.141`; M4 switches the VM adapter back to NAT if Bridged does not work on this Wi-Fi; a phone hotspot is the fallback.
➡ Then: "network OK" from each member.

**B2 · M2 · node2 · NameNode, DataNode, worker**
```bash
hdfs --daemon start namenode
sleep 10
hdfs --daemon start datanode
start-worker.sh spark://172.23.10.141:7077
hdfs dfsadmin -safemode get
```
✅ Expect: `Safe mode is OFF` (wait up to 30 s).
➡ Then: "NameNode up".

**B3 · M3 · node3 · DataNode, master, worker, history server**
```bash
hdfs --daemon start datanode
start-master.sh
start-worker.sh spark://172.23.10.141:7077
hdfs dfs -test -d /spark-logs && start-history-server.sh
jps
```
✅ Expect: `DataNode Master Worker HistoryServer`.
➡ Then: "daemons up".

**B4 · all · gates**
```bash
bash scripts/check.sh hdfs          # nodes 1-4
bash scripts/check.sh spark         # nodes 1-3
```
✅ Expect: `GATE hdfs PASSED` (2 live DataNodes) and `GATE spark PASSED` (2 ALIVE workers).
If bulk transfers stall: `ZT_IF=$(ip -o -4 addr show | awk -v ip=$MY_IP '$4 ~ "^"ip"/" {print $2}'); sudo ip link set dev $ZT_IF mtu 1400`.

**B5 · M4 · node4 · start the search service** (terminal A, then do not touch it)
```bash
cd ~/bd_project/code/node4_search
uvicorn app:app --host 0.0.0.0 --port 8000 2>&1 | tee ~/bd_project/state/logs/serve.log
```
✅ Expect: it loads `CURRENT` = v0 by itself. Terminal B: `curl -s localhost:8000/health` → `"version":0`, 1000 docs.
➡ Then: "node4 serving v0".

---

# PART C · Before the examiner arrives: check-list (nothing of Phase 1 yet)

Each block says who runs it. Tick every box.

**C1 · M2 · node2 · HDFS holds only Phase 0**
```bash
hdfs dfs -ls /serving                  # only v0
hdfs dfs -ls /serving/v0               # docs.parquet index.parquet manifest.json _READY
hdfs dfs -ls /search/input/phase-0     # pages.jsonl
hdfs dfs -ls /search/input/phase-1     # empty
```
- [ ] only `v0`; `phase-1` empty.

**C2 · M1 · node1 · the lake holds only Phase 0**
```bash
bash scripts/check.sh ingest0
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2>/dev/null
$SQL -e "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type" 2>/dev/null
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs" 2>/dev/null
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.links.refs" 2>/dev/null
```
- [ ] `GATE ingest0 PASSED`; docs **1000**; `doc_changes` has only phase 0; tag `phase0` on `docs` and `links`; no `phase1` tag.

**C3 · M3 · node3 · index and export**
```bash
bash scripts/check.sh index
bash scripts/check.sh export v0
```
- [ ] both `PASSED`.

**C4 · M4 · node4 · v0 served from memory**
```bash
bash scripts/check.sh pulled v0
bash scripts/check.sh serve v0
curl -s localhost:8000/health
ls ~/bd_project/state/serving          # v0 CURRENT (no v1)
python code/node4_search/q.py "bubonic plague"
```
- [ ] both gates `PASSED`; version 0, 1000 docs; terminal A still running the service (**never restart it**).

**C5 · projector machine · tabs**
- [ ] NameNode `http://node2:9870` · Spark master `http://172.23.10.141:8080` · history server `http://172.23.10.141:18080` · search UI `http://node4:8000`.

**C6 · M4 and M3 · baselines** (from A2.12; re-take them if the cluster was restarted)
- [ ] doc 94 is "Beard" (M4). Rank of doc 146 = ____________ (M3).

**C7 · everyone**
- [ ] **No Phase 1 command has been run.** If one was, do Part E.

---

# PART D · The live run (10 minutes), Phase 1 and the TA checks

Order is fixed: M2 → M1 → M3 → M4. Each member waits for the previous "➡ Then" line in chat. Fill the ⏱ column in the rehearsal.

| Step | Who | Does | ⏱ |
|---|---|---|---|
| D1 | M2 | Show Phase 0 on the cluster | ____ |
| D2 | M1 | Ingest Phase 1 | ____ |
| D3 | M3 | Incremental index | ____ |
| D4 | M3 | PageRank | ____ |
| D5 | M3 | Export v1 | ____ |
| D6 | M4 | Pull v1, search | ____ |
| D7 | M3 + M4 | TA search checks | ____ |
| D8 | M1 | Idempotency | ____ |

**D1 · M2 · node2 · Phase 0 is on the cluster (TA section 1)**
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|^Name:"
hdfs dfs -ls -R / | grep -i jsonl
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations
```
✅ Expect: `Live datanodes (2)` with 172.23.194.191 and 172.23.10.141; `pages.jsonl` non-zero; `Status: HEALTHY`; `Live_repl=2`. UI: `http://node2:9870` → Datanodes (2) and Utilities → Browse the file system → `/search/input/phase-0`.
➡ Then: "go for Phase 1".

**D2 · M1 · node1 · ingest Phase 1**
```bash
hdfs dfs -put -f data/phase-1/changes.json /search/input/phase-1/changes.jsonl
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
$SQL -e "SELECT snapshot_id, committed_at, operation FROM iceberg_catalog.search.docs.snapshots" 2>/dev/null
```
✅ Expect: lines read 100 · accepted 100 (50 DELETE) · ADDED/UPDATED/DELETED **25 / 25 / 50** · docs 1000 → **975** · links kept 467 · dangling dropped 7. TA check: at least 2 snapshots (Phase 0 `append`, Phase 1 `overwrite`).
➡ Then: "Phase 1 ingested".

**D3 · M3 · node3 · incremental index**
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
$SQL -e "SELECT term, doc_id FROM iceberg_catalog.search.postings WHERE doc_id = 'phase1-new-001' LIMIT 5" 2>/dev/null
```
✅ Expect: `Iceberg says 100 docs changed`, `mode: incremental`, docs 975, terms **36521**, postings **220622**. TA check: rows for `phase1-new-001`.

**D4 · M3 · node3 · PageRank**
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1.log
$SQL -e "SELECT COUNT(*) AS deleted_doc_ranks FROM iceberg_catalog.search.pagerank WHERE doc_id = '1'" 2>/dev/null
```
✅ Expect: N=975, 467 links, sum 1.000000, fingerprint **54ee091e74b5**. TA check: **0**.

**D5 · M3 · node3 · export v1**
```bash
spark-submit code/node3_compute/export.py --version v1 --phase 1 --export-root hdfs://node2:9000/serving 2> state/logs/export-v1.log
hdfs dfs -ls /serving/v0/
hdfs dfs -ls /serving/v1/
hdfs dfs -test -e /serving/v1/_READY && echo "v1 ready"
```
✅ Expect: 13 × `PASS`, `EXPORT v1: OK`, 975 / 220622 / 36521. TA check: each folder has `docs.parquet`, `index.parquet` (plus our `manifest.json`, `_READY`); `v1 ready`.
➡ Then: "v1 exported".

**D6 · M4 · node4 · pull v1 (the service switches live, never stops)** (terminal B)
```bash
python code/node4_search/pull_export.py --version v1 --source hdfs://node2:9000/serving
curl -s http://localhost:8000/health
```
✅ Expect: `verified v1: 975 docs, 220622 postings, 13/13 checks passed` · `CURRENT: v0 -> v1` · `switched: True` · health `"status":"ok"`, `"version":1`, `"docs":975`.

**D7 · TA search checks (M4 runs the curl, M3 the SQL)**

*D7.1 · doc 94 updated in place*
- M4: `curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool`
- M3: `$SQL -e "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id = '94'" 2>/dev/null`

✅ Expect: same `doc_id` 94, title **"Beard (Phase 1)"** (it was "Beard").

*D7.2 · doc 146 (Cooking) gains rank*
- M4: `curl -s "http://localhost:8000/search?q=cooking&limit=5" | python3 -m json.tool`
- M3: `$SQL -e "SELECT doc_id, rank FROM iceberg_catalog.search.pagerank WHERE doc_id = '146'" 2>/dev/null`

✅ Expect: **higher than the baseline** (new links from doc 98 "Black" and phase1-new-011).

*D7.3 · multi-word query*
- M4: `curl -s "http://localhost:8000/search?q=plant+growth&limit=5"`

✅ Expect: valid JSON, no 500.

**D8 · M1 · node1 · idempotency (last minute)**
```bash
$SQL -e "SELECT COUNT(*) FROM iceberg_catalog.search.docs" 2>/dev/null       # 975
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
$SQL -e "SELECT COUNT(*) FROM iceberg_catalog.search.docs" 2>/dev/null       # same: 975
```
✅ Expect: the same count; `changes merged: 0`; same snapshot id before and after.
➡ Then: "demo done".

If a job is still running when time is short, keep talking through the log lines it has already printed. Never skip a job or fake its output.

---

# PART E · Reset after a rehearsal

Run **A1** (M4, M2, M4), then **A2** (M1, M3, M4, M2), then redo the baselines (A2.12 and C6). Do not repeat any configuration step on any node: the Iceberg jar is already in `$SPARK_HOME/jars` and the configs are final.

# PART F · If a TA asks why the jobs are slow

The data is tiny (1000 pages), so the time is latency, not compute. Our four machines sit in different houses on a ZeroTier overlay and some links are relayed (node2 to node3 is the slowest, with high ping and about 1.8 MB/s). Every Spark job and every HDFS or Iceberg metadata call crosses those links to the NameNode on node2. On a normal LAN the same jobs take well under a minute. The results (counts, fingerprints, 13/13 checks) are identical; only the wall-clock differs.
