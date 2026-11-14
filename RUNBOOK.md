# RUNBOOK

The one guide. Do the steps in order. Every step looks the same:

> **Step · WHO** — RUN (the command) — ✅ OK when (what you must see) — ❌ if not (link)

Stuck? Go to [TROUBLESHOOTING.md](TROUBLESHOOTING.md). Expected numbers and viva answers: [VIVA.md](VIVA.md).

## How it works (read once)

- The repo is cloned to `~/bd_project` on every VM. Any folder name works; the scripts find themselves.
- **One command per terminal:** `source ~/bd_project/venv/bin/activate`. The prompt then shows `(bd node3)`.
  You never set `ME=` or `ME_IP=`. The node detects itself from the IPs in `team.env`.
- Then use `bd`. `bd help` shows only the commands for **your** node, in the order of this guide.
- Everything we create lives in `~/bd_project/state/` (configs, HDFS data, logs, jars, serving folder). The course's own
  Hadoop/Spark config in `/opt` is not touched. Delete `state/` and run `bd setup` for a clean start.
- Each pipeline command prints an `EXPECTED:` line. Compare it with what you see above it.
- 📸 = take a screenshot (evidence for the report).

| Member | Node | Runs |
|---|---|---|
| M1 | node1 | ingestion (Spark driver, HDFS client). No daemons |
| M2 | node2 | HDFS NameNode + DataNode, Spark worker, the Iceberg tables |
| M3 | node3 | Spark master + worker, DataNode, history server. Drivers for index, PageRank, export |
| M4 | node4 | search service (FastAPI). HDFS client only, no Spark |

---

## A. Before the call (at home, needs internet)

### A1 · all
**RUN**
```bash
git clone https://github.com/Greninja34/bd_project.git ~/bd_project
```
node4 also needs: `sudo apt install -y git python3-venv`.
You need the course installs: Java 11, Hadoop 3.3.6 (`/opt/hadoop`), ZeroTier. Nodes 1–3 also need Spark 3.3.4 (`/opt/spark`) and Python 3.10.
✅ OK when `ls ~/bd_project/bd` works.
❌ if not: [wrong path](TROUBLESHOOTING.md#path-venv).

### A2 · node3 only (M3, WSL2)
node3 runs in **WSL2 Ubuntu**. ZeroTier must run **inside** that Ubuntu, not only on Windows.
Why: Spark executors on node2 must connect *into* the driver on node3 (port 40000). WSL2's NAT blocks that. With ZeroTier inside Ubuntu the traffic lands on Ubuntu's own interface and Windows never sees it.

1. `bash ~/bd_project/bd setup` once. It writes `/etc/wsl.conf` (`systemd=true`, `generateHosts=false`) and tells you what is still missing.
2. In **Windows PowerShell**: `wsl --shutdown`. Open Ubuntu again.
3. In Ubuntu: `curl -s https://install.zerotier.com | sudo bash`, then `sudo zerotier-cli join <network id>`.
4. In ZeroTier Central: authorize the **new** member, set its IP by hand to `172.23.10.141`, delete the **old Windows** member of node3.
5. Keep one Ubuntu terminal open the whole session. WSL stops itself when the last terminal closes.

✅ OK when `ip -4 -br a` inside Ubuntu shows `172.23.10.141`.
❌ if not: [node3 on WSL2](TROUBLESHOOTING.md#wsl2).

### A3 · all
The IPs are already in `team.env`. If one changes, **one** person edits `team.env`, pushes, and everyone runs `git pull` and `bd setup`.
**RUN**
```bash
ip -4 -br a            # your ZeroTier IP from team.env must be listed
sudo zerotier-cli peers | grep LEAF
```
✅ OK when your IP is listed and the peers say `DIRECT` (`RELAY` works but is slow).
❌ if not: [ZeroTier IP not on this machine](TROUBLESHOOTING.md#zt-ip) · [ping fails](TROUBLESHOOTING.md#ping) · [duplicate node IDs](TROUBLESHOOTING.md#dup-id).

### A4 · all
**RUN**
```bash
bash ~/bd_project/bd setup
```
It checks the prerequisites (OK / MISSING + the fix), makes the `venv`, gets the Iceberg jar, writes the Hadoop/Spark config, fixes `/etc/hosts`, opens ufw for the other 3 nodes. Run it again any time: it is safe.
✅ OK when it ends with `setup finished`.
❌ if not: it prints the exact fix for every FAIL. Run it again after. [sudo problems](TROUBLESHOOTING.md#sudo-host).

---

## B. Call 1: bring the cluster up

### B1 · all
**RUN**
```bash
source ~/bd_project/venv/bin/activate
bd doctor
```
✅ OK when no `FAIL` in the sections *This machine*, */etc/hosts*, *Reach the other nodes*. (Daemon ports are not up yet: `WARN` is fine.)
Write down your ZeroTier node ID line. All 4 must be different.
❌ if not: [TROUBLESHOOTING](TROUBLESHOOTING.md) by the message.

### B2 · all 4 at the same time
**RUN**
```bash
bd netcheck
```
Start it on all four nodes within a minute of each other. It opens test listeners, tests every port and sends 20 MB to each node.
✅ OK when every node ends with `ALL OK` and bulk speeds above 1 MB/s. 📸
**Do not go on until all 4 are ALL OK.** This is the test that finds firewall, WSL2 and MTU problems before HDFS or Spark hide them.
❌ if not: [ufw](TROUBLESHOOTING.md#ufw) · [WSL2](TROUBLESHOOTING.md#wsl2) · [bulk stalls / MTU](TROUBLESHOOTING.md#mtu).

### B3 · node2 (M2), once
**RUN**
```bash
bd hdfs-format
```
Answer `yes`. It formats HDFS, starts the NameNode and creates `/search/input/phase-0`, `/search/input/phase-1`, `/search/exports`, `/warehouse`, `/spark-logs`.
✅ OK when you see `formatted`, `NameNode listening on 172.23.194.191:9000` and the folder listing.
❌ if not: [clusterID](TROUBLESHOOTING.md#clusterid) · [port in use / old daemons](TROUBLESHOOTING.md#port-in-use).
(It refuses to run twice: formatting again would erase HDFS.)

### B4 · node2 (M2) and node3 (M3)
**RUN** (each on their own node, any order)
```bash
bd start
```
node2 starts NameNode, DataNode, Spark worker. node3 starts DataNode, Spark master, worker, history server. It also stops the course's old daemons (YARN etc.) and reads the new logs for known errors.
✅ OK when each line says `up` and the log check says `no known errors`.
❌ if not: [clusterID](TROUBLESHOOTING.md#clusterid) · [port in use](TROUBLESHOOTING.md#port-in-use) · [old daemons](TROUBLESHOOTING.md#old-daemons).

### B5 · any node
**RUN**
```bash
bd status
```
It waits up to 3 minutes.
✅ OK when: `2 live DataNodes` (the node2 and node3 IPs), `2 ALIVE Spark workers`, `safe mode is OFF`, `/spark-logs exists`, `Cluster is healthy`. 📸 also `http://node3:8080` (2 workers) and `http://node2:9870` → Datanodes (2 in service).
❌ if not: [DataNode shows 127.x](TROUBLESHOOTING.md#dn-ip) · [safe mode](TROUBLESHOOTING.md#safemode) · [only one worker](TROUBLESHOOTING.md#one-worker).

### B6 · node1 (M1) first, then node3 (M3)
**RUN**
```bash
bd check
```
Runs a small Spark job from this node as the driver.
✅ OK when `task ran on node2 ... python 3.10`, `task ran on node3 ... python 3.10`, `workers used: node2, node3 -> OK`. The table `check.hello` has 1 row after node1 and 2 rows (`node1`, `node3`) after node3: two drivers, one Iceberg table on HDFS. 📸
Then M3 cleans up: `bd check --drop`.
❌ if not: [ONLY ONE WORKER](TROUBLESHOOTING.md#one-worker) · [Initial job has not accepted any resources](TROUBLESHOOTING.md#no-resources) · [Python versions](TROUBLESHOOTING.md#py-version).

### B7 · node4 (M4)
**RUN**
```bash
bd serve
```
✅ OK when `{"status":"no-version", ... "last_error":null}`. Open `http://node4:8000`: badge says *no version loaded*. Test HDFS from here: `hdfs dfs -ls /search` lists `exports` and `input`.
❌ if not: [venv or dependencies](TROUBLESHOOTING.md#path-venv) · [connection refused :9000](TROUBLESHOOTING.md#refused-9000).

If the call ends here: `bd stop` on every node (node4 stops the service). Next time: `bd start`, `bd status`. No format.

---

## C. Call 2, Phase 0

### C1 · node1 (M1)
**RUN**
```bash
bd ingest 0
```
Uploads `pages.jsonl` to HDFS, shows its block replication, then runs the ingestion job.
✅ OK when (📸 the fsck lines and the summary):
`Live_repl=2` with the node2 and node3 IPs · lines read 1040 · rejected 20 (`unparseable_json`) · duplicates superseded 20 · accepted 1000 (20 without `links`) · changes merged 1000 · ADDED/UPDATED/DELETED 1000/0/0 · links kept 499 · dangling 0.
Post the `docs snapshot ... after` id in the group chat: it is **the Phase 0 snapshot**.
❌ if not: [Initial job has not accepted any resources](TROUBLESHOOTING.md#no-resources) · [ClassNotFound iceberg](TROUBLESHOOTING.md#iceberg-class) · [Permission denied](TROUBLESHOOTING.md#permission).

### C2 · node1 (M1): look, then run it again
**RUN**
```bash
bd sql "SELECT count(*) FROM iceberg_catalog.search.docs"
bd sql "SELECT phase, reason, count(*) FROM iceberg_catalog.search.rejects GROUP BY phase, reason"
bd sql "SELECT line_no, doc_id, reason, detail, substr(raw,1,60) FROM iceberg_catalog.search.rejects ORDER BY line_no LIMIT 25"
bd ingest 0
```
✅ OK when 1000 · `unparseable_json 20` and `duplicate_superseded 20` 📸 · on the re-run `unchanged 1000`, `changes merged 0`, and the docs snapshot id is the **same** before and after 📸 (idempotent). Tell M3: **"Phase 0 ingested"**.
❌ if not: [Table or view not found](TROUBLESHOOTING.md#table-not-found).

### C3 · node3 (M3)
**RUN**
```bash
bd index 0
bd pagerank 0
bd export v0
```
✅ OK when 📸:
- index: `mode: full`, docs 1000, terms 37551, postings 233643, avg/max length 461.0 / 13903
- PageRank: `N=1000 docs, 499 links`, sum 1.000000, max/min 0.001752 / 0.000264, **fingerprint `60a6221350e7`**
- export: 13 × `PASS`, `EXPORT v0: OK`

Tell M4: **"v0 exported"**.
❌ if not: [stale export](TROUBLESHOOTING.md#stale-export) · [already exists](TROUBLESHOOTING.md#exists) · [no resources](TROUBLESHOOTING.md#no-resources).

### C4 · node4 (M4)
**RUN**
```bash
bd pull v0
bd q "bubonic plague"; bd q "alan turing"; bd q april; bd q ocean; bd q "computer science"; bd q zzzqqq
```
✅ OK when `verified v0: 1000 docs, 233643 postings, 13/13 checks passed` and `CURRENT: None -> v0` 📸. Queries:

| query | v0 top results |
|---|---|
| bubonic plague | Bubonic plague (100) 1.000 · Flea (3523) 0.622 · London (460) 0.495 |
| alan turing | Alan Turing (13) 0.907 · Mathematics (467) · List of mathematicians (3433) |
| april | March (468) 0.870 · May (469) 0.807 · Time limit (828) · … April (1) is 5th |
| ocean | Indian Ocean (1978) 1.000 · Arctic Ocean (1980) · Pacific Ocean (1975) |
| computer science | Computer science (110) 1.000 · 1 (number) (571) · Computer (112) |
| zzzqqq | 0 matches |

📸 the UI for two queries (score bars visible: blue BM25, orange PageRank). Why April is 5th: [VIVA.md](VIVA.md#why-april-is-5th).
❌ if not: [venv or dependencies](TROUBLESHOOTING.md#path-venv) · [connection refused / minReplication](TROUBLESHOOTING.md#minreplication).

### C5 · node2 (M2): do this BEFORE Phase 1 is ingested
**RUN**
```bash
bd report
hdfs dfs -ls -R /warehouse/search/docs
hdfs fsck /warehouse -files -blocks | grep -E "Total files|Average block replication|Under-replicated"
bd tag phase0
```
✅ OK when 📸: `docs 1000 rows | 1 snapshots`, links 499, rejects 40, doc_stats 1000, postings 233643, pagerank 1000 · `Average block replication: 2.0`, `Under-replicated blocks: 0` · tag `phase0  TAG  <the Phase 0 snapshot id>`.
(`data/` = Parquet rows, `metadata/` = JSON + Avro. `version-hint.text` says which `vN` is current.)

---

## D. Call 2, Phase 1

### D1 · node2 (M2) says "go", then node1 (M1)
**RUN** (node1)
```bash
bd ingest 1
bd sql "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type ORDER BY phase, change_type"
bd sql "SELECT doc_id, reason, detail FROM iceberg_catalog.search.rejects WHERE phase = 1"
bd ingest 1
```
(The file `changes.json` is stored in HDFS as `changes.jsonl`.)
✅ OK when 📸: lines read 100 · accepted 100 (50 DELETE) · ADDED/UPDATED/DELETED 25/25/50 · docs 1000 → 975 · links kept 467, dangling dropped 7 · 7 `dangling_link_dropped` rows · on the re-run `unchanged 50`, `changes merged 0`, same snapshot. Tell M3: **"Phase 1 ingested"**.

### D2 · node3 (M3)
**RUN**
```bash
bd index 1
bd index 1
bd pagerank 1
bd export v1
```
✅ OK when 📸:
- first index: `Iceberg says 100 docs changed ... {'UPDATED': 25, 'DELETED': 50, 'ADDED': 25}`, `mode: incremental`, docs 975, terms 36521, postings 220622
- second index: `mode: up-to-date`
- PageRank: `N=975 docs, 467 links`, max/min 14.08, tied at minimum 548, **fingerprint `54ee091e74b5`**
- export: 13 × `PASS`, `docs / postings / terms: 975 / 220622 / 36521`

Tell M4: **"v1 exported"**.

### D3 · node4 (M4)
**RUN**
```bash
bd q april; bd q "alan turing"            # still v0
bd pull v1
bd q "bubonic plague"; bd q "alan turing"; bd q april; bd q phase; bd q "computer science"
```
✅ OK when `CURRENT: v0 -> v1`, `switched: True`, 975 docs 📸, and:

| query | v1 |
|---|---|
| bubonic plague | **Bubonic plague (Phase 1)** (100), the updated doc |
| alan turing | Alan Turing (13) is **gone** (deleted); top is List of mathematicians (3433) |
| april | 66 matches (was 71); April (1) is gone |
| phase | 69 matches (was 20): the 25 added and 25 updated pages |
| computer science | 174 matches; "Computer science (Phase 1)" (110) |

### D4 · node2 (M2)
**RUN**
```bash
bd tag phase1
bd report
bd sql "SELECT count(*) FROM iceberg_catalog.search.docs VERSION AS OF 'phase0'"
bd sql "SELECT count(*) FROM iceberg_catalog.search.docs"
bd sql "SELECT doc_id, title FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' WHERE doc_id IN ('1','100')"
bd sql "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id IN ('1','100')"
bd sql "SELECT count(*) AS deleted_in_phase1 FROM (SELECT doc_id FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' EXCEPT SELECT doc_id FROM iceberg_catalog.search.docs)"
```
✅ OK when 📸: `docs 975 rows | 2 snapshots` (history: `append 1000`, then `overwrite added 975, deleted 1000`) · 1000 vs 975 · Phase 0 has `1 April` and `100 Bubonic plague`, now only `100 Bubonic plague (Phase 1)` · `deleted_in_phase1 = 50`.
Why "delete 1000, add 975" for a 100-row change? Copy-on-write: the data files (4 Parquet files here) are rewritten. The old files stay because the `phase0` snapshot points at it.

---

## E. Demos

### E1 · Kill an executor in the middle of PageRank · node3 (M3) + node2 (M2)
**RUN** node3, terminal 1:
```bash
bd pagerank 1 --slow 3
```
**RUN** node2, when node3's screen shows `iter 2` or `iter 3`:
```bash
jps | grep CoarseGrainedExecutorBackend
kill -9 <that pid>
```
**RUN** node3, terminal 2, after it finished:
```bash
grep -oE "Lost executor|ExecutorLostFailure|No more replicas|FetchFailed" ~/bd_project/state/logs/pagerank-p1-slow.log | sort | uniq -c
```
✅ OK when 📸 (job output, the grep, `http://node3:8080` or `:18080`): the killed iteration takes much longer, then the others are normal · the log shows `Lost executor`, `ExecutorLostFailure`, `FetchFailed` · `sum of ranks: 1.000000` and **fingerprint `54ee091e74b5`** (same as the clean run). Spark recomputed the lost partitions from lineage.
Stronger variant: M2 runs `bd stop` on node2 during the job (the whole worker goes away); the job finishes on node3 alone, same fingerprint. Then `bd start` on node2.
❌ if the fingerprint differs: tell the team before the demo. [TROUBLESHOOTING](TROUBLESHOOTING.md).

### E2 · A broken export is refused · node3 (M3), then node4 (M4)
**RUN** node3:
```bash
bd export v1-broken --phase 1 --simulate-failure
```
**RUN** node4:
```bash
bd pull v1-broken; echo "exit code $?"; bd health
```
✅ OK when 📸: `FAIL every doc has a PageRank: 5 missing`, `FAIL PageRank sums to 1: 0.998...`, `FAIL every posting points at an exported doc: 1 orphan postings`, `VALIDATION FAILED - nothing published`, `exit code 1`; HDFS has `_failed_v1-broken_<time>` but no `v1-broken` · node4: `REFUSED: ... _SUCCESS not found ... Still serving v1.`, `exit code 1`, health still `"version":"v1"`.

### E3 · Switch v0 ↔ v1 · node4 (M4)
**RUN**
```bash
bd switch v0; bd q "alan turing"
bd switch v1; bd q "alan turing"
curl -s localhost:8000/api/versions | python3 -m json.tool
```
(Or the UI: *Versions on this node* → *serve this*.)
✅ OK when Alan Turing is back on v0 and gone on v1; `/api/versions` lists v0 and v1, valid, with their Iceberg snapshot ids 📸.

### E4 · A bad delete, and rollback · node2 (M2), with M3 and M4
**RUN** node2:
```bash
bd sql "DELETE FROM iceberg_catalog.search.docs WHERE title LIKE 'C%'"
bd sql "SELECT count(*) FROM iceberg_catalog.search.docs"
bd sql "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history"
```
✅ 878 rows and a 3rd snapshot. Search on node4 is unaffected (`bd q "computer science"` still works): it serves exported Parquet, never Iceberg.
**RUN** node2:
```bash
GOOD=$(bd sql "SELECT snapshot_id FROM iceberg_catalog.search.docs.refs WHERE name = 'phase1'"); echo "GOOD=$GOOD"
bd sql "CALL iceberg_catalog.system.rollback_to_snapshot('search.docs', $GOOD)"
bd sql "SELECT count(*) FROM iceberg_catalog.search.docs"
bd sql "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history"
```
✅ OK when `GOOD=` is a number · the procedure prints `<bad id>  <good id>` · count back to **975** · history has a 4th row with the good id; the bad snapshot has `is_current_ancestor = false` 📸.
**RUN** node3: `bd index 1` → `mode: up-to-date` (the pipeline sees the state from before the mistake).
Time travel reads an old snapshot for one query. Rollback moves the "current" pointer. Neither copies or deletes data files.

### E5 · A DataNode dies · node3 (M3) + node2 (M2)
**RUN** node3: `bd datanode stop`
**RUN** node2:
```bash
hdfs dfs -cat /search/input/phase-0/pages.jsonl | wc -l
bd sql "SELECT count(*) FROM iceberg_catalog.search.docs"
# about 90 seconds later:
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication|Status"
```
✅ OK when 📸: 1040 and 975 (reads still work from the other copy) · later `Live datanodes (1)`, `Dead datanodes (1)`, `Under-replicated blocks: <n> (100 %)`, `Status: HEALTHY` · `http://node2:9870` → Datanodes shows node3 dead.
**RUN** node3: `bd datanode start`. About 30 s later on node2 the same two commands.
✅ `Live datanodes (2)`, `Under-replicated blocks: 0`, `Average block replication: 2.0`.
(Under-replicated, not re-replicated: with 2 DataNodes and one down there is no third node to copy to. When node3 returns, its block report fixes the counts.)

### E6 · Where the time and traffic go · node3 (M3)
**RUN**
```bash
bd meter bd pagerank 1
```
Then open `http://node3:18080` → the application → *Stages*: sort by *Duration*, look at *Shuffle Read/Write* 📸.
✅ OK when you see the `[netmeter]` line (MB sent/received). What to say: the data is tiny, bandwidth is never the problem; PageRank is latency-bound (10 iterations, each a few small jobs with a shuffle); the index is CPU-bound (regex over 10 MB of wiki text). That is why we use 8 shuffle partitions, not 200.

---

## F. Stop, restart, start over

| Want | Run |
|---|---|
| Stop for today | node4 `bd stop` (service) · node3 `bd stop` · node2 `bd stop` |
| Start another day (no format!) | node2 and node3 `bd start` · any node `bd status` · node4 `bd serve` (it loads the last version) |
| IPs changed | edit `team.env`, push, everyone `git pull` and `bd setup`, then `bd stop` and `bd start` |
| Rehearse again from zero | node4 `bd reset-serving` · node2 `bd reset-lake` · then C1 again |
| Delete everything of ours on a node | `bd stop`; `rm -rf ~/bd_project/state`; `bd setup` (HDFS must then be formatted again: B3) |

## Evidence checklist (📸)

| Member | Screenshots |
|---|---|
| M1 | `fsck` replication of pages.jsonl · Phase 0 summary · rejects query · re-run (same snapshot) · Phase 1 summary · doc_changes query · Phase 1 re-run |
| M2 | `bd status` + NameNode UI (2 DataNodes) · `bd report` after Phase 0 and after Phase 1 · time-travel queries · bad delete + rollback history · DataNode dead and back |
| M3 | `bd netcheck` ALL OK · master UI with 2 workers · index (full, then incremental) · PageRank Phase 0 + 1 · export (13 PASS) · executor kill (output, grep, UI) · broken export · history server stages · `bd meter` line |
| M4 | UI with v0 results · `bd pull v0` · `bd pull v1` and the same queries · REFUSED pull · version switch |

Logs are in `~/bd_project/state/logs/`: `ingest-p0.log`, `index-p1.log`, `pagerank-p1-slow.log`, `export-v0.log`, ... Save them. `bd logs` shows the newest with known errors.
