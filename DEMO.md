# Demo day: the steps, one after the other

## How the three files connect

| File | What it is | When |
|---|---|---|
| [RUNBOOK.md](RUNBOOK.md) | **Setup + first full run** (S01-S70): install configs, bring the cluster up, run both phases, run every demo once, with check gates | Days before. Do it fully once as a **rehearsal** |
| **DEMO.md** (this file) | **The live show** (D01-D15): the same commands, re-ordered to tell the story to the examiner, each with what to say | On the day |
| [VIVA.md](VIVA.md) | Why each step works + expected numbers + viva questions | Read before the day |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) + `scripts/doctor.sh` | When something fails | Any time |

Every D-step says which RUNBOOK step it comes from, so the expected numbers and the "if it fails" links are always one lookup away.

## How to read a D-step

- Each command block starts with a label: **`▶ node2 · terminal 1`** = go to that machine, open that terminal, paste only that block.
- **One block = one node.** If a step has two labels, the two members work **in turn** (the order written) or **at the same time** (it says "at the same time"). A node not named in the step does nothing.
- **say in chat:** = the sentence the next person waits for.
- **Point at:** = what the speaker shows on the screen and explains to the examiner.

## Before the examiner arrives (state of the cluster at the start of the demo)

Reset to "start over" (RUNBOOK, after S70) **or** a fresh cluster, then:
- RUNBOOK **S01-S28 done**: HDFS up (2 DataNodes), Spark up (master + 2 workers), `check.sh` gates 0-3 PASSED.
- **node4**: the search service from S28 is running (terminal A stays open) and the UI at `http://node4:8000` says "no version loaded".
- HDFS has the five folders; **no tables yet** (the lake is empty), `/search/exports` empty.
- Tabs open on the projector machine: Spark master `http://172.23.10.141:8080` · history server `:18080` · NameNode `http://node2:9870` · UI `http://node4:8000`.

**Every terminal you open** (all nodes):
```bash
source ~/bd_project/venv/bin/activate && cd ~/bd_project
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"   # nodes 1, 2, 3 (used for table queries)
```
Stuck? `bash scripts/doctor.sh`, paste the output in the chat, then TROUBLESHOOTING. Never re-format HDFS during the demo.

Spec items: **(1)** ingestion + dirty records + Iceberg, **(2)** inverted index, **(3)** PageRank, **(4)** fault tolerance, **(5)** BM25, **(6)** search + UI before/after changes.

---

## D01 · Show the cluster is real (RUNBOOK S22, S25) — node2 and node3, at the same time, then talk
Who speaks: Member 2 (HDFS part), Member 3 (Spark part). Goal: prove 2 DataNodes and 2 workers across machines.

**▶ node2 · terminal 1** (HDFS: Member 2)
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|^Name:"
```
Point at: `Live datanodes (2)` and two `Name:` lines with the **node2 and node3 ZeroTier IPs** (172.23.194.191 and 172.23.10.141). Say: node2 holds the NameNode (the directory) and a DataNode; node3 the second DataNode; replication is 2.

**▶ node3 · terminal 1** (Spark: Member 3)
```bash
curl -s http://172.23.10.141:8080/json/ | python3 -c "import sys,json; d=json.load(sys.stdin); print([(w['host'], w['state']) for w in d['workers']])"
```
Point at: two workers, both `ALIVE`, one on node2's IP and one on node3's IP (the master UI tab shows the same). Say: node3 is the master and a worker; node2 is the second worker; node1 and node4 run no daemons, they are only clients.

---

## D02 · Phase 0 ingestion, and idempotence (1) (RUNBOOK S29-S31) — node1 only
Who: Member 1. Everyone else watches.

**▶ node1 · terminal 1**
```bash
hdfs dfs -put -f data/phase-0/pages.jsonl /search/input/phase-0/pages.jsonl
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations | grep -E "Live_repl|Average block replication"
```
Point at: `Live_repl=2` with two IPs, `Average block replication: 2.0`. Say: the client asks the NameNode where to put each block and streams it to a DataNode, which forwards it to the second one.

**▶ node1 · terminal 1** (the ingest job: Spark driver on node1, tasks run on the workers of node2 and node3)
```bash
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
Point at the summary: 1040 lines read, **20 rejected** (`unparseable_json`), **20 duplicates superseded** (last copy wins), **1000 accepted**, **499 links**, ADDED 1000.

**▶ node1 · terminal 1** (look at what was rejected)
```bash
$SQL -e "SELECT phase, reason, count(*) FROM iceberg_catalog.search.rejects GROUP BY phase, reason" 2> state/logs/sql.log
```
**▶ node1 · terminal 1** (run the SAME phase again)
```bash
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
Point at: `changes merged: 0`, `links table rewritten: False`, and `docs snapshot before -> after` shows the **same id twice** = ingesting a phase twice changes nothing.
**say in chat:** "Phase 0 ingested" + the snapshot id.

---

## D03 · Index, PageRank, export v0 (2, 3) (RUNBOOK S33-S35) — node3 only
Who: Member 3. Waits for D02's "Phase 0 ingested".

**▶ node3 · terminal 1** (inverted index)
```bash
spark-submit code/node3_compute/build_index.py --phase 0 2> state/logs/index-p0.log
```
Point at: `mode: full`, **37551 terms, 233643 postings**. Say: a Python UDF on the workers tokenizes each doc; the `groupBy(term, doc_id)` is the shuffle.

**▶ node3 · terminal 1** (PageRank)
```bash
spark-submit code/node3_compute/pagerank.py --phase 0 2> state/logs/pagerank-p0.log
```
Point at: 10 iterations, `sum=1.000000` each time, fingerprint **`60a6221350e7`**. Say: dangling rank is spread evenly so the total stays 1.

**▶ node3 · terminal 1** (validate + export)
```bash
spark-submit code/node3_compute/export.py --version v0 --phase 0 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v0.log
```
Point at: **13 × PASS**, `EXPORT v0: OK`. Say: written to a temp folder, `_SUCCESS` last, then an atomic rename.
**say in chat:** "v0 exported".

---

## D04 · Search the original corpus (5, 6) (RUNBOOK S37-S38) — node4 only
Who: Member 4 (you). Waits for "v0 exported".

**▶ node4 · terminal B** (terminal A keeps the service running)
```bash
python code/node4_search/pull_export.py --version v0 --source hdfs://node2:9000/search/exports
```
Point at: `verified v0: 1000 docs, 233643 postings, 13/13 checks passed`, `CURRENT: None -> v0`, `switched: True`.

**▶ node4 · terminal B**
```bash
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py april
python code/node4_search/q.py ocean
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py zzzqqq
```
Point at: Bubonic plague first · April **5th** · Indian Ocean first · Alan Turing first · `zzzqqq` 0 matches (no word, no score).
**▶ node4 · browser** `http://node4:8000`: search "bubonic plague" and "april"; show the score bars (blue BM25, orange PageRank).
Say: `final = 0.7·BM25/max + 0.3·PageRank/max`; April has the best BM25 but no in-links, so it drops to 5th.

---

## D05 · Tag the Phase 0 state (1) (RUNBOOK S39) — node2 only
Who: Member 2. Must happen **before** D06.

**▶ node2 · terminal 1**
```bash
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs" 2> state/logs/sql.log
```
Point at: `docs 1000 rows | 1 snapshots`, the tag `phase0` pointing at the snapshot Member 1 posted. Say: a tag is a named, permanent pointer to one snapshot.
**say in chat:** "go for Phase 1".

---

## D06 · Phase 1 ingestion: the change set (1) (RUNBOOK S41-S42) — node1 only
Who: Member 1.

**▶ node1 · terminal 1**
```bash
hdfs dfs -put -f data/phase-1/changes.json /search/input/phase-1/changes.jsonl
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
Point at: 100 lines (50 are DELETE) · detected from Iceberg ADDED/UPDATED/DELETED = **25 / 25 / 50** · docs 1000 → **975** · **7 dangling links dropped**.

**▶ node1 · terminal 1** (what Iceberg recorded, then re-run)
```bash
$SQL -e "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type ORDER BY phase, change_type" 2> state/logs/sql.log
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
Point at: the re-run merges nothing (`changes merged: 0`, same snapshot).
**say in chat:** "Phase 1 ingested".

---

## D07 · Phase 1: incremental index, PageRank, export v1 (2, 3) (RUNBOOK S44-S46) — node3 only
Who: Member 3.

**▶ node3 · terminal 1**
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1.log
spark-submit code/node3_compute/export.py --version v1 --phase 1 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1.log
```
Point at: "Iceberg says 100 docs changed", **`mode: incremental`** (only those docs re-indexed, from the snapshot diff) · 975 docs · fingerprint **`54ee091e74b5`** · 13 × PASS, `EXPORT v1: OK`.
**say in chat:** "v1 exported".

---

## D08 · The search follows the change (6) (RUNBOOK S48) — node4 only
Who: Member 4 (you). Waits for "v1 exported".

**▶ node4 · terminal B** (still v0)
```bash
python code/node4_search/q.py "alan turing"
```
**▶ node4 · terminal B** (switch to v1)
```bash
python code/node4_search/pull_export.py --version v1 --source hdfs://node2:9000/search/exports
```
**▶ node4 · terminal B** (same queries, new answers)
```bash
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py april
python code/node4_search/q.py phase
```
Point at: `CURRENT: v0 -> v1` · Alan Turing found before, **gone** after (DELETED) · "Bubonic plague (**Phase 1**)" (UPDATED) · "phase" 20 → **69** matches (25 added pages) · April gone. Say: the service never stopped; the new version is fully loaded and verified, then swapped in under a lock.

---

## D09 · Time travel (1) (RUNBOOK S49) — node2 only
Who: Member 2.

**▶ node2 · terminal 1**
```bash
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase1" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase1" 2> state/logs/sql.log
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs VERSION AS OF 'phase0'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
```
Point at: **1000** (Phase 0 snapshot) vs **975** (now); history shows `append 1000` then `overwrite`. Say: Iceberg rewrites the changed files (copy-on-write) and keeps the old ones for the old snapshot.

---

## D10 · Spark fault tolerance (4) (RUNBOOK S51-S53) — node3 starts it, node2 kills, node3 counts
Who: Member 3 and Member 2, in this exact order.

**▶ node3 · terminal 1** (starts a PageRank that is slowed so it lasts ~2 minutes)
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 --slow-seconds 3 2> state/logs/pagerank-p1-slow.log
```
**say in chat:** "running". Member 3 watches the screen; when it prints `iter 2` or `iter 3`, tell node2 "kill now".

**▶ node2 · terminal 1** (kills the executor of that job on node2; at the same time node3's job is still running)
```bash
jps | grep CoarseGrainedExecutorBackend
kill -9 <that pid>
```
Point at: the job on node3 **keeps going** and finishes (the killed iteration takes much longer).

**▶ node3 · terminal 2** (after the job finished)
```bash
grep -oE "Lost executor|ExecutorLostFailure|No more replicas|FetchFailed" state/logs/pagerank-p1-slow.log | sort | uniq -c
```
Point at: `Lost executor`, `ExecutorLostFailure`, `FetchFailed` in the log, the master UI showing the killed executor, and **the same fingerprint `54ee091e74b5`** as D07. Say: Spark noticed the lost executor, re-ran the parent stages from the lineage and retried the tasks; nothing was checkpointed.

---

## D11 · A broken export never reaches the search (RUNBOOK S54-S55) — node3, then node4
Who: Member 3, then Member 4 (you).

**▶ node3 · terminal 1**
```bash
spark-submit code/node3_compute/export.py --version v1-broken --phase 1 --simulate-failure --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1-broken.log
echo "exit code $?"
```
Point at: 3 × `FAIL` (5 docs without PageRank, sum 0.998, 1 orphan posting), `VALIDATION FAILED - nothing published`, `exit code 1`.
**say in chat:** "broken export made".

**▶ node4 · terminal B**
```bash
python code/node4_search/pull_export.py --version v1-broken --source hdfs://node2:9000/search/exports; echo "exit code $?"
curl -s localhost:8000/api/health; echo
```
Point at: `REFUSED ... Still serving v1`, `exit code 1`, health still `"version":"v1"`. Say: the spec's "keep the previous search version available if export fails".

---

## D12 · Switch versions back and forth (RUNBOOK S56) — node4 only
Who: Member 4 (you).

**▶ node4 · terminal B**
```bash
curl -s -X POST localhost:8000/api/admin/switch/v0; echo
python code/node4_search/q.py "alan turing"
curl -s -X POST localhost:8000/api/admin/switch/v1; echo
python code/node4_search/q.py "alan turing"
```
Point at: Alan Turing is back on v0 and gone on v1; each version corresponds to one Iceberg snapshot (`curl -s localhost:8000/api/versions` lists them).

---

## D13 · A bad update, and recovery (1) (RUNBOOK S57-S60) — node2, node4, node2, node3, in this order
Who: Members 2, 4, 2, 3.

**▶ node2 · terminal 1** (someone deletes docs by mistake)
```bash
$SQL -e "DELETE FROM iceberg_catalog.search.docs WHERE title LIKE 'C%'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
```
Point at: **878** rows. **say in chat:** "bad delete done".

**▶ node4 · terminal B** (search is unaffected)
```bash
python code/node4_search/q.py "computer science"
```
Point at: still answers. Say: node4 serves exported Parquet, never Iceberg directly.

**▶ node2 · terminal 1** (roll back to the good snapshot)
```bash
GOOD=$($SQL -e "SELECT snapshot_id FROM iceberg_catalog.search.docs.refs WHERE name = 'phase1'" 2> state/logs/sql.log); echo "GOOD=$GOOD"
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.docs', $GOOD)" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history" 2> state/logs/sql.log
```
Point at: `GOOD=` a number · count back to **975** · the bad snapshot shows `is_current_ancestor = false`. Say: rollback moves the table pointer; no data file is copied or deleted.
**say in chat:** "rolled back".

**▶ node3 · terminal 1**
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
```
Point at: `mode: up-to-date` (the pipeline sees the state from before the mistake).

---

## D14 (optional) · A DataNode dies and returns (RUNBOOK S61-S64) — node3, node2, node3, node2
Who: Members 3 and 2, in this order.

**▶ node3 · terminal 1**
```bash
hdfs --daemon stop datanode
```
**say in chat:** "DataNode stopped".

**▶ node2 · terminal 1** (reads still work; then wait about 90 s)
```bash
hdfs dfs -cat /search/input/phase-0/pages.jsonl | wc -l
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication|Status"
```
Point at: 1040 lines (the other copy), `Live 1 / Dead 1`, under-replicated, `Status: HEALTHY`. Say: with only 2 DataNodes there is no third to re-replicate to, so it waits.
**say in chat:** "node3 dead, start it again".

**▶ node3 · terminal 1**
```bash
hdfs --daemon start datanode
```
**▶ node2 · terminal 1** (about 30 s later: the same `dfsadmin` and `fsck` commands as above)
Point at: `Live datanodes (2)`, `Under-replicated blocks: 0`, replication 2.0.

---

## D15 (optional) · Where the time and traffic go (RUNBOOK S65) — node3 only
Who: Member 3.

**▶ node3 · terminal 1**
```bash
ZT_IF=$(ip -o -4 addr show | awk -v ip=$MY_IP '$4 ~ "^"ip"/" {print $2}')
rx0=$(cat /sys/class/net/$ZT_IF/statistics/rx_bytes); tx0=$(cat /sys/class/net/$ZT_IF/statistics/tx_bytes)
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1-meter.log
echo "received $(( ($(cat /sys/class/net/$ZT_IF/statistics/rx_bytes) - rx0) / 1048576 )) MB, sent $(( ($(cat /sys/class/net/$ZT_IF/statistics/tx_bytes) - tx0) / 1048576 )) MB"
```
Then open `http://node3:18080` → the application → *Stages*. Say: the data is tiny; PageRank is latency-bound (10 iterations, a shuffle each), not bandwidth-bound.

---

Time: D01-D13 about 30 min. Cut D14-D15 first if short, never D02-D13.
After the demo: shut down in the RUNBOOK order (S67-S70: node4, then node3, then node2).
Each member must be able to explain their steps: [VIVA.md](VIVA.md) section 7. Likely first question to M4 (D04, D08, D11, D12): BM25 with k1 = 1.2, b = 0.75, the two bugs in the course snippet, the final-score normalisation, and why a version switch can't serve a broken export.
