# ROLLBACK_TEMP: go back to "Phase 0 finished" on all four nodes (temporary sheet)

Goal: undo Phase 1 only. Keep everything from Phase 0: ingest, index, PageRank, the v0 export, the HDFS setup.
Iceberg rollback only moves a table's "current" pointer back. No data file is deleted, so it is safe and can be undone.
**Not tested on this cluster.** If any gate in step R6 fails, stop and use the full reset instead (VERIFY.md A1 then A2).

Rules: do NOT format HDFS, do NOT touch configs, do NOT re-run RUNBOOK S10. Say the step id in chat, and wait for the
previous "-> Then" line before you start. Order: M4, M2, M1, M3, M4.

Opener in every new terminal:
```bash
source ~/bd_project/venv/bin/activate && cd ~/bd_project
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"      # M1, M2, M3 only
```

| Member | Node | IP |
|---|---|---|
| M1 | node1 | 172.23.59.75 |
| M2 | node2 | 172.23.194.191 |
| M3 | node3 | 172.23.10.141 |
| M4 | node4 | 172.23.204.87 |

---

## R0 · everyone · health first (before touching anything)

**M3 · node3**
```bash
ip -4 -br a | grep 172.23        # 172.23.10.141
jps                              # DataNode Master Worker HistoryServer
```
**M2 · node2**
```bash
jps                              # NameNode DataNode Worker
bash scripts/check.sh hdfs       # 2 live DataNodes, GATE hdfs PASSED
bash scripts/check.sh spark      # 2 workers, GATE spark PASSED
```
**Nobody has a Spark job or spark-sql session running** (`jps` shows no `SparkSubmit` on node1, node2 or node3).
-> M2 says: "cluster healthy".

## R1 · M4 · node4 · stop the service, put the local copy back to v0

```bash
cp -a ~/bd_project/state/serving ~/serving_backup_$(date +%H%M)      # safety copy first
pkill -f "uvicorn app:app"
rm -rf ~/bd_project/state/serving/v1
printf v0 > ~/bd_project/state/serving/CURRENT
ls ~/bd_project/state/serving                                          # CURRENT  v0
```
Do NOT start the service yet (it starts in R7).
-> M4 says: "node4 stopped".

## R2 · M2 · node2 · find the Phase 0 snapshot of every table (read only, changes nothing)

```bash
for T in docs links doc_changes rejects doc_stats postings pagerank; do
  echo "== $T"
  $SQL -e "SELECT snapshot_id, committed_at, operation FROM iceberg_catalog.search.$T.snapshots ORDER BY committed_at" 2>/dev/null
done
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs" 2>/dev/null
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.links.refs" 2>/dev/null
```
Pick the snapshot to go back to, per table:

| Table | Which snapshot |
|---|---|
| `docs`, `links` | the one tagged `phase0` (if there is no tag: the **first** snapshot, operation `append`) |
| `doc_changes`, `rejects`, `doc_stats`, `postings`, `pagerank` | the **last snapshot committed before the Phase 1 ingest**, i.e. before the `overwrite` snapshot on `docs` |

Sanity rule: `docs` has the Phase 0 `append` and the Phase 1 `overwrite`. Every other table's chosen snapshot must have
`committed_at` **earlier** than that `overwrite`. If a table has only one snapshot, it is the Phase 0 one.
Write the 7 ids in chat (table = id). Two members read them back to each other before the next step.
-> M2 says: "ids confirmed".

## R3 · M2 · node2 · roll the Iceberg tables back

Replace each `<ID_...>` with the id from R2. One table at a time.
```bash
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.docs',        <ID_docs>)"        2>/dev/null
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.links',       <ID_links>)"       2>/dev/null
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.doc_changes', <ID_doc_changes>)" 2>/dev/null
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.rejects',     <ID_rejects>)"     2>/dev/null
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.doc_stats',   <ID_doc_stats>)"   2>/dev/null
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.postings',    <ID_postings>)"    2>/dev/null
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.pagerank',    <ID_pagerank>)"    2>/dev/null
```
Check right away:
```bash
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2>/dev/null                    # 1000
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.links" 2>/dev/null                   # 499
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.postings" 2>/dev/null                # 233643
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.pagerank" 2>/dev/null                # 1000
$SQL -e "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type" 2>/dev/null   # phase 0 only
```
Any number is different: stop, post the output, do not continue.
Snapshot tags: there must be **no** `phase1` tag (`...docs.refs`). If one exists: `ALTER TABLE iceberg_catalog.search.docs DROP TAG phase1` (and on `links`).
-> M2 says: "tables rolled back".

## R4 · M2 · node2 · remove the Phase 1 files from HDFS

```bash
hdfs dfs -rm -r -skipTrash /serving/v1
hdfs dfs -rm -skipTrash /search/input/phase-1/changes.jsonl
hdfs dfs -ls -R /serving /search/input
```
Expect: `/serving/v0` with `docs.parquet index.parquet manifest.json _READY`; `phase-0/pages.jsonl` present; `phase-1` empty.
Leave `/warehouse` and `/spark-logs` alone.
-> M2 says: "Phase 1 removed".

## R5 · Spark (M1, M2, M3): nothing to roll back, just make sure it is clean

Spark keeps no data of its own. All results live in Iceberg and HDFS, which R3 and R4 already fixed.
```bash
jps | grep -E "SparkSubmit|CoarseGrainedExecutorBackend"     # nothing (no leftover job)
bash scripts/check.sh spark                                  # node1-3: GATE spark PASSED (2 workers)
```
If a leftover job shows: `kill <pid>` from `jps`.
Do NOT restart the master or workers unless `check.sh spark` fails (then X1.1 in VERIFY.md).
Old Phase 1 logs in `state/logs/*-p1.log` and `/spark-logs` can stay.

## R6 · everyone · gates (each member on their own node)

**M1 · node1**
```bash
bash scripts/check.sh ingest0                  # GATE ingest0 PASSED
```
**M2 · node2**
```bash
bash scripts/check.sh ingest0                  # PASSED
bash scripts/check.sh export v0                # PASSED
hdfs dfs -ls /serving                          # only v0
hdfs fsck /serving /search -files -blocks | grep -E "Status|Average block replication"   # HEALTHY, 2.0
```
**M3 · node3**
```bash
bash scripts/check.sh index                    # PASSED
bash scripts/check.sh export v0                # PASSED
```
All gates pass: go on. Any gate fails: **stop, paste the output with the step id, and switch to VERIFY.md A1 then A2.**
-> Each member says: "<node> gates OK".

## R7 · M4 · node4 · start the service on v0 (terminal A, then do not touch it)

```bash
cd ~/bd_project/code/node4_search
uvicorn app:app --host 0.0.0.0 --port 8000 2>&1 | tee ~/bd_project/state/logs/serve.log
```
Terminal B:
```bash
curl -s localhost:8000/health                        # "version":0, 1000 docs
bash scripts/check.sh pulled v0
bash scripts/check.sh serve v0
python code/node4_search/q.py "bubonic plague"       # Bubonic plague first
python code/node4_search/q.py "alan turing"          # Alan Turing first
python code/node4_search/q.py april                  # April 5th
python code/node4_search/q.py zzzqqq                 # 0 matches
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool    # doc 94 title plain "Beard"
```
-> M4 says: "node4 serving v0".

## R8 · continue

Now the state is the same as after VERIFY.md A2.12. Go to **Part C** (check-list), then **Part D** (live run).
Take the baselines again: M4 doc 94 = "Beard", M3 rank of doc 146.

---

## If something goes wrong
- Wrong snapshot id picked: roll that table forward/back again with the same `CALL` using the right id (nothing was deleted).
- Any R6 gate fails or counts do not match: full reset, VERIFY.md A1 then A2.
- Anything else: `bash scripts/doctor.sh`, paste the whole output in chat with the step id.
