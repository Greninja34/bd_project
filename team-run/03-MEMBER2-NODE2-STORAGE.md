# 03: Member 2, Node 2 (HDFS NameNode + DataNode, Spark worker, the lakehouse)

**Your node in one line:** you run HDFS's **NameNode** (the directory of every file and where its
blocks are) and one of its two **DataNodes**, plus one Spark **worker**. The Iceberg tables are
plain files in your HDFS under `/warehouse/search/`. You own showing their **snapshots, time
travel and rollback**, and what happens when a DataNode dies.

**What you must be able to explain:** NameNode vs DataNode; replication 2 (and why you can't
re-replicate with 2 DataNodes when one is down); what's in an Iceberg table folder (`data/`
Parquet, `metadata/` JSON + Avro); snapshot vs tag; time travel vs rollback; copy-on-write.
Your report tool is [`code/node2_storage/lake_report.py`](../code/node2_storage/lake_report.py).

Before you start: [`01-SETUP-ALL-MEMBERS.md`](01-SETUP-ALL-MEMBERS.md) §0–§9 done on node2
(including §6's heap line and §8's Iceberg jar).

Helper for read-only SQL (paste into each terminal; runs only on node2):
```bash
isql() { spark-sql --properties-file ~/spark-config/search.conf --master 'local[2]' -e "$1" 2>/dev/null; }
```

---

## M2-1. Format and start HDFS on node2  (the first step of the whole cluster)

```bash
jps                                         # nothing running (setup §2)
rm -rf ~/hadoop-data                        # the course's single-node HDFS: only test files in it
mkdir -p ~/hadoop-data/namenode ~/hadoop-data/datanode ~/hadoop-data/tmp
hdfs namenode -format -force 2>&1 | grep -E "successfully formatted|ERROR"
hdfs --daemon start namenode
hdfs --daemon start datanode
jps
ss -tlnp | grep -E ':9000|:9870'
```
- `format` creates a new, empty namespace with a new **cluster ID** in `~/hadoop-data/namenode`.
  Do it **once**. Formatting again later wipes the directory of all files (the blocks on the
  DataNodes would be orphaned).
- `hdfs --daemon start namenode` starts only that one daemon (not `start-dfs.sh`, setup §2).

✅ "successfully formatted"; `jps` shows `NameNode` and `DataNode`; port 9000 on `0.0.0.0`.
Announce **"NameNode up"**. Member 3 now starts node3's DataNode (M3-1).

## M2-2. Two live DataNodes, and the HDFS folders

```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|^Name:|^Hostname:"
hdfs dfs -mkdir -p /search/input/phase-0 /search/input/phase-1 /search/exports /warehouse /spark-logs
hdfs dfs -ls /
```
- `/spark-logs` **must exist before any Spark job** (event logging is on in `search.conf`).

✅ `Live datanodes (2):` with **node2's and node3's ZeroTier IPs** (`Name: 10.x.x.x:9866`).
If an address is `127.0.x.x` or a NAT address, fix `/etc/hosts` on that node (setup §4) and restart its DataNode.
Open `http://node2:9870` → *Datanodes* tab → 2 "In service". 📸 screenshot both.

## M2-3. Spark worker on node2  (after Member 3's M3-2 "master up")

```bash
start-worker.sh spark://node3:7077
jps                                        # + Worker
```
✅ `http://node3:8080` lists **2 workers ALIVE** (node2, node3), 2 cores and 1.5 GB each.
Worker UI: `http://node2:8081`.

---

## After Phase 0 (Member 4 is serving v0)

## M2-4. The lakehouse after Phase 0

```bash
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/node2_storage/lake_report.py 2>/dev/null
```
✅ `docs 1000 rows | 1 snapshots`, `links 499`, `rejects 40`, `doc_stats 1000`, `postings 233643`,
`postings_stem 209719`, `pagerank 1000`, `synonyms ~52000` (Word2Vec neighbours: the vocabulary × 5;
the exact count varies a little per training run). 📸 screenshot.

Look at what Iceberg put in HDFS (this is the Iceberg hands-on, on our real table):
```bash
hdfs dfs -ls -R /warehouse/search/docs
hdfs dfs -cat /warehouse/search/docs/metadata/v*.metadata.json | head -c 600; echo
hdfs fsck /warehouse -files -blocks | grep -E "Total files|Total blocks|Average block replication|Under-replicated"
```
- `data/*.parquet`: the rows. `metadata/`: `vN.metadata.json` (schema + snapshot list), `snap-*.avro`
  (manifest list), `*-m0.avro` (manifests). With a Hadoop catalog, `version-hint.text` says which `vN` is current.
- ✅ `Average block replication: 2.0` and `Under-replicated blocks: 0`: every Iceberg file has 2 copies.

**Tag the Phase 0 state** (a named, permanent pointer to this snapshot):
```bash
isql "ALTER TABLE iceberg_catalog.search.docs  CREATE TAG phase0"
isql "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase0"
isql "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs"
```
✅ `phase0  TAG  <the Phase 0 snapshot id Member 1 posted>`. Do this **before** Member 1 ingests Phase 1.

---

## After Phase 1 (Member 4 is serving v1)

## M2-5. Snapshots and time travel

```bash
isql "ALTER TABLE iceberg_catalog.search.docs  CREATE TAG phase1"
isql "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase1"
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/node2_storage/lake_report.py 2>/dev/null
```
✅ `docs 975 rows | 2 snapshots`. In the history: `append 1000` then `overwrite added 975, deleted 1000`. 📸 screenshot.

Why did a 100-row change set "delete 1000, add 975"? The table is one small Parquet file.
Iceberg's default for MERGE is **copy-on-write**: any file that contains a changed row is rewritten
in full. The old file stays on HDFS because the Phase 0 snapshot still points at it.

Time travel: read the table as it was, without changing anything:
```bash
isql "SELECT count(*) FROM iceberg_catalog.search.docs VERSION AS OF 'phase0'"
isql "SELECT count(*) FROM iceberg_catalog.search.docs"
isql "SELECT doc_id, title FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' WHERE doc_id IN ('1','100')"
isql "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id IN ('1','100')"
isql "SELECT count(*) AS deleted_in_phase1 FROM (SELECT doc_id FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' EXCEPT SELECT doc_id FROM iceberg_catalog.search.docs)"
hdfs dfs -ls /warehouse/search/docs/data /warehouse/search/docs/metadata
```
✅ 1000 vs 975 · Phase 0 has `1 April` and `100 Bubonic plague`; now only `100 Bubonic plague (Phase 1)` ·
`deleted_in_phase1 = 50` · `data/` has 2 Parquet files (old + new), `metadata/` has `v1..vN.metadata.json`. 📸 screenshot.

## M2-6. A bad update, and recovering from it

This shows "recover from bad updates". Someone deletes a lot of docs by mistake:
```bash
isql "DELETE FROM iceberg_catalog.search.docs WHERE title LIKE 'C%'"
isql "SELECT count(*) FROM iceberg_catalog.search.docs"
isql "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history"
```
✅ 878 rows (97 docs gone) and a 3rd snapshot in the history.

Meanwhile the search on node4 is **not affected**: it serves the exported Parquet of v1, never
Iceberg itself. Member 4 can show "computer science" still works.

Roll back to the last good state:
```bash
GOOD=$(isql "SELECT snapshot_id FROM iceberg_catalog.search.docs.refs WHERE name = 'phase1'"); echo "GOOD=$GOOD"
isql "CALL iceberg_catalog.system.rollback_to_snapshot('search.docs', $GOOD)"
isql "SELECT count(*) FROM iceberg_catalog.search.docs"
isql "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history"
```
✅ `GOOD=` a number (if it's empty, copy the `phase1` id from the `refs` query by hand); the
procedure prints `<bad id>  <good id>`; count back to **975**; the history has a 4th row with the
**good** id again, and the bad snapshot shows `is_current_ancestor = false`. 📸 screenshot.

- **Time travel** (M2-5) reads an old snapshot for one query. **Rollback** moves the table's
  "current" pointer. Neither one copies or deletes a data file. The rollback is a new metadata
  version pointing at an old snapshot, so history only ever grows.
- Ask Member 3 to run `build_index.py --phase 1` now: it prints `mode: up-to-date`, because the
  current snapshot is again the one it indexed. The pipeline sees exactly the state from before the mistake.

## M2-7. A DataNode dies  (with Member 3)

```bash
# [M3 on node3]
hdfs --daemon stop datanode
# [M2 on node2] reads still work right away: the client uses the other copy
hdfs dfs -cat /search/input/phase-0/pages.jsonl | wc -l          # 1040
isql "SELECT count(*) FROM iceberg_catalog.search.docs"           # 975
# about 90 seconds later
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication|Status"
```
✅ 1040 and 975 even with node3's DataNode down · then `Live datanodes (1)`, `Dead datanodes (1)`,
`Under-replicated blocks: <n> (100 %)`, `Status: HEALTHY` (no block is *missing*: one copy of each is left).
📸 screenshot, including `http://node2:9870` → Datanodes showing node3 *Dead*.

```bash
# [M3 on node3]
hdfs --daemon start datanode
# [M2] about 30 seconds later
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication"
```
✅ `Live datanodes (2)`, `Under-replicated blocks: 0`, `Average block replication: 2.0`.

Why under-replicated and not re-replicated? To get back to 2 copies, the NameNode needs a *second
live* DataNode that doesn't already hold the block. With only 2 DataNodes and one down, there is
none, so it waits. When node3 comes back, it reports its blocks (block report) and the counts are
back to 2 without copying anything.

---

## M2-8. Starting over, and restarting the cluster

**Wipe the pipeline's data but keep HDFS** (for a clean rehearsal):
```bash
hdfs dfs -rm -r -skipTrash /warehouse/search /warehouse/check '/search/exports/*' 2>/dev/null
hdfs dfs -ls -R /warehouse /search/exports
```
(Member 4 also empties `/serving` on node4: M4-9.)

**Shut down** (in this order): Member 4 stops uvicorn (Ctrl+C) → Member 3 `stop-worker.sh; stop-master.sh;
hdfs --daemon stop datanode` → you `stop-worker.sh; hdfs --daemon stop datanode; hdfs --daemon stop namenode`.

**Start again another day** (no format! the data is kept): you `hdfs --daemon start namenode;
hdfs --daemon start datanode` → Member 3 `hdfs --daemon start datanode; start-master.sh; start-worker.sh spark://node3:7077`
→ you `start-worker.sh spark://node3:7077` → `hdfs dfsadmin -safemode get` says OFF → `doctor.sh` on all nodes.
If ZeroTier IPs changed: fix `/etc/hosts` + `SPARK_LOCAL_IP` (setup §4, §7a) first.

---

## If it breaks

| Symptom | Fix |
|---|---|
| node3's DataNode never appears in the report | on node3: `tail -50 $HADOOP_HOME/logs/hadoop-*-datanode-*.log`. **`Incompatible clusterIDs`** = its data dir is from the old single-node HDFS → M3-1's `rm -rf ~/hadoop-data/datanode` |
| `Name node is in safe mode` | normal for the first ~30 s after start. `hdfs dfsadmin -safemode wait` |
| a DataNode shows with a `127.0.x.x` / NAT address | that node's `/etc/hosts` still maps its name to loopback (setup §4) |
| `isql` hangs | another app holds the cluster? `isql` uses `local[2]` so it shouldn't. Check `jps` and `http://node3:8080` |

**Viva quick answers**
- *Where is the Iceberg "catalog"?* A Hadoop catalog: no service, just the warehouse folder on HDFS.
  `version-hint.text` + `vN.metadata.json` per table is the whole catalog.
- *Why does the bad DELETE not break the search?* Node 4 serves an exported, validated Parquet
  copy. A new version only reaches it through export → checks → pull → verify.
- *What would you do in production after the rollback?* `CALL iceberg_catalog.system.expire_snapshots(...)`
  to delete the files that only the bad snapshot references; we keep them so the history stays visible.
- *Why replication 2 and not 3?* We have 2 DataNodes; a replica on the same node twice adds nothing.
