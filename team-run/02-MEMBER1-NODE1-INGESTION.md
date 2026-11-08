# 02: Member 1, Node 1 (Ingestion)

**Your node in one line:** you hold the input files, put them into HDFS, and run the Spark job
that validates them and merges them into the Iceberg tables. Node 1 runs **no daemons**: it's an
HDFS *client* and a Spark *driver* (`--deploy-mode client`). The actual tasks run on the workers
on node2 and node3.

**What you must be able to explain:** every reject reason; "the last copy wins" for duplicates;
why the MERGE only contains real changes (idempotency); how changes are detected **from Iceberg**
(time travel between two snapshots); why links to missing docs are dropped *after* the merge.
The code is [`code/node1_ingestion/ingest.py`](../code/node1_ingestion/ingest.py). Its
docstring lists the 8 steps, and each step is one commented block.

Before you start: [`01-SETUP-ALL-MEMBERS.md`](01-SETUP-ALL-MEMBERS.md) §0–§9 done on node1.
The order across the team is in [`06-TEAM-RUN-ORDER.md`](06-TEAM-RUN-ORDER.md).

A helper for quick read-only SQL. Paste it into each terminal you use (`local[2]` = runs only on
node1, so it doesn't take cores from the cluster):
```bash
isql() { spark-sql --properties-file ~/spark-config/search.conf --master 'local[2]' -e "$1" 2>/dev/null; }
```

---

## M1-1. HDFS works from node1  (after Member 2's M2-3)

```bash
hdfs dfs -ls /
```
✅ lists `/search`, `/warehouse`, `/spark-logs` (created by Member 2).

## M1-2. The cluster runs your tasks  (after Member 3's M3-3)

```bash
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/tools/cluster_check.py 2>/dev/null
```
✅ `task ran on node2 ... python 3.10` and `task ran on node3 ... python 3.10`,
`workers used: node2, node3 -> OK`, and a table with one row `written_by = node1`.
(Member 3 runs the same thing on node3 and sees **two** rows: proof that both drivers share one Iceberg catalog on HDFS.)

## M1-3. Upload Phase 0 to HDFS

```bash
cd ~/project_search_engine
hdfs dfs -put -f data/phase-0/pages.jsonl /search/input/phase-0/pages.jsonl
hdfs dfs -ls /search/input/phase-0
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations | grep -E "Live_repl|Average block replication"
```
- `-put` copies the local file into HDFS. The client asks the NameNode on node2 where to write
  each block, then streams the bytes to a DataNode, which forwards them to the second
  DataNode (the replication pipeline).
- `fsck ... -locations` shows where each block is stored.

✅ `Live_repl=2` with two addresses (node2's and node3's ZeroTier IPs), and
`Average block replication: 2.0`. 📸 screenshot.

## M1-4. Ingest Phase 0

```bash
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/node1_ingestion/ingest.py \
    --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> ~/ingest-p0.log
```
- Spark's own log goes to `~/ingest-p0.log` (read it if something fails); the summary prints on screen.
- While it runs, open `http://node3:8080` (Spark master UI): the application `ingest-phase0` runs
  on 2 executors (node2, node3). Its job UI is at `http://node1:4040`.

✅ The summary must say (📸 screenshot):

```
lines read: 1040
rejected (invalid) lines: 20
   reason unparseable_json: 20
duplicate copies superseded (same doc_id): 20
accepted records (unique doc_id): 1000
   of which had no 'links' key (kept, links=[]): 20
changes merged: 1000
detected from Iceberg: ADDED / UPDATED / DELETED: 1000 / 0 / 0
docs in table before -> after: 0 -> 1000
links kept (both ends exist): 499
dangling links dropped: 0
docs snapshot before -> after: None -> <a long number>
```
Write the snapshot id in the group chat: it's **the Phase 0 snapshot**.

What happened, block by block (this is your viva answer):
1. `textFile(...).zipWithIndex()` reads the file in parallel and keeps each line's number.
2. `parse_line` runs on the executors: JSON → checks → either a clean row or a `reason`.
   Lines 1021–1040 are truncated JSON → `unparseable_json`.
3. A window `row_number() over (partition by doc_id order by version desc, line_no desc)` keeps
   one row per id. No record has `version`, so the **last** copy wins. Lines 981–1000 lose to their
   identical copies on lines 1001–1020 (`duplicate_superseded ... (identical copy)`).
4. A SHA-256 `content_hash` of url+title+text+links is compared with what's in the table. Only
   new or changed docs go into the merge (the table is empty, so all 1,000 do).
5. `MERGE INTO search.docs ... WHEN MATCHED AND op='DELETE' THEN DELETE / WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT`.
6. **Change detection from Iceberg:** the table is read at the snapshot from before the merge and
   at the snapshot from after it (time travel), and the two are compared by `content_hash` →
   ADDED / UPDATED / DELETED, stored in `search.doc_changes`.
7. Links: every `out_links` entry whose target is not a live doc is dropped; the rest go into `search.links`.
8. Rejects go into `search.rejects` (partitioned by phase), and one row goes into `search.pipeline_runs`.

## M1-5. Look at what you wrote

```bash
isql "SELECT count(*) FROM iceberg_catalog.search.docs"
isql "SELECT phase, reason, count(*) FROM iceberg_catalog.search.rejects GROUP BY phase, reason"
isql "SELECT line_no, doc_id, reason, detail, substr(raw,1,60) FROM iceberg_catalog.search.rejects ORDER BY line_no LIMIT 25"
isql "SELECT doc_id, title, url, size(out_links) FROM iceberg_catalog.search.docs ORDER BY cast(doc_id AS int) LIMIT 5"
isql "SELECT count(*) FROM iceberg_catalog.search.links"
```
✅ 1000 · `unparseable_json 20` + `duplicate_superseded 20` · 499. 📸 screenshot the rejects.

## M1-6. Idempotency: ingest Phase 0 again

```bash
spark-submit --properties-file ~/spark-config/search.conf code/node1_ingestion/ingest.py \
    --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2>> ~/ingest-p0.log
```
✅ `unchanged ...: 1000`, `changes merged: 0`, `links table rewritten: False`, and
`docs snapshot before -> after` shows **the same id twice**: no new snapshot, identical contents.
📸 screenshot. This is the spec's "ingesting the same phase twice should not change the resulting table".
(Only `pipeline_runs`, the audit log, gets a new row.)

Then tell Member 3: **"Phase 0 ingested"**.

---

## Phase 1 (after the team has looked at the Phase 0 results)

## M1-7. Upload and ingest the change set

```bash
cd ~/project_search_engine
hdfs dfs -put -f data/phase-1/changes.json /search/input/phase-1/changes.jsonl
spark-submit --properties-file ~/spark-config/search.conf code/node1_ingestion/ingest.py \
    --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> ~/ingest-p1.log
```
- The Drive file is `changes.json`; we store it under the spec's name `changes.jsonl`.

✅ (📸 screenshot)
```
lines read: 100
rejected (invalid) lines: 0
accepted records (unique doc_id): 100
   of which DELETE: 50
changes merged: 100
detected from Iceberg: ADDED / UPDATED / DELETED: 25 / 25 / 50
docs in table before -> after: 1000 -> 975
links kept (both ends exist): 467
dangling links dropped: 7
```
Write the new snapshot id in the chat: **the Phase 1 snapshot**.

```bash
isql "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type ORDER BY phase, change_type"
isql "SELECT doc_id, reason, detail FROM iceberg_catalog.search.rejects WHERE phase = 1"
isql "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id IN ('1','100','phase1-new-001')"
```
✅ phase 0: ADDED 1000; phase 1: ADDED 25, DELETED 50, UPDATED 25 · 7 `dangling_link_dropped`
rows (e.g. `phase1-new-001 -> 14`: the new page links to *Alanis Morissette*, which Phase 1
deletes) · doc 1 (April) is gone, doc 100 is now "Bubonic plague (Phase 1)".

## M1-8. Idempotency again

Run M1-7's `spark-submit` a second time.

✅ `unchanged ...: 50`, `DELETE of a doc that is not in the table: 50`, `changes merged: 0`, same
snapshot before and after. 📸 screenshot. Then tell Member 3: **"Phase 1 ingested"**.

---

## If it breaks

| Symptom | Fix |
|---|---|
| `Initial job has not accepted any resources` | another job is using all 4 cores (check node3:8080 → "Running Applications"), or workers can't reach `node1:40000`: `ufw` on node1 (setup §5), `spark.driver.host node1` in your `search.conf` |
| `File does not exist: /search/input/...` | M1-3 / M1-7 upload not done, or a typo in the path |
| `ClassNotFoundException ... iceberg` | `/opt/iceberg/...jar` missing on node1 (setup §8) |
| `Table or view not found` in `isql` | the job failed before creating it; read `~/ingest-p0.log` (`grep -m5 -i error`) |
| Want to start completely over | ask Member 2 (M2-10: wipe the warehouse), then M1-4 again |

**Viva quick answers**
- *Why not keep the first duplicate?* Without a `version`, later in the file = newer; that matches
  "highest version wins" when versions are line order. The toy file shows why it matters: its two
  copies of doc 2 are different.
- *Why keep docs with no `links` key?* `links` isn't content. The 4 required fields are there, and
  the docs are searchable. The flag `--missing-links reject` gives 980 docs if the TA disagrees.
- *Where is "detect changes from Iceberg"?* Step 6: `snapshot_diff()` in `common/lake.py` reads
  `docs` at two snapshot ids (`spark.read.option("snapshot-id", ...)`) and compares the hashes.
  The index job uses the same function to re-index only those docs.
- *Why is a re-run a no-op and not just "the same result"?* We compare hashes **before** the MERGE
  and skip it entirely when nothing changed, so Iceberg doesn't even create an empty snapshot.
