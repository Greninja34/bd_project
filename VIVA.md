# VIVA

Numbers to compare against, the lakehouse tables, the design decisions, known limitations, and questions to quiz each other.
Every number was measured on the real data (Spark local mode, a 2-worker standalone cluster, a real HDFS: identical results).

## 1. The numbers at each step

### Ingestion (`ingest.py`)
| | Phase 0 (`pages.jsonl`) | Phase 0 again | Phase 1 (`changes.jsonl`) | Phase 1 again |
|---|---|---|---|---|
| lines read | 1040 | 1040 | 100 | 100 |
| rejected (invalid) | 20 `unparseable_json` | 20 | 0 | 0 |
| duplicates superseded | 20 (identical copies) | 20 | 0 | 0 |
| accepted (unique ids) | 1000 (20 had no `links` key → `[]`) | 1000 | 100 (50 DELETE) | 100 |
| unchanged / DELETE of absent doc | 0 / 0 | **1000** / 0 | 0 / 0 | **50 / 50** |
| changes merged | 1000 | **0** | 100 | **0** |
| ADDED / UPDATED / DELETED (from Iceberg) | 1000 / 0 / 0 | 0 / 0 / 0 | 25 / 25 / 50 | 0 / 0 / 0 |
| docs after | 1000 | 1000 | **975** | 975 |
| links kept / dangling dropped | 499 / 0 | 499 / 0 | **467 / 7** | 467 / 7 |
| new docs snapshot? | yes | **no** | yes | **no** |

With `--missing-links reject`: 40 `missing_links` + 20 `unparseable_json` rejected, **980** docs, 499 links.

### Index (`build_index.py`)
| | Phase 0 | Phase 1 |
|---|---|---|
| mode | full | **incremental** (100 changed docs from the snapshot diff) |
| docs in index | 1000 | 975 |
| distinct terms | 37,551 | 36,521 |
| postings rows | 233,643 | 220,622 |
| avg / max doc length (terms) | 461.0 / 13,903 | 447.4 / 13,903 |
| most common terms (df) | one 589, references 588, people 584, called 542 | one 558, references 553, people 550 |

Incremental Phase 1 = full rebuild of Phase 1, row for row (checked with a hash over both tables).

### PageRank (`pagerank.py`, 10 iterations, d = 0.85)
| | Phase 0 | Phase 1 |
|---|---|---|
| docs / links | 1000 / 499 | 975 / 467 |
| dangling docs (no out-link) | 501 | 508 |
| L1 change at iteration 1 → 10 | 0.425 → 0.00097 | 0.458 → 0.0085 |
| sum of ranks | 1.000000 | 1.000000 |
| max / min | 0.001752 / 0.000264 (6.65×) | 0.003926 / 0.000279 (14.08×) |
| docs tied at the minimum | 501 | 548 |
| **fingerprint** | **`60a6221350e7`** | **`54ee091e74b5`** |

The fingerprint was identical with an executor killed in the middle of the run.

### Export (`export.py`)
| | v0 | v1 |
|---|---|---|
| checks | 13, all PASS | 13, all PASS |
| docs / postings / terms | 1000 / 233,643 / 37,551 | 975 / 220,622 / 36,521 |
| files | `docs/`, `postings/` ~850 KB, `manifest.json`, `_SUCCESS` | similar |
| `--simulate-failure` | | 3 FAIL (5 docs without PageRank, sum 0.998, 1 orphan posting) → nothing published, exit 1 |

### Search (node 4): top 3 per query
| query | v0 | v1 |
|---|---|---|
| bubonic plague | Bubonic plague (100) 1.000 · Flea (3523) 0.622 · London (460) 0.495 | Bubonic plague (Phase 1) (100) 0.721 · Flea · London |
| alan turing | Alan Turing (13) 0.907 · Mathematics (467) · List of mathematicians (3433) | List of mathematicians (3433) 0.721 · 20th century (3407) · Mars (disambiguation) (2972) |
| april | March (468) 0.870 · May (469) 0.807 · Time limit (828) 0.778 · … April (1) 5th, 0.745 | March 0.816 · December (185) 0.794 · May 0.741 (April deleted) |
| phase | 20 matches: Phases of the Moon (3540) · States of matter (3325) · Ebola virus (242) | 69 matches: Phases of the Moon · Contact network (Phase 1) (131) · Phase 1 Added Page 1 |
| ocean | Indian Ocean (1978) 1.000 · Arctic Ocean (1980) · Pacific Ocean (1975) | the same three |
| computer science | Computer science (110) 1.000 · 1 (number) (571) · Computer (112) | 1 (number) 0.722 · Computer science (Phase 1) (110) 0.721 · Information (3609) |
| solar system planets | Planet (639) 1.000 · Solar System (800) · Jupiter (408) | Planet · Solar System · Comet (168) |

Query latency: 2–30 ms. Loading + verifying a version: ~0.1 s, a few tens of MB in memory at most.

### Time (measured on one laptop; expect 2–5× longer over ZeroTier)
| job | local[4] | 2-worker cluster on one laptop |
|---|---|---|
| ingest | 27 s | 30 s |
| index | 28 s (16 s of work) | 24 s of work |
| PageRank | 48 s (34 s of work) | 50 s; **101 s with `--slow-seconds 2` and a killed executor** |
| export | 30 s | 30 s |

## 2. What's in the lakehouse (`iceberg_catalog.search.*`)

| table | written by | columns | notes |
|---|---|---|---|
| `docs` | ingest (MERGE) | doc_id, url, title, text, out_links, content_hash, phase, ingested_at | `out_links` = links as received; the hash detects real changes |
| `links` | ingest (overwrite only if different) | src, dst | only links whose two ends exist |
| `rejects` | ingest (per-phase partition) | phase, source, line_no, doc_id, reason, detail, raw | reasons: `unparseable_json`, `missing_or_empty:<field>`, `bad_url`, `bad_type:links`, `unknown_op`, `missing_links`, `duplicate_superseded`, `dangling_link_dropped` |
| `doc_changes` | ingest | doc_id, change_type, phase, from_snapshot, to_snapshot | computed from the two snapshots, after the merge |
| `doc_stats` | index | doc_id, length, snippet | `length` = BM25's len(D) |
| `postings` | index | term, doc_id, tf | the inverted index (`df` is added at export) |
| `pagerank` | pagerank | doc_id, rank, iterations, docs_snapshot_id | |
| `pipeline_runs` | every job | run_ts, stage, phase, docs_snapshot_id, details (JSON) | the audit log; lets the index find "what changed since I last ran", and lets the export refuse stale inputs |

## 3. Design decisions

### Where the spec was unclear

| Question | What the code does | Change it |
|---|---|---|
| Duplicates with no `version` field | highest `version` wins if present, otherwise **the last copy in the file** wins. The loser goes to `rejects` as `duplicate_superseded` ("identical copy" or "different content") | |
| Missing `links` key (20 docs) | **kept, with `links = []`** (only `doc_id, url, title, text` are required) → 1,000 docs | `ingest --missing-links reject` → 980 docs |
| Rejected records | stored in Iceberg `search.rejects` with phase, line number and reason; counts printed | |
| `changes.json` vs `changes.jsonl` | the `hdfs dfs -put` of Phase 1 stores it in HDFS **as `changes.jsonl`** | |
| DELETE carrying a body | only `doc_id` is used; the body is ignored | |
| Phase 1: recompute or incremental? | **incremental** index: Iceberg tells which docs changed between two snapshots; only those are re-indexed. PageRank is recomputed (it is global) | `build_index.py --phase 1 --full` |
| PageRank graph | only the `links` field, cleaned (both ends must exist) | |
| PageRank details | start at 1/N, dangling rank spread evenly, **exactly 10** iterations | `pagerank.py --iterations N` (N ≥ 10) |
| BM25 snippet bugs | both fixed (length lookup; only matching docs are scored) | |
| Normalisation | max BM25 over **this query's** matches; max PageRank over **the corpus** | |
| Text processing | strip wiki markup, lowercase, drop ~120 stopwords, **title indexed with the body**; exact words, no stemming | |
| Export validation | 13 checks (counts, ids, PageRank sums to 1, no orphan postings, built from the current snapshot ...) | |

### Design choices

- **Iceberg Hadoop catalog on HDFS:** the course setup; no metastore service to run.
- **One generated `spark-defaults.conf`** per node (`state/conf/spark`), so every job on every node has the same settings and `spark-submit file.py` needs no flags.
- **Spark ports pinned** (driver 40000/40001, block managers 40010+, worker 40020) and the driver advertises the node's ZeroTier IP: the firewall list is exact and the `nc` port test can check it.
- **HDFS URIs use the name `node2`:** Iceberg stores absolute paths in table metadata. If an IP changes, only `/etc/hosts` changes.
- **Hash before MERGE:** re-runs are no-ops (no empty snapshots), so idempotency is visible.
- **Change detection by snapshot diff:** the spec's "detect changes from Iceberg". Works after a rollback too.
- **Incremental index, full PageRank:** postings are per document; PageRank is global.
- **RDD PageRank:** lineage is explicit, which is what the fault-tolerance demo shows.
- **Export = gate:** 13 checks, temp folder, `_SUCCESS` last, atomic rename. Node 4 verifies again. The switch is complete or it does not happen.
- **Node 4 serves Parquet from local disk:** no dependency on the cluster at query time. A bad Iceberg update cannot break search.
- **Shared tokenizer:** queries and documents go through the same `textproc.py`.

## 4. Known limitations (say them before the examiner does)

- **Exact word matching only:** no stemming, synonyms or spelling correction. "phases" does not find "phase"; a typo matches nothing. The spec's ranking does not ask for more, and it keeps results explainable.
- **PageRank is almost binary on this data** (501 docs at the minimum, 490 at the maximum after normalising), so 0.3 × PageRank works as a +0.255 bonus for having an in-link. That is why "april" ranks April 5th. It is the spec's formula working as defined.
- **Phase files are upserts:** a doc missing from a later `pages.jsonl` is not deleted. Only `DELETE` records delete.
- **Copy-on-write rewrites the single docs data file** on every change (fine at 10 MB; merge-on-read for big tables).
- **HDFS permissions are off** (4 different usernames on 4 VMs). In production: one service user or Kerberos.
- **Replication 2 with 2 DataNodes:** when one dies there is no node to re-replicate to; data is safe but under-replicated until it returns.
- **Not tested on the real machines:** ZeroTier, ufw, WSL2 (see TROUBLESHOOTING.md#not-tested).

### Why April is 5th

April has the best BM25 (`bm25 = 1.000`) but nobody links to it, so `pr = 0.15`: `0.7·1.0 + 0.3·0.15 = 0.745`. March has `bm25 = 0.815` and an in-link, so `pr = 1.0`: `0.7·0.815 + 0.3 = 0.870`. The link graph is so sparse that normalised PageRank is almost binary.

## 5. Viva questions (quiz each other)

**Architecture / HDFS**
1. What does the NameNode store, and what do the DataNodes store? *(Namespace + block locations / the blocks.)*
2. Which setting makes a file get 2 copies, and on which node must it be set? *(`dfs.replication` on the writing client: every node.)*
3. node3's DataNode dies. Can you still read `pages.jsonl`? Why is the file "under-replicated" and not re-replicated? *(Yes, from the other copy. There's no third DataNode to copy to.)*
4. Why doesn't node4 run any daemon, and how does it still get data from HDFS? *(It's a client: `hdfs dfs -get` talks to the NameNode and DataNodes.)*
5. Why must `/etc/hosts` not map the hostname to 127.0.1.1?

**Spark**
6. Driver, executor, master, worker: which runs where in our setup?
7. What's a shuffle? Point one out in the index job. *(`groupBy(term, doc_id)`.)*
8. Why `spark.sql.shuffle.partitions = 8`?
9. What is lineage, and how did it save the PageRank job when we killed an executor?
10. Why did the killed iteration take ~3× longer and the others not?
11. What happens if the **driver** dies? *(The job is lost. Lineage only covers executors. Rerun; Iceberg commits are atomic, so no half-written table.)*

**Ingestion / Iceberg**
12. Walk through the 8 steps of `ingest.py`.
13. Why do 20 duplicate pairs end up as 1000 docs, and which copy wins?
14. How is the job idempotent? What exactly stays the same on a re-run?
15. How are changes "detected from Iceberg"?
16. What's a snapshot? A tag? What does `VERSION AS OF` do vs `rollback_to_snapshot`?
17. Why did a 100-row MERGE "delete 1000, add 975" records? *(Copy-on-write: the affected data files are rewritten.)*
18. After rollback, is the bad snapshot's data deleted? *(No: `is_current_ancestor = false`, removed only by `expire_snapshots`.)*
19. Why are 7 links dropped in Phase 1? *(New pages link to docs Phase 1 deletes.)*

**Index / PageRank / export**
20. What is `tf`, `df`, `length`, and which job computes each?
21. How does the incremental index know what to redo? How do you know it's correct? *(Snapshot diff; we compared it against a full rebuild.)*
22. Write the PageRank update. What is dangling mass and why spread it?
23. Why do ranks always sum to 1? Which export check relies on it?
24. Name 5 of the 13 export checks. Why is "built from the current snapshot" one of them?
25. Why can't node4 see a half-written export?

**Search**
26. Write BM25. What do k1 and b do? *(tf saturation / length normalisation.)*
27. The two bugs in the course snippet?
28. Why normalise BM25 per query but PageRank per corpus?
29. Why does "april" not return April first in v0? Is that a bug?
30. What happens to queries while the service switches from v0 to v1?
31. The export for v1 fails validation. What does the user of the search see? *(v0, unchanged.)*

## 6. What each member must be able to explain

### M1, node1: ingestion (`code/node1_ingestion/ingest.py`; step notes in section 7.1)
Every reject reason; "the last copy wins" for duplicates; why the MERGE only contains real changes (idempotency); how changes are detected **from Iceberg** (time travel between two snapshots); why links to missing docs are dropped *after* the merge. The 8 steps:
1. `textFile(...).zipWithIndex()` reads the file in parallel and keeps each line's number.
2. `parse_line` on the executors: JSON → checks → a clean row or a `reason` (lines 1021–1040 are truncated JSON → `unparseable_json`).
3. Window `row_number() over (partition by doc_id order by version desc, line_no desc)` keeps one row per id.
4. SHA-256 `content_hash` of url+title+text+links vs what is in the table: only new or changed docs go into the MERGE.
5. `MERGE INTO search.docs ... WHEN MATCHED AND op='DELETE' THEN DELETE / WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT`.
6. Change detection: read the table at the snapshot before the merge and after it, compare by `content_hash` → ADDED / UPDATED / DELETED into `search.doc_changes`.
7. Links: every `out_links` target that is not a live doc is dropped; the rest go into `search.links`.
8. Rejects → `search.rejects` (partitioned by phase), one row → `search.pipeline_runs`.

Quick answers:
- *Why the last duplicate?* Without a `version`, later in the file = newer. The toy file shows why it matters: its two copies of doc 2 differ.
- *Why keep docs with no `links` key?* `links` is not content; the 4 required fields are there. `--missing-links reject` gives 980 docs if the TA disagrees.
- *Where is "detect changes from Iceberg"?* Step 6: `snapshot_diff()` in `common/lake.py` reads `docs` at two snapshot ids and compares the hashes. The index job uses it too.
- *Why is a re-run a no-op?* Hashes are compared **before** the MERGE and it is skipped when nothing changed, so Iceberg creates no empty snapshot.

### M2, node2: HDFS, Spark worker, the lakehouse (`code/node2_storage/lake_report.py`)
NameNode vs DataNode; replication 2 (and why you cannot re-replicate with 2 DataNodes when one is down); what is in an Iceberg table folder (`data/` Parquet, `metadata/` JSON + Avro, `version-hint.text`); snapshot vs tag; time travel vs rollback; copy-on-write.
- *Where is the Iceberg "catalog"?* A Hadoop catalog: no service, only the warehouse folder on HDFS.
- *Why does the bad DELETE not break search?* Node 4 serves an exported, validated Parquet copy. A new version only arrives through export → checks → pull → verify.
- *In production after the rollback?* `CALL iceberg_catalog.system.expire_snapshots(...)` deletes files only the bad snapshot uses. We keep them so the history stays visible.
- *Why replication 2, not 3?* We have 2 DataNodes; a second replica on the same node adds nothing.

### M3, node3: Spark master/worker, index, PageRank, export (`code/node3_compute/`)
Master vs worker vs driver vs executor; what the index's `groupBy` shuffles; incremental indexing from snapshots; the PageRank formula and why dangling rank is spread evenly; lineage and how Spark recovers a lost executor; the export checks and why publishing is atomic.
- *Index:* a Python UDF on the executors runs `textproc.doc_terms` (strip markup, lowercase, split, drop stopwords); `explode` gives one row per word; `groupBy(term, doc_id).count()` gives `tf`: **that is the shuffle**. `length` = terms per doc (BM25's len(D)). `df` is computed in the export.
- *Incremental:* the job finds the docs snapshot it indexed last time (in `pipeline_runs`), asks Iceberg for the diff to the current one, deletes the postings of those docs and adds postings for the ones that still exist. Checked equal to a full rebuild (same hash).
- *PageRank:* each iteration `links.join(ranks)` → every doc sends `rank/outdegree` → `reduceByKey` → `(1-d)/N + d·(incoming + dangling/N)`. Dangling = rank of docs with no out-link (501 of 1000).
- *Fault tolerance:* cached `ranks` partitions and shuffle files on the killed executor are lost. The scheduler sees `ExecutorLostFailure`/`FetchFailed`, re-runs the parent stages from the lineage, retries the failed tasks. Nothing is checkpointed. If the **driver** dies the job is lost (rerun; Iceberg commits are atomic).
- *Export safety:* write to `_tmp_vN_<time>/`, run the blocking checks, write `manifest.json` and `_SUCCESS`, then **rename** to `vN`. HDFS rename is atomic.
- *Why is PageRank slower than the index?* Latency-bound: 10 iterations × several small jobs × a shuffle each. The index is CPU-bound. That is why shuffle partitions are 8, not 200.

### M4, node4: search (`code/node4_search/`)
The BM25 formula (k1 = 1.2, b = 0.75); the two bugs in the course snippet; the final score and its normalisation; why PageRank pushes April down; why a version switch cannot serve a broken or half-copied export.
- *Bug 1:* the snippet reads `row["length"]` from the index, which has no length column → `KeyError`. We look the length up in the docs table by `doc_id`.
- *Bug 2:* it starts **every** doc at 0.0, so with `+ 0.3·PageRank` docs matching nothing would rank. We only score docs containing at least one query term.
- *Score:* `IDF(t) = ln(1 + (N - df + 0.5)/(df + 0.5))`; per doc `IDF · tf·(k1+1) / (tf + k1·(1 - b + b·len/avgdl))`, summed over the query's distinct terms. `final = 0.7·BM25/max BM25 (this query) + 0.3·PR/max PR (corpus)`.
- *Why max BM25 per query but max PageRank over the corpus?* BM25 depends on the query; PageRank is a fixed property of the corpus.
- *`pull_export.py` steps:* `_SUCCESS` in HDFS? → `hdfs dfs -get` into a hidden `.incoming-vN` → verify locally (manifest OK, checks passed, sizes, row counts) → rename to `vN` → write `CURRENT` (temp file + rename) → `POST /api/admin/reload`. The service loads and verifies again before swapping under a lock; on failure it keeps the old version and `CURRENT` is put back.
- *Why load everything in memory?* 975 docs and 220k postings are a few tens of MB. A query takes milliseconds and needs no Spark.
- *Repeated query words?* Counted once (`dict.fromkeys`), usual BM25 practice.


## 7. What each RUNBOOK step does (by member; step ids refer to [RUNBOOK.md](RUNBOOK.md))

The RUNBOOK has only commands and expected numbers. Everything you must be able to say about a step is here. Rules that hold for all steps: every terminal starts with `source ~/bd_project/venv/bin/activate` and `cd ~/bd_project` (the prompt then shows `(node3)`); activating loads `scripts/env.sh`, which points `HADOOP_CONF_DIR` and `SPARK_CONF_DIR` at our own configs in `~/bd_project/state/conf/` and sets `SPARK_LOCAL_IP` to your ZeroTier IP. The Spark/Iceberg settings (master `spark://172.23.10.141:7077`, `hdfs://node2:9000`, catalog `iceberg_catalog`, event log) are in `state/conf/spark/spark-defaults.conf` (S10), so plain `spark-submit` and `spark-sql` need no `--master` or `--properties-file`. The course's own config in `/opt` is never touched. Spark jobs print their summary on screen; the noisy driver log goes to a file with `2> state/logs/<name>.log` (`2>` sends only stderr). There is no wrapper program: every command is a real command you can explain; the only helpers are `scripts/env.sh` (loaded by `activate`), `scripts/check.sh` (read-only gates), `scripts/doctor.sh` (read-only "paste me in the chat"). Logs: `state/logs/` (jobs), `state/logs/hadoop/`, `state/logs/spark/` (daemons); error patterns: [TROUBLESHOOTING#log-patterns](TROUBLESHOOTING.md#log-patterns).

| Member | Node | Role |
|---|---|---|
| M1 | node1 (172.23.59.75) | ingestion: Spark driver + HDFS client. No daemons |
| M2 | node2 (172.23.194.191) | storage: HDFS NameNode + DataNode, Spark worker, the Iceberg tables |
| M3 | node3 (172.23.10.141) | compute: Spark master + worker, DataNode, history server. Drivers for index, PageRank, export |
| M4 | node4 (172.23.204.87) | search: FastAPI service. HDFS client only, no Spark |

### 7.0 Setup and network steps (everyone)

You already did the course guides (Java 11, Hadoop 3.3.6, Spark 3.3.4 + Python 3.10, ZeroTier). Setup turns those single-machine installs into one 4-node cluster. Nothing is reinstalled, only configured. Everything we write goes into `~/bd_project/state/` (configs, HDFS data, logs, jars, serving folder) or `/etc/hosts`; delete `state/` for a clean start.

- **S01** clones the code and shows the installed versions. Spark 3.3.4 breaks on Python 3.11+, so the executors use `/usr/bin/python3.10` on every node. node4 needs `git` + `python3-venv`; everyone needs `nc` for the port tests.
- **S02 (node3, WSL2).** ZeroTier must run **inside** Ubuntu, not only on Windows. Why: Spark executors on node2 must connect *into* the driver on node3 (port 40000); WSL2's NAT blocks that. With ZeroTier inside Ubuntu the traffic lands on Ubuntu's own interface and Windows never sees it. `systemd=true` lets ZeroTier run as a service; `generateHosts=false` stops WSL from rewriting `/etc/hosts` at every start (we write it in S05). WSL stops when the last terminal closes, so keep one open.
- **S03.** Your IP from `team.env` is listed; peers DIRECT (RELAY works but is slow); all 4 ZeroTier node IDs differ. If an IP ever changes: one person edits `team.env` and pushes; everyone `git pull` and redoes S05-S10.
- **S04.** `scripts/env.sh` finds which of the four team IPs is on this machine (so it knows if it is node1..node4) and exports `HADOOP_CONF_DIR` / `SPARK_CONF_DIR` (our own copies in `state/conf/`), the log and pid folders under `state/`, `SPARK_LOCAL_IP`, and `SERVING_DIR` on node4. The last line makes `source ~/bd_project/venv/bin/activate` load it in every new terminal. Nodes 1-3 get a venv without pip (Spark runs on the system Python 3.10; the venv is only for the activate hook); node4 installs the web service dependencies.
- **S05.** From now on every config uses the **names** `node1`..`node4`. Why disable `127.0.1.1 <hostname>`: with it, Java resolves this machine's own name to a loopback address, so the NameNode or Spark master listens only on loopback and no other VM can connect. The last `sed` adds your hostname (your SRN) to your own line so `sudo` and Java still resolve it, now to the ZeroTier IP. Nothing else may map your hostname to 127.x.
- **S06.** HDFS and Spark use many ports (some chosen at random), so instead of a port list we trust the 3 teammates' ZeroTier IPs. If `ufw` is `inactive` the loop is harmless.
- **S07.** Optional MTU 1400, not kept after a reboot; only when [TROUBLESHOOTING#mtu](TROUBLESHOOTING.md#mtu) says so.
- **S08.** Iceberg is a library for Spark. The jar is needed where a **driver** runs (node1, node3, and node2 for its `spark-sql`); Spark ships it to the executors itself (`spark.jars`). The checksum proves the download is intact.
- **S09.** Copies the course's default Hadoop config, then overwrites the two files that matter:

| Setting | Why |
|---|---|
| `fs.defaultFS = hdfs://node2:9000` | the NameNode's address. Every node, even the clients node1 and node4, needs it (it is why `hdfs dfs -ls /search` needs no `hdfs://`) |
| `hadoop.tmp.dir`, `namenode.name.dir`, `datanode.data.dir` | fresh folders in `state/hdfs`, not the course's `~/hadoop-data` |
| `dfs.replication = 2` | every block is stored on 2 DataNodes (node2 + node3). **The writer's** setting decides this, so it must be on every node |
| `rpc-bind-host = 0.0.0.0` | the NameNode listens on all interfaces, so loopback mistakes cannot hide it |
| `permissions.enabled = false` | our 4 VMs have 4 different usernames. Node 1 creates the Iceberg tables and node 3 writes into the same folders; without this node 3 gets "Permission denied" |
| `ip-hostname-check = false` | the NameNode accepts DataNodes even if reverse DNS of the ZeroTier IP is odd |
| `heartbeat.recheck-interval = 30000` | a stopped DataNode is marked dead after about 90 s instead of 10.5 min (for the failure demo) |
| `replace-datanode-on-failure = NEVER` | with only 2 DataNodes there is no spare to swap into a write pipeline; without this, writes fail while one DataNode is down |
| `HADOOP_HEAPSIZE_MAX = 512m` | the HDFS daemons get 512 MB instead of 1/4 of RAM each |

- **S10.** `spark-env.sh` is read by the daemons (master and worker); `spark-defaults.conf` is read by every `spark-submit` / `spark-sql`. node4 only gets the serving folder (no Spark there).

| Setting | Why |
|---|---|
| `SPARK_LOCAL_IP`, `SPARK_PUBLIC_DNS` = your ZeroTier IP | without them Spark advertises the VM's **NAT** address (10.0.2.15, 192.168.x.x), which no other VM can reach. Symptom: *Initial job has not accepted any resources* |
| `SPARK_MASTER_HOST` = node3's IP | only used by node 3's `start-master.sh`; harmless elsewhere |
| `SPARK_WORKER_CORES/MEMORY/PORT` | the workers on node2 and node3 each offer 2 cores, 1.5 GB, and use port 40020 |
| `spark.master = spark://<node3 IP>:7077` | every job connects to the master on node3 |
| `spark.driver.host` = your IP, `bindAddress` 0.0.0.0 | executors connect **back** to the driver, so it must advertise a reachable address but listen on all |
| ports 40000 / 40001 / 40010 (+ `maxRetries`) | fixed ports make connection errors predictable and the firewall list exact |
| `shuffle.partitions = 8` | 200 tiny tasks over a ZeroTier link is what makes jobs crawl |
| `cores.max = 4` | one job takes all 4 cores, so **run one job at a time** |
| `pyspark.python = /usr/bin/python3.10` | driver and executors must run the same Python (3.10) |
| `hadoop.fs.defaultFS`, `dfs.replication` | the Spark side of the HDFS settings |
| `sql.extensions` + `catalog.iceberg_catalog.*` | turns on Iceberg: the catalog `iceberg_catalog`, type `hadoop`, tables stored under `hdfs://node2:9000/warehouse` |
| `spark.jars` | the Iceberg jar from S08 |
| `eventLog.*`, `history.fs.logDirectory` | finished jobs are written to HDFS `/spark-logs`; the history server on node3 reads them |
| `derby.system.home` | stops `spark-sql` from leaving `metastore_db/` folders wherever you run it |
| `log4j2.properties` level `warn` | Spark's log goes to stderr (we send it to a file); the job summaries on stdout become readable |

- **S11.** Before we start anything, any `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`, `Master`, `Worker` or `HistoryServer` in `jps` is left over from the course: it holds the ports and is configured for `localhost`. They are stopped with the **course** config. **Never run `start-dfs.sh`, `start-yarn.sh` or `start-all.sh`** in this project: they SSH into the other VMs or start a second NameNode; we start each daemon by hand so you see what runs where.
- **S12 / S12a (`check.sh setup`, `doctor.sh`).** `check.sh <stage>` is read-only: it only looks (ping, `nc`, `jps`, `ss`, `hdfs -ls/-cat`, `curl`) and prints `GATE <stage> PASSED` or `FAILED`. It never starts anything. `doctor.sh` is the "paste me in the chat" script: sections for this machine, `/etc/hosts`, ping and ZeroTier, daemons and ports, HDFS and Spark as seen from here, firewall, and the last ERROR/WARN lines of every daemon log each annotated with its fix.
- **S13-S16 (port test before any daemon runs).** This is the WSL2 failure (executors on node2 must reach the driver on node3, port 40000), so we test it with `nc` instead of finding out in the middle of a Spark job. `nc -l` opens a throwaway listener; `nc -zv` only tries to connect. `succeeded` = reachable. `timed out` = a firewall or NAT drops it ([ufw](TROUBLESHOOTING.md#ufw), [WSL2](TROUBLESHOOTING.md#wsl2)). `refused` = the route works but nobody listens, which is the normal answer before the daemons run. Swap roles (node2 listens, node3 probes) because one direction proves nothing about the other.
- **S17 (`check.sh network`).** Pings every peer and probes the daemon ports of each node. A port with no listener is reported as "skipped" (route fine); only a timeout fails. Optional bigger test: `python3 scripts/netcheck.py` on all 4 at once.

### 7.1 Member 1 (node1): ingestion

**Role.** You are the front door of the data. You upload the raw files to HDFS, then run a Spark job that validates every line, removes duplicates, and merges the clean rows into the Iceberg `docs` table. node1 runs no daemons: it is an HDFS client and a Spark driver.

**You must be able to explain**
- What a Spark driver and an executor are, and why executors on node2/node3 connect *into* your driver (ports 40000/40001).
- HDFS: block, replication 2 (`Live_repl=2` in fsck), NameNode (metadata) vs DataNode (blocks), why the client writes blocks straight to DataNodes.
- Why bad lines are stored in `rejects` with a reason instead of being dropped (20 `unparseable_json`, 20 `duplicate_superseded`).
- `MERGE INTO` and idempotence: the second run finds 0 changes and the Iceberg snapshot id does not change.
- How Phase 1 changes are detected *from Iceberg* (time-travel diff between two snapshots): ADDED / UPDATED / DELETED.
- Why `changes.json` is stored in HDFS as `changes.jsonl`.

**Steps**
- **S26.** Starts a small Spark job with node1 as the driver. It proves: tasks run on BOTH workers, executors use Python 3.10, executors can import our shared code, and Iceberg on HDFS works (it writes one row into the table `check.hello`).
- **S29.** `-put` copies the local file into HDFS (`-f` overwrites an earlier copy). The path has no `hdfs://` because `fs.defaultFS` is `hdfs://node2:9000`. `fsck -blocks -locations` shows where every block is stored.
- **S30.** The job reads the file line by line, parses and validates each line, keeps the last copy of duplicate `doc_id`s, compares with the current table (content hash) and runs `MERGE INTO search.docs` with only the real changes. Then it finds what changed from the Iceberg snapshots, rebuilds the link graph (drops links to docs that do not exist) and stores the rejects. The summary is on screen, the Spark log in `state/logs/ingest-p0.log`. The `docs snapshot ... after` id is the Phase 0 snapshot that M2 tags.
- **S31.** `SQL=...` is a shortcut variable for this terminal. `spark-sql -e` runs one statement; `--master local[2]` runs it on this machine only (a read does not need the cluster). **`--conf spark.eventLog.enabled=false`** stops it from writing an event-log file into HDFS `/spark-logs` (matters while a DataNode is down: such a file would have only 1 replica). The last command is Phase 0 again on the same input: nothing changes.
- **S41.** Uploads the 100 change lines (local `changes.json`, HDFS name `changes.jsonl`) and runs the same job for phase 1: `UPSERT` rows insert or replace, `DELETE` rows remove. Then the job rebuilds the links.
- **S42.** `doc_changes` is the change log derived from Iceberg; `rejects` for phase 1 holds the links dropped because their target is gone.
- **S32 / S43.** Gates `ingest0` / `ingest1` read the Iceberg metadata in HDFS (the `total-records` of each snapshot), so no Spark job is needed to check 1000 / 499 / 40 and later 975 / 467 / 47.

### 7.2 Member 2 (node2): storage

**Role.** You own HDFS and the Iceberg tables. node2 runs the HDFS NameNode (the metadata master), a DataNode, and a Spark worker. You format and start HDFS, watch replication, tag the Iceberg snapshots, and show time travel and rollback.

**You must be able to explain**
- NameNode vs DataNode; what `hdfs namenode -format` creates and why doing it twice destroys HDFS (cluster ID).
- Replication 2: what `fsck` shows, under-replicated blocks when a DataNode dies, the ~90 s before the NameNode declares it dead, why nothing is re-copied with only 2 DataNodes.
- Iceberg layout in HDFS: `data/` (Parquet), `metadata/` (JSON + Avro), `version-hint.text`; snapshots; copy-on-write (why a 100-row change rewrites files and the old ones stay).
- Tags, time travel (`VERSION AS OF`), rollback (`rollback_to_snapshot`): reading old vs moving the "current" pointer.
- Why safe mode exists, and why the course's old daemons must not run next to ours.

**Steps**
- **S18.** Formatting writes an empty NameNode metadata directory (`state/hdfs/namenode`) and gives the filesystem a new cluster ID. Every DataNode later remembers that ID; formatting again would make existing DataNodes refuse to join (and erase all HDFS metadata). If `jps` shows old course daemons, stop them as in S11 first.
- **S19.** Starts the NameNode in the background (log in `state/logs/hadoop/`). It listens on `172.23.194.191:9000`. Right after start it is in safe mode (read-only) until DataNodes report; for an empty new filesystem it leaves it by itself within ~30 s (`hdfs dfsadmin -safemode leave` as last resort). The five folders: `/search/input/phase-0` and `/search/input/phase-1` raw files, `/search/exports` exports for node4, `/warehouse` Iceberg tables, `/spark-logs` Spark event logs (the history server reads it).
- **S20 / S24.** The DataNode stores the block files (`state/hdfs/datanode`) and reports to the NameNode at `node2:9000` (port 9866 for data). The worker registers at the Spark master on node3 (`spark://172.23.10.141:7077`); it keeps retrying until node3's master is up, so the order between M2 and M3 does not matter.
- **S22 / S25.** The gate asks the NameNode which DataNodes are alive (`dfsadmin -report`), asks the Spark master (status page as JSON; **the trailing slash in `/json/` matters**) which workers are ALIVE, and checks safe mode, the five folders, and a written-and-deleted test file with `Live_repl=2`. DataNodes and workers need a few seconds to register: wait 30 s and run it again.
- **S39.** `lake_report.py` prints every table, its row count and its snapshots. `ls -R` shows the Iceberg files in HDFS (`data/` = Parquet rows, `metadata/` = JSON + Avro; `version-hint.text` says which `vN` is current). `fsck` shows replication of those files. `CREATE TAG` gives a snapshot a permanent name. Do this BEFORE Phase 1 is ingested, after M3's tables exist. A tag that already exists: use another name.
- **S49.** Same report, now with 2 snapshots; then the same table read at the tag `phase0` and at "now". Why "delete 1000, add 975" for a 100-row change? Copy-on-write: the data files (4 Parquet files here) are rewritten; the old files stay because the `phase0` snapshot points at them.
- **S52.** `CoarseGrainedExecutorBackend` is the Spark executor process of the running job on your worker. `kill -9` ends it at once, like a crashed machine. Spark notices, starts a new executor and recomputes the lost partitions from lineage. Stronger variant: `stop-worker.sh` on node2 during the job; the job finishes on node3 alone with the same fingerprint.
- **S57 / S59.** The first line asks Iceberg for the snapshot id behind the tag `phase1` and keeps it in `GOOD`. The `CALL` moves the table's "current" pointer back to it. No data files are copied or deleted (time travel only reads an old snapshot; rollback moves the pointer). Search on node4 is unaffected by the bad delete (S58) because it serves exported Parquet, never Iceberg. Then M3 runs `build_index.py --phase 1` again (S60): `mode: up-to-date`.
- **S61-S64.** While node3's DataNode is stopped, reads still work: HDFS reads the second copy from node2's own DataNode. About 90 s later the NameNode stops hearing heartbeats from node3 and marks it dead; the blocks that had a copy there become under-replicated (under-replicated, not re-replicated: with 2 DataNodes and one down there is no third node to copy to). When node3 returns, its block report fixes the counts.
- **Another day / reset (RUNBOOK last table).** Stop order: workers, DataNodes, NameNode last (M3 stops node3 first). Start another day **without format**. "Re-format an existing HDFS" also needs `rm -rf state/hdfs/datanode` on node3 (old cluster ID).

### 7.3 Member 3 (node3): compute

**Role.** You run the Spark master and the heavy jobs. node3 runs the Spark master, a worker, a DataNode and the history server; you are the driver of the inverted index, PageRank and the validated export.

**You must be able to explain**
- Spark standalone: master (7077, UI 8080), workers (2 here), driver, executors; why the driver's ports are pinned (40000/40001) and why node3 runs in WSL2 with ZeroTier inside.
- The inverted index (term → doc, tf), BM25, and full vs **incremental** index (Iceberg tells which docs changed between two snapshots).
- PageRank (d = 0.85, 10 iterations, dangling-node mass spread evenly, sum of ranks = 1) written with RDDs; why the fingerprint stays the same after an executor crash (lineage recomputation).
- The export: 13 validation checks, `_SUCCESS` file, rename to `_failed_...` on failure, why a broken export is never served.
- Where time and network go (the metering demo); why 8 shuffle partitions.

**Steps**
- **S21.** Starts node3's DataNode. It stores block copies and reports to the NameNode at `node2:9000`; it keeps retrying until the NameNode is up.
- **S23.** `start-master.sh` starts the cluster manager on `spark://172.23.10.141:7077` (UI `http://node3:8080`; `SPARK_MASTER_HOST` is set in our `spark-env.sh`). `start-worker.sh <master url>` starts a worker that registers there. The history server (UI `http://node3:18080`) shows finished jobs from the event logs in HDFS `/spark-logs`; it needs that folder (S19 first), otherwise repeat that line later.
- **S27.** The same test as S26 with node3 as the driver. `check.hello` now has a second row: two drivers on two machines share one Iceberg table on HDFS. `--drop` (run last) deletes the test table.
- **S33.** Reads the `docs` table, turns wiki markup into terms, counts term frequency per doc and writes `postings` and `doc_stats` to Iceberg. The first time it builds everything (`full`).
- **S34.** Reads the `links` table, runs 10 PageRank iterations with RDD joins and `reduceByKey`, spreads the rank of docs without out-links evenly, writes the `pagerank` table. `--phase` is only for the run log.
- **S35.** Runs 13 checks (index built from the current docs snapshot, every doc has a PageRank, ranks sum to 1, no orphan postings, ...). Only if all pass does it write plain Parquet (docs, postings) plus `manifest.json` and `_SUCCESS` to `/search/exports/v0`. If a check fails the folder is renamed `_failed_v0_<time>` and the job exits 1. The gate `export v0` re-reads `manifest.json` (status OK, 13/13 checks) from HDFS.
- **S44.** The first run asks Iceberg which docs differ between the snapshot of the last index and now, and re-indexes only those. The second run finds nothing to do. (`--full` forces a rebuild.)
- **S45.** PageRank is global (one changed link changes everything), so it is fully recomputed.
- **S51-S53.** `--slow-seconds 3` makes every task sleep 3 s so the job lasts long enough to kill something. `grep -oE ... | sort | uniq -c` counts the failure messages in the driver log. Spark recomputed the lost partitions from lineage, so the fingerprint equals the clean run.
- **S54.** `--simulate-failure` corrupts the export on purpose; the validation must catch it (nothing published, folder renamed `_failed_v1-broken_<time>`).
- **S61 / S63.** Stop and start of node3's DataNode for the failure demo (M2 runs S62, S64).
- **S65.** Reads the byte counters of the ZeroTier interface before and after the job and prints the difference. What to say: the data is tiny, bandwidth is never the problem; PageRank is latency-bound (10 iterations, each a few small jobs with a shuffle); the index is CPU-bound (regex over 10 MB of wiki text). That is why we use 8 shuffle partitions, not 200.
- **S68.** Stop order on node3: history server, worker, master, DataNode, before M2 stops the NameNode.

### 7.4 Member 4 (node4): search

**Role.** You turn the validated export into a search engine. node4 runs no Hadoop or Spark daemons: it is an HDFS client (it copies an export out of HDFS) and runs the FastAPI service that answers queries from plain Parquet. You also own the safety rule: a bad export is refused and the old version keeps serving.

**You must be able to explain**
- Why the service reads exported Parquet, not Iceberg (Iceberg problems like the bad DELETE cannot reach search).
- The ranking: `score = 0.7 · BM25/max + 0.3 · PageRank/max` (blue and orange bars in the UI); why April is 5th (above).
- What `pull_export.py` does in order: look for `_SUCCESS`, `hdfs dfs -get` into `.incoming-<v>`, verify manifest and counts, atomic rename, write `CURRENT`, ask the service to reload.
- Versions on node4 (`state/serving/v0`, `v1`, `CURRENT`) and switching without downtime.
- What happens on `REFUSED`.

**Steps** (you need two terminals: A runs the service, B everything else)
- **S28.** `uvicorn app:app` runs the FastAPI app `app` from `app.py`, so you must `cd` into `code/node4_search` first (that is why this terminal does not stay in `~/bd_project`). `--host 0.0.0.0` makes it reachable from the other nodes on port 8000. It stays in the foreground (Ctrl-C stops it). The health call shows which version is loaded (`no-version` before the first pull); `hdfs dfs -ls /search` proves node4 can reach the NameNode.
- **S37.** `pull_export.py` copies the export from HDFS into `state/serving/` (the venv sets `SERVING_DIR`), verifies it (manifest, 13 checks, file sizes, row counts), makes it the `CURRENT` version and tells the running service to reload; any failure stops it and the old version keeps serving. `q.py "words" [k]` calls the service's `/api/search` and prints the top `k` (default 5). If the service was not running, the pull still works and says so: start it (S28) and it loads `CURRENT`. v0 top results:

| query | v0 top results |
|---|---|
| bubonic plague | Bubonic plague (100) 1.000 · Flea (3523) 0.622 · London (460) 0.495 |
| alan turing | Alan Turing (13) 0.907 · Mathematics (467) · List of mathematicians (3433) |
| april | March (468) 0.870 · May (469) 0.807 · Time limit (828) · ... April (1) is 5th |
| ocean | Indian Ocean (1978) 1.000 · Arctic Ocean (1980) · Pacific Ocean (1975) |
| computer science | Computer science (110) 1.000 · 1 (number) (571) · Computer (112) |
| zzzqqq | 0 matches |

- **S38.** The UI at `http://node4:8000`; screenshot with the score bars visible (blue BM25, orange PageRank). Why April is 5th: above.
- **S48.** Before the pull the v0 answers, after it the v1 answers:

| query | v1 |
|---|---|
| bubonic plague | **Bubonic plague (Phase 1)** (100), the updated doc |
| alan turing | Alan Turing (13) is **gone** (deleted); top is List of mathematicians (3433) |
| april | 66 matches (was 71); April (1) is gone |
| phase | 69 matches (was 20): the 25 added and 25 updated pages |
| computer science | 174 matches; "Computer science (Phase 1)" (110) |

- **S40 / S50 (`check.sh pulled vN`, `serve vN`).** `pulled` reads `state/serving/vN` on node4 (`_SUCCESS`, manifest OK, 13/13); `serve` asks `/api/health` (version and doc count) and runs the query `ocean`.
- **S55.** There is no `_SUCCESS` for `v1-broken` (the failed export was renamed), so the pull refuses at step 1 and nothing changes.
- **S56.** Both versions sit side by side in `state/serving/`; the POST makes the service load another one in memory (or in the UI: *Versions on this node* → *serve this*).
- **S67 / another day.** Stop = Ctrl-C in terminal A; start again with S28 (it loads `CURRENT`). Rehearse from zero: empty `state/serving`, start the service again.
