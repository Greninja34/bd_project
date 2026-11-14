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
| `changes.json` vs `changes.jsonl` | `bd ingest 1` uploads it to HDFS **as `changes.jsonl`** | |
| DELETE carrying a body | only `doc_id` is used; the body is ignored | |
| Phase 1: recompute or incremental? | **incremental** index: Iceberg tells which docs changed between two snapshots; only those are re-indexed. PageRank is recomputed (it is global) | `bd index 1 --full` |
| PageRank graph | only the `links` field, cleaned (both ends must exist) | |
| PageRank details | start at 1/N, dangling rank spread evenly, **exactly 10** iterations | `pagerank.py --iterations N` (N ≥ 10) |
| BM25 snippet bugs | both fixed (length lookup; only matching docs are scored) | |
| Normalisation | max BM25 over **this query's** matches; max PageRank over **the corpus** | |
| Text processing | strip wiki markup, lowercase, drop ~120 stopwords, **title indexed with the body**; exact words, no stemming | |
| Export validation | 13 checks (counts, ids, PageRank sums to 1, no orphan postings, built from the current snapshot ...) | |

### Design choices

- **Iceberg Hadoop catalog on HDFS:** the course setup; no metastore service to run.
- **One generated `spark-defaults.conf`** per node (`state/conf/spark`), so every job on every node has the same settings and `spark-submit file.py` needs no flags.
- **Spark ports pinned** (driver 40000/40001, block managers 40010+, worker 40020) and the driver advertises the node's ZeroTier IP: the firewall list is exact and `bd netcheck` can test it.
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

### M1, node1: ingestion (`code/node1_ingestion/ingest.py`)
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

