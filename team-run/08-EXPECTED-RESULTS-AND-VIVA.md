# 08: Expected results, design decisions, viva

Every number here was **measured** by running the code on the real data (2026-11-08): in Spark
local mode, on a 2-worker standalone cluster, and on a real HDFS. All three gave identical results.

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
| checks | 14 blocking + 2 NLP, all PASS | 14 blocking + 2 NLP, all PASS |
| docs / postings / terms | 1000 / 233,643 / 37,551 | 975 / 220,622 / 36,521 |
| files | `docs/` ~2.5 MB (with `body`, `keywords`), `postings/` ~850 KB, `postings_stem/` ~720 KB, `synonyms/` ~600 KB | similar |
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

Query latency: 2–30 ms. Loading + verifying a version: ~0.1 s, ~30 MB in memory.

### NLP twists (all measured; with every switch off the results above are unchanged)
| | Phase 0 / v0 | Phase 1 / v1 |
|---|---|---|
| stemmed index: distinct stems / postings | 28,485 / 209,719 (from 37,551 words) | 27,706 / 198,039 |
| Word2Vec: training tokens / vocabulary (min count 5) | 461,002 / 10,448 | 10,126 words |
| Word2Vec: training time (local / 2-worker cluster) | 47 s / — | 42 s / 38 s |
| Word2Vec neighbours (vary slightly per run) | king → queen, consort · planet → jupiter, saturn · river → danube, yangtze · computer → hardware, computers · disease → fever, diseases | king → queen, elizabeth · disease → diseases, infectious |
| keywords (TF-IDF top 5) | e.g. Speed: speed, hour, units, velocity, average · Flea: flea, fleas, rat, oriental, plague | |
| export size (all 4 Parquet sets) | ~4.7 MB | similar |
| Porter stemmer check | 84/84 textbook cases; identical to NLTK's original Porter on all 33,968 corpus words longer than 2 letters | |

| Query (v0) | switch | spec ranking | with the switch |
|---|---|---|---|
| phases | `stem` | 12 matches | 28 matches (v1: 12 → 75) |
| bubnic plauge | `spell` | 0 matches, *did you mean bubonic plague* | Bubonic plague · Flea |
| computr sciense | `spell` | 0 matches | Computer science · 1 (number) · Computer |
| river | `expand` | River · China · ... | + danube, yangtze; 94 matches |
| king (v1 model) | `expand` | King · ... | + queen, elizabeth → **Queen first** (query drift) |
| speed of light | `smart` | snippet = article start | "The biggest **speed** is the **speed** of **light**." |

Query latency with every switch on: ~30 ms.

### Time (measured on one laptop; expect 2–5× longer over ZeroTier)
| job | local[4] | 2-worker cluster on one laptop |
|---|---|---|
| ingest | 27 s | 30 s |
| index | 28 s (16 s of work) | 24 s of work |
| PageRank | 48 s (34 s of work) | 50 s; **101 s with `--slow-seconds 2` and a killed executor** |
| export | 30 s | 30 s |
| word2vec (NLP) | 79 s (47 s training) | 38 s training |

## 2. What's in the lakehouse (`iceberg_catalog.search.*`)

| table | written by | columns | notes |
|---|---|---|---|
| `docs` | ingest (MERGE) | doc_id, url, title, text, out_links, content_hash, phase, ingested_at | `out_links` = links as received; the hash detects real changes |
| `links` | ingest (overwrite only if different) | src, dst | only links whose two ends exist |
| `rejects` | ingest (per-phase partition) | phase, source, line_no, doc_id, reason, detail, raw | reasons: `unparseable_json`, `missing_or_empty:<field>`, `bad_url`, `bad_type:links`, `unknown_op`, `missing_links`, `duplicate_superseded`, `dangling_link_dropped` |
| `doc_changes` | ingest | doc_id, change_type, phase, from_snapshot, to_snapshot | computed from the two snapshots, after the merge |
| `doc_stats` | index | doc_id, length, snippet, body | `length` = BM25's len(D); `body` = cleaned prose (NLP snippets) |
| `postings` | index | term, doc_id, tf | the inverted index (`df` is added at export) |
| `postings_stem` | index | term, doc_id, tf | NLP: the same with Porter stems |
| `synonyms` | word2vec | term, neighbour, similarity, rank, docs_snapshot_id | NLP: 5 nearest Word2Vec neighbours per word |
| `pagerank` | pagerank | doc_id, rank, iterations, docs_snapshot_id | |
| `pipeline_runs` | every job | run_ts, stage, phase, docs_snapshot_id, details (JSON) | the audit log; lets the index find "what changed since I last ran", and lets the export refuse stale inputs |

## 3. Design decisions, in one line each

- **Iceberg Hadoop catalog on HDFS**: the course setup; no metastore service to run.
- **One `search.conf`** passed with `--properties-file`, so every job on every node has the same settings.
- **Hash before MERGE**: makes re-runs no-ops (no empty snapshots), so idempotency is visible.
- **Change detection by snapshot diff**: the spec's "detect changes from Iceberg". Works after a rollback too.
- **Incremental index, full PageRank**: postings are per-document; PageRank is global.
- **RDD PageRank**: lineage is explicit, which is what the fault-tolerance demo shows.
- **Export = gate**: 14 blocking checks (+ 2 NLP warnings), temp folder, `_SUCCESS` last, atomic rename. Node 4 verifies again. Either the switch is complete or it doesn't happen.
- **Node 4 serves Parquet from local disk**: no dependency on the cluster at query time. A bad Iceberg update can't break search.
- **Shared tokenizer**: queries and documents go through the same `textproc.py`.

## 4. Known limitations (say them before the examiner does)

- **Stemming is optional** (`stem=1`), and the spec ranking uses exact words. Porter also over-stems sometimes ("university" and "universe" both → "univers").
- **Word2Vec on 460 k tokens is small**: neighbours are topical rather than true synonyms (illness → pain, mental), and they change a little with each training run. Expansion can cause **query drift** ("king" → *Queen* first), which is why it's weighted (0.3 × similarity) and off by default.
- **Spelling correction only touches words missing from the vocabulary**: a real but wrong word ("plant" for "planet") is not corrected.
- **PageRank is almost binary on this data**, so 0.3 × PageRank works as a +0.255 bonus for having an in-link (that's why "april" ranks April 5th). This is the spec's formula.
  You might expect PageRank to barely change the ranking. It does change it, because of the max-normalisation.
- **Phase files are treated as upserts**: a doc missing from a later `pages.jsonl` is not deleted. Only `DELETE` records delete.
- **Copy-on-write rewrites the single docs data file** on every change (fine at 10 MB; merge-on-read would be the choice for big tables).
- **HDFS permissions are off** (4 different usernames on 4 VMs). In production you'd use one service user or Kerberos.

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
17. Why did a 100-row MERGE "delete 1000, add 975" records? *(Copy-on-write, one data file.)*
18. After rollback, is the bad snapshot's data deleted? *(No: `is_current_ancestor = false`, removed only by `expire_snapshots`.)*
19. Why are 7 links dropped in Phase 1? *(New pages link to docs Phase 1 deletes.)*

**Index / PageRank / export**
20. What is `tf`, `df`, `length`, and which job computes each?
21. How does the incremental index know what to redo? How do you know it's correct? *(Snapshot diff; we compared it against a full rebuild.)*
22. Write the PageRank update. What is dangling mass and why spread it?
23. Why do ranks always sum to 1? Which export check relies on it?
24. Name 5 of the 14 blocking export checks. Why are the NLP ones only warnings? Why is "built from the current snapshot" one of them?
25. Why can't node4 see a half-written export?

**Search**
26. Write BM25. What do k1 and b do? *(tf saturation / length normalisation.)*
27. The two bugs in the course snippet?
28. Why normalise BM25 per query but PageRank per corpus?
29. Why does "april" not return April first in v0? Is that a bug?
30. What happens to queries while the service switches from v0 to v1?
31. The export for v1 fails validation. What does the user of the search see? *(v0, unchanged.)*

**NLP twists**
32. What does the Porter stemmer do to "phases", "relational", "hopping"? Why must the query be stemmed with the *same* code as the index?
33. Why is there a second index (`postings_stem`) instead of stemming the only one? *(The spec ranking must stay exact; the switch picks the index per query.)*
34. How does "did you mean" pick a correction? Why can't it make a correct query worse?
35. What is Word2Vec (skip-gram) learning? Which part runs on the workers and which on the driver? Why does only the driver need numpy?
36. Why are expansion words weighted 0.3 × similarity, not 1? Show query drift with "king".
37. What is TF-IDF, and why compute keywords in the export rather than the index job? *(df is global: a change anywhere changes every doc's keywords.)*
38. Why do the NLP export checks only warn?
