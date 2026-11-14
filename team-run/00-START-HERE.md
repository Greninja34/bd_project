# Distributed Search Engine: team runbook (Team 32)

**HDFS (2 DataNodes, replication 2) + Spark standalone (2 workers) + Iceberg + FastAPI, across 4 VMs on ZeroTier.**

The code is written and was tested end to end on the real `pages.jsonl` + `changes.json`:
in Spark local mode, on a 2-worker Spark standalone cluster, and against a real HDFS. The tests
covered both phases, a worker killed in the middle of PageRank, a deliberately broken export,
and an Iceberg rollback. What's left is to set up the four VMs, run it, and understand it. Every
step below is a command you type, with what it does, so each of you can explain your part.

## The files

| Read / use | What it is |
|---|---|
| **`00-START-HERE.md`** | this page: who does what, in what order, what we assumed |
| [`01-SETUP-ALL-MEMBERS.md`](01-SETUP-ALL-MEMBERS.md) | **everyone**: ZeroTier, `/etc/hosts`, firewall, Hadoop + Spark + Iceberg config for the cluster |
| [`02-MEMBER1-NODE1-INGESTION.md`](02-MEMBER1-NODE1-INGESTION.md) | Member 1: upload to HDFS, ingestion + validation job, idempotency, Phase 1 changes |
| [`03-MEMBER2-NODE2-STORAGE.md`](03-MEMBER2-NODE2-STORAGE.md) | Member 2: HDFS NameNode + DataNode, Spark worker, Iceberg snapshots / time travel / rollback, DataNode failure |
| [`04-MEMBER3-NODE3-COMPUTE.md`](04-MEMBER3-NODE3-COMPUTE.md) | Member 3: Spark master + worker, DataNode, inverted index, PageRank, export, Spark fault tolerance |
| [`05-MEMBER4-NODE4-SEARCH.md`](05-MEMBER4-NODE4-SEARCH.md) | Member 4: FastAPI + UI, BM25 + final score, pulling exports, version switching |
| [`06-TEAM-RUN-ORDER.md`](06-TEAM-RUN-ORDER.md) | **the call**: the order of every step across the 4 nodes, with who does it |
| [`07-SOLO-PRACTICE.md`](07-SOLO-PRACTICE.md) | each of you runs the whole pipeline alone on one VM first (about 30 min) |
| [`08-EXPECTED-RESULTS-AND-VIVA.md`](08-EXPECTED-RESULTS-AND-VIVA.md) | the numbers you should see at each step, design decisions, viva Q&A |

The code is in [`../code/`](../code/):

```
code/
  common/textproc.py          wiki markup -> plain text -> terms   (used by Spark AND by the search service)
  common/lake.py              Iceberg table names, snapshot helpers, run log
  node1_ingestion/ingest.py   Member 1: parse, validate, dedupe, MERGE INTO Iceberg, change detection, link cleaning
  node2_storage/lake_report.py  Member 2: every table, row counts, snapshots, history, tags, rejects, run log
  node3_compute/build_index.py  Member 3: inverted index (full or incremental)
  node3_compute/pagerank.py     Member 3: PageRank with RDDs, >= 10 iterations, d = 0.85
  node3_compute/export.py       Member 3: 13 validation checks, atomic Parquet export
  node4_search/app.py           Member 4: FastAPI service + version switching
  node4_search/bm25.py          Member 4: the course BM25 snippet with its 2 bugs fixed + the final score
  node4_search/pull_export.py   Member 4: HDFS -> /serving/vN, verify, switch
  node4_search/q.py             Member 4: query the service from the terminal
  node4_search/static/index.html  the search UI
  tools/doctor.sh             any node: what's wrong with my network / daemons?
  tools/cluster_check.py      proves both workers, Python 3.10, Iceberg-on-HDFS all work
  tools/netmeter.sh           bytes sent/received on ZeroTier during a command
```

## Who does what

| Member | Node | Runs (daemons) | Owns |
|---|---|---|---|
| 1 | **node1** Ingestion | nothing (HDFS client + Spark driver only) | `ingest.py`, data quality, idempotent merge, Phase 1 changes |
| 2 | **node2** Storage | HDFS **NameNode + DataNode**, Spark **worker** | HDFS, replication 2, Iceberg tables, snapshots, time travel, rollback |
| 3 | **node3** Compute | Spark **master + worker**, HDFS **DataNode** | index, PageRank, export, Spark fault-tolerance demo |
| 4 | **node4** Search | the FastAPI service (no Spark, no HDFS daemon) | BM25 + final score, UI, pulling exports, switching versions |

Put **node2 and node3 on the two most stable laptops** (they hold all the data). Node 3 needs the
most RAM (4–6 GB VM), node 2 next (4 GB), node 1 3 GB, node 4 2 GB.

## The pipeline in one picture

```
 node1 (M1)                    node2 (M2) + node3 (M3)                            node4 (M4)
 ----------                    -----------------------                            ----------
 pages.jsonl ──hdfs put──►  HDFS /search/input/phase-0/   (2 copies: node2 + node3)
                                │
 ingest.py (driver) ──tasks──►  Spark workers node2 + node3
                                │  parse, validate, dedupe, MERGE INTO
                                ▼
                            Iceberg on HDFS /warehouse/search/
                              docs  links  rejects  doc_changes  pipeline_runs
                                │
            node3 drivers:  build_index.py  → postings, doc_stats
                            pagerank.py     → pagerank            (10 iterations)
                            export.py       → checks → /search/exports/v0/  (+ manifest.json, _SUCCESS)
                                                                   │
                                                 pull_export.py ◄──┘ hdfs dfs -get, verify
                                                 /serving/v0/ → CURRENT=v0 → FastAPI reloads
                                                 http://node4:8000   BM25 x 0.7 + PageRank x 0.3
 Phase 1: changes.jsonl → the same stages → v1 → switch (v0 stays available)
```

## How long it takes (honest estimate)

| What | Who | Time |
|---|---|---|
| [`07-SOLO-PRACTICE.md`](07-SOLO-PRACTICE.md) on your own VM | each of you, before the call | 30–45 min |
| [`01-SETUP-ALL-MEMBERS.md`](01-SETUP-ALL-MEMBERS.md) §1–§8 | each of you, before the call (needs the ZeroTier IPs) | 30–60 min |
| **Call 1**: cluster up (HDFS, Spark, cluster check, service started) | together | 45–90 min |
| **Call 2**: Phase 0, Phase 1, failure demos, screenshots | together | 2–3 h |

The spec says a phase takes 10–15 min of pipeline time. Locally each job takes 20–50 s; over
ZeroTier, expect 1–4 min per job, and longer when traffic is RELAYED instead of DIRECT.
I haven't run this on your four VMs, so these are estimates.

## Decisions we made where the spec is unclear

Where the spec leaves a choice open, the code does the
following. Each choice can be explained, and the ones marked ⚙ can be changed with a flag:

| Question | What the code does | Change it |
|---|---|---|
| Duplicates with no `version` field | highest `version` wins if present, otherwise **the last copy in the file** wins. The loser goes to `rejects` as `duplicate_superseded`, marked "identical copy" or "different content" | |
| Missing `links` key (20 docs) | **kept, with `links = []`** (only `doc_id, url, title, text` are required) → 1,000 docs | ⚙ `--missing-links reject` → 980 docs |
| Rejected records | stored in Iceberg `search.rejects` with phase, line number and reason; counts printed | |
| `changes.json` vs `changes.jsonl` | we upload it to HDFS **as `changes.jsonl`** | |
| DELETE carrying a body | only `doc_id` is used; the body is ignored | |
| Phase 1: recompute or incremental? | **incremental**: the index job asks Iceberg which docs changed between snapshots and re-indexes only those. PageRank is recomputed (it is global) | ⚙ `build_index.py --full` |
| PageRank graph | only the `links` field, cleaned (both ends must exist) | |
| PageRank details | start at 1/N, dangling rank spread evenly, **exactly 10** iterations | ⚙ `--iterations N` (N ≥ 10) |
| BM25 snippet bugs | both fixed (length lookup; only matching docs are scored) | |
| Normalisation | max BM25 over **this query's** matches; max PageRank over **the corpus** | |
| Text processing | strip wiki markup, lowercase, drop ~120 stopwords, **title indexed with the body**; exact words, no stemming | |
| Export validation | 13 checks (counts, ids, PageRank sums to 1, no orphan postings, built from the current snapshot ...) | |

## Spec deliverables → where they are shown

| Spec item | Shown by | Step |
|---|---|---|
| 1. Phase-based ingestion, dirty records | Member 1: ingest summary, `rejects` table, idempotent re-run | M1-3 … M1-7 |
| Iceberg snapshots, time travel, recovery | Member 2: history, tags, `VERSION AS OF`, bad delete + `rollback_to_snapshot` | M2-6 … M2-8 |
| HDFS replication 2 across nodes 2+3 | Member 2: `dfsadmin -report`, `fsck` "Average block replication 2.0", DataNode stop/start | M2-3, M2-9 |
| 2. Distributed inverted index | Member 3: `build_index.py` (full in Phase 0, incremental in Phase 1) | M3-5, M3-8 |
| 3. PageRank (≥ 10 iterations) | Member 3: `pagerank.py` iteration log | M3-6 |
| 4. Spark fault tolerance | Member 3 + 2: executor killed mid-PageRank, same result fingerprint | M3-10 |
| 5. BM25 relevance ranking | Member 4: score breakdown in the UI/API | M4-5 |
| 6. Search service + frontend, before/after changes | Member 4: v0 queries, switch to v1, same queries, refused broken export | M4-5 … M4-8 |
