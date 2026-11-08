# Distributed Search Engine (Big Data mini project, Team 32)

A mini-Google across **4 VMs on ZeroTier**: documents are ingested, validated and versioned in
**Apache Iceberg on HDFS** (replication 2), indexed and ranked with **Spark** (inverted index,
PageRank), exported as validated Parquet, and served by a **FastAPI** search service whose score is
`0.7 · BM25/max + 0.3 · PageRank/max`. On top of the spec: five optional NLP switches
(stemming, did-you-mean, Word2Vec query expansion, smart snippets + keywords, MiniLM semantic rerank).

| Node | Member | Runs |
|---|---|---|
| node1 | 1 | ingestion (Spark driver, HDFS client) |
| node2 | 2 | HDFS NameNode + DataNode, Spark worker, the Iceberg lakehouse |
| node3 | 3 | Spark master + worker, DataNode; index, Word2Vec, PageRank, export |
| node4 | 4 | FastAPI search service + UI |

## Start here

1. **Clone to the path every guide uses:**
   ```bash
   git clone git@github.com:Greninja34/bd_project.git ~/project_search_engine
   ```
2. **[`PREREQUISITES.md`](PREREQUISITES.md)**: what your VM needs. Check it with
   `bash ~/project_search_engine/code/tools/check_prereqs.sh node<N>`.
3. **[`team-run/00-START-HERE.md`](team-run/00-START-HERE.md)**: roles, plan, decisions. Then:
   - [`07-SOLO-PRACTICE.md`](team-run/07-SOLO-PRACTICE.md): the whole pipeline alone on your VM first
   - [`01-SETUP-ALL-MEMBERS.md`](team-run/01-SETUP-ALL-MEMBERS.md): turn the 4 VMs into one cluster
   - your guide: [Member 1](team-run/02-MEMBER1-NODE1-INGESTION.md) ·
     [Member 2](team-run/03-MEMBER2-NODE2-STORAGE.md) · [Member 3](team-run/04-MEMBER3-NODE3-COMPUTE.md) ·
     [Member 4](team-run/05-MEMBER4-NODE4-SEARCH.md)
   - [`06-TEAM-RUN-ORDER.md`](team-run/06-TEAM-RUN-ORDER.md): the order on the call
   - [`08-EXPECTED-RESULTS-AND-VIVA.md`](team-run/08-EXPECTED-RESULTS-AND-VIVA.md): numbers to expect + viva questions

## What's in the repo

| Path | Contents |
|---|---|
| `code/common/` | `textproc.py` (wiki markup → terms, Porter stemmer), `lake.py` (Iceberg helpers) |
| `code/node1_ingestion/` | `ingest.py`: parse, validate, dedupe, `MERGE INTO`, change detection from Iceberg snapshots |
| `code/node2_storage/` | `lake_report.py`: tables, snapshots, history, tags, rejects, run log |
| `code/node3_compute/` | `build_index.py`, `word2vec.py`, `pagerank.py`, `export.py` |
| `code/node4_search/` | `app.py` (FastAPI), `bm25.py`, `nlp.py`, `semantic.py`, `pull_export.py`, `q.py`, `static/index.html` |
| `code/tools/` | `doctor.sh` (network/daemons), `check_prereqs.sh`, `cluster_check.py`, `netmeter.sh` |
| `data/` | `phase-0/pages.jsonl` (corpus), `phase-1/changes.json` (change set), `toy/` |
| `team-run/` | the runbooks |

## Status

Tested end to end on one laptop (Spark local mode, a 2-worker standalone cluster, real HDFS). Both
phases, killing an executor mid-PageRank, a broken export being refused, an Iceberg rollback, and
all NLP switches work. **Not yet run on the 4 real VMs**, so expect network/VM issues and
start any debugging from `code/tools/doctor.sh`.
Pinned versions: Ubuntu 24.04, Java 11, Hadoop 3.3.6, Spark 3.3.4 (Python 3.10), Iceberg 1.8.1.
