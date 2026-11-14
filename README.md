# Distributed Search Engine (Big Data mini project)

A mini-Google on **4 machines connected by ZeroTier**. Documents are ingested, validated and versioned in **Apache Iceberg on HDFS** (replication 2), indexed and ranked with **Spark** (inverted index, PageRank), exported as validated Parquet, and served by a **FastAPI** search service. Score: `0.7 · BM25/max + 0.3 · PageRank/max`.

| Node | Member | Runs |
|---|---|---|
| node1 | M1 | ingestion (Spark driver, HDFS client) |
| node2 | M2 | HDFS NameNode + DataNode, Spark worker, the Iceberg tables |
| node3 | M3 | Spark master + worker, DataNode, history server; index, PageRank, export |
| node4 | M4 | FastAPI search service + UI (HDFS client only) |

Pinned versions: Ubuntu 24.04, Java 11, Hadoop 3.3.6, Spark 3.3.4 (Python 3.10), Iceberg 1.8.1.

## Quickstart (every node)

```bash
git clone https://github.com/Greninja34/bd_project.git ~/bd_project
bash ~/bd_project/bd setup
source ~/bd_project/venv/bin/activate
bd help
```
`bd help` shows only the commands for your node, in order. The IPs of the four nodes are in [`team.env`](team.env).

## The docs

| File | What |
|---|---|
| [RUNBOOK.md](RUNBOOK.md) | **The guide.** Numbered steps: who, what to run, what you must see |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | By symptom or error text: cause and exact fix. Includes node3 on WSL2 |
| [VIVA.md](VIVA.md) | Expected numbers, design decisions, limitations, what each member must explain, viva questions |

## What is in the repo

| Path | Contents |
|---|---|
| `bd`, `scripts/` | the one command and what it runs (`setup`, `doctor`, `netcheck`, `start`, `status`, ...) |
| `team.env` | the one team settings file (ZeroTier IPs) |
| `code/common/` | `textproc.py` (wiki markup to terms), `lake.py` (Iceberg helpers) |
| `code/node1_ingestion/` | `ingest.py`: parse, validate, dedupe, `MERGE INTO`, change detection from Iceberg snapshots |
| `code/node2_storage/` | `lake_report.py`: tables, snapshots, history, tags, rejects, run log |
| `code/node3_compute/` | `build_index.py`, `pagerank.py`, `export.py` |
| `code/node4_search/` | `app.py` (FastAPI), `bm25.py`, `pull_export.py`, `q.py`, `static/index.html` |
| `code/tools/` | `cluster_check.py` |
| `data/` | `phase-0/pages.jsonl`, `phase-1/changes.json`, `toy/` |
| `venv/`, `state/` | created by `bd setup`, not in git (configs, HDFS data, logs, jars, serving folder) |

## Status

<!-- TEST-STATUS -->
Tested end to end on a 4-container podman cluster (4 users, NAT + overlay networks, real HDFS + Spark standalone + Iceberg): both phases, every demo, and a blocked node2->node3 port (the WSL2 failure) caught by `bd netcheck` and `bd check`. **Not yet run on the 4 real machines**: real ZeroTier, ufw and WSL2 are verified by `bd netcheck` (RUNBOOK step B2).
