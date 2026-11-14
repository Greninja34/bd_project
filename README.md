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
```
Then follow [RUNBOOK.md](RUNBOOK.md): **one ordered call script** (S01, S02, ...). Every step names its node and member; the quick index at its top lists "my steps" per member. After each stage there is a **GATE** (`bash ~/bd_project/scripts/check.sh <stage>`) that must print `GATE <stage> PASSED` before anyone continues. Something broke? `bash ~/bd_project/scripts/doctor.sh` and paste the output in the group chat. Every later terminal starts with:
```bash
source ~/bd_project/venv/bin/activate
cd ~/bd_project
```
There is no wrapper program: you type the real `hdfs`, `spark-submit`, `spark-sql` and `uvicorn` commands. The IPs of the four nodes are in [`team.env`](team.env).

## The docs

| File | What |
|---|---|
| [RUNBOOK.md](RUNBOOK.md) | **The call script.** One ordered timeline S01..S70: only the real commands, the numbers to expect, what to say in the chat, a GATE after every stage |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | By symptom or error text: cause and exact fix. Includes node3 on WSL2 |
| [VIVA.md](VIVA.md) | Expected numbers, design decisions, limitations, viva questions, and section 7: what every RUNBOOK step does, per member (by step id) |

## What is in the repo

| Path | Contents |
|---|---|
| `scripts/` | `env.sh` (loaded by `activate`: node detection, config dirs) · `check.sh <stage>` (read-only GATE before the next stage) · `doctor.sh` (read-only, run it when something breaks and paste the output in the chat) · `netcheck.py` (optional port/bandwidth diagnostic) |
| `team.env` | the one team settings file (ZeroTier IPs) |
| `code/common/` | `textproc.py` (wiki markup to terms), `lake.py` (Iceberg helpers) |
| `code/node1_ingestion/` | `ingest.py`: parse, validate, dedupe, `MERGE INTO`, change detection from Iceberg snapshots |
| `code/node2_storage/` | `lake_report.py`: tables, snapshots, history, tags, rejects, run log |
| `code/node3_compute/` | `build_index.py`, `pagerank.py`, `export.py` |
| `code/node4_search/` | `app.py` (FastAPI), `bm25.py`, `pull_export.py`, `q.py`, `static/index.html` |
| `code/tools/` | `cluster_check.py` |
| `data/` | `phase-0/pages.jsonl`, `phase-1/changes.json`, `toy/` |
| `venv/`, `state/` | created by the RUNBOOK setup steps (S04-S10), not in git (configs, HDFS data, logs, jars, serving folder) |

## Status

<!-- TEST-STATUS -->
Tested end to end on a 4-container podman cluster (4 users, NAT + overlay networks, real HDFS + Spark standalone + Iceberg): both phases, every demo, and a blocked node2->node3 port (the WSL2 failure) caught by the `nc` port test and the cluster_check. **Not yet run on the 4 real machines**: real ZeroTier, ufw and WSL2 are verified by the `nc` port test (RUNBOOK S13-S17). Note: `check.sh` and `doctor.sh` have **not** been run against a live cluster yet (only syntax-checked). The RUNBOOK lists the plain commands that an earlier wrapper script ran; only the wrapper-free form (setup heredocs, hand-started daemons) has not been executed yet.
