# Prerequisites per member

What each VM needs **before the team call**. Most of it you already have from the course guides.
Do the items marked 🌐 at home: they need internet, and campus wifi is unreliable.

Check yourself at any time (read-only, prints OK / MISSING per item):
```bash
bash ~/project_search_engine/code/tools/check_prereqs.sh node1     # or node2 / node3 / node4
```

## Everyone (all 4 VMs)

| # | What | How / where | Check |
|---|---|---|---|
| 1 | Ubuntu 24.04 VM, your SRN as username | course guide 02 | `lsb_release -d` |
| 2 | Java 11 + `JAVA_HOME` | course guide 03 | `java -version` → 11 |
| 3 | `openssh-server`, passwordless `ssh localhost` | course guide 03 §4 | `ssh localhost echo ok` |
| 4 | Hadoop 3.3.6 at `/opt/hadoop`, `HADOOP_HOME` / `HADOOP_CONF_DIR` / PATH in `~/.bashrc` | course guide 05 (node1 and node4 only use it as an HDFS **client**) | `hadoop version` |
| 5 | ZeroTier installed, joined to the team network, authorised | course guide 04 | `ip -br a \| grep zt` |
| 6 | 🌐 `git` + this repo cloned to **`~/project_search_engine`** | `sudo apt install -y git` then `git clone git@github.com:Greninja34/bd_project.git ~/project_search_engine` | `ls ~/project_search_engine/code` |
| 7 | `sudo` rights (for `/etc/hosts`, `ufw`, `/opt`) | your own account | `sudo -v` |

## Member 1: node1 (Ingestion), 3 GB RAM

| What | How | Why |
|---|---|---|
| Spark 3.3.4 at `/opt/spark`, Python 3.10 at `/usr/bin/python3.10`, `PYSPARK_PYTHON` set | course guide 07 | node1 runs the ingestion **driver** |
| 🌐 Iceberg jar at `/opt/iceberg/` | team-run/01 §8 | the driver loads the Iceberg catalog |

## Member 2: node2 (HDFS NameNode + DataNode, Spark worker), 4 GB RAM, a stable laptop

| What | How | Why |
|---|---|---|
| Spark 3.3.4 + Python 3.10 + `PYSPARK_PYTHON` | course guide 07 | runs a Spark **worker** (executors run Python 3.10 tasks) |
| 🌐 Iceberg jar at `/opt/iceberg/` | team-run/01 §8 | for the `spark-sql` snapshot / time-travel / rollback demos |
| ~10 GB free disk | | HDFS NameNode + DataNode data in `~/hadoop-data` |

## Member 3: node3 (Spark master + worker, DataNode, index/PageRank/export), 4–6 GB RAM, a stable laptop

| What | How | Why |
|---|---|---|
| Spark 3.3.4 + Python 3.10 + `PYSPARK_PYTHON` | course guide 07 | master, worker, and the drivers of 4 jobs |
| 🌐 Iceberg jar at `/opt/iceberg/` | team-run/01 §8 | the drivers read and write Iceberg |
| 🌐 `~/nlp-venv`: Python 3.10 venv with `numpy==1.26.4` | team-run/01 §8b | NLP twist: `pyspark.ml` (Word2Vec) needs numpy on the driver |

## Member 4: node4 (Search service + UI), 3 GB RAM

| What | How | Why |
|---|---|---|
| (No Spark needed.) Hadoop from "Everyone" #4 | course guide 05 | `hdfs dfs -get` pulls exports (client only) |
| 🌐 `python3-venv` + `~/search-venv` with `requirements.txt` (FastAPI, uvicorn, pandas, pyarrow, numpy, fastembed) | team-run/05 M4-1 | the service; fastembed = MiniLM via ONNX Runtime, no PyTorch |
| 🌐 the MiniLM model (~90 MB) | `~/search-venv/bin/python code/node4_search/semantic.py --download` | NLP twist: semantic rerank, offline on demo day |
| `/serving` owned by you | `sudo mkdir -p /serving && sudo chown $USER:$USER /serving` | where exported versions live |
| a browser on node4 (or any team VM) | | the UI at `http://node4:8000` |

## Before the first call, every member also

1. Does [`team-run/07-SOLO-PRACTICE.md`](team-run/07-SOLO-PRACTICE.md) on their own VM (~30–45 min). It needs the
   Spark items above; members 1–3 also need `~/search-venv` (M4-1) and `~/nlp-venv` (§8b) for the full solo run.
2. Does [`team-run/01-SETUP-ALL-MEMBERS.md`](team-run/01-SETUP-ALL-MEMBERS.md) §0, §1, §2, §7b, §8 (no IPs needed yet).
3. Posts their ZeroTier IP in the group chat.
4. Reads their own guide (team-run/02–05) once, top to bottom.
