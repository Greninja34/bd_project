# 06: The team run: who does what, in which order

The commands live in each member's guide (step ids like **M2-1**). This page is the **order**.
Each row ends with something you can see. Don't start a row until the previous row's ✅ is there.
Announce each "→ say" line in the group chat, so the next person knows they can go.

`M1`…`M4` = Member 1…4 on node1…node4. `[all]` = everyone at once.

## Before the call (each of you, at home: needs internet)

| Who | Do | Guide |
|---|---|---|
| all | the whole pipeline alone on your VM | [`07-SOLO-PRACTICE.md`](07-SOLO-PRACTICE.md) |
| all | setup §0, §1, §2, §7b, §8 (no IPs needed for these) | [`01`](01-SETUP-ALL-MEMBERS.md) |
| all | ZeroTier joined (§3), IP posted in the chat | [`01`](01-SETUP-ALL-MEMBERS.md) §3 |
| M4 | venv + pip install + `/serving` | M4-1 |
| M3 | numpy venv for Word2Vec | setup §8b |
| all | read your own guide once, top to bottom | 02–05 |

## Call 1: the cluster (≈ 45–90 min)

| # | Who | Do | ✅ / → say |
|---|---|---|---|
| 1 | all | setup §4 (`/etc/hosts`), §5 (ufw), §6 (Hadoop conf), §7a + §7c (Spark conf) with the real IPs | each pings node1–node4 |
| 2 | M2 | **M2-1** format + start NameNode, DataNode | → "NameNode up" |
| 3 | M3 | **M3-1** DataNode on node3 | |
| 4 | M2 | **M2-2** report + create the HDFS folders | `Live datanodes (2)` → "HDFS ready" |
| 5 | M3 | **M3-2** Spark master + worker, **M3-3** history server | → "master up" |
| 6 | M2 | **M2-3** Spark worker on node2 | node3:8080 shows 2 workers ALIVE |
| 7 | all | `bash ~/project_search_engine/code/tools/doctor.sh` | all port checks OK except node4:8000 |
| 8 | M1 | **M1-1**, **M1-2** cluster check | tasks on node2 + node3, Python 3.10 |
| 9 | M3 | **M3-4** cluster check (2 rows), then `--drop` | |
| 10 | M4 | **M4-2** HDFS client, **M4-3** start the service | `/api/health` → no-version |

If Call 1 ends here, stop the cluster with M2-8's shutdown order. Next time, start it with M2-8's
restart order (no format) and run `doctor.sh`.

## Call 2: Phase 0, Phase 1, the demos (≈ 2–3 h with the NLP demos)

### Phase 0
| # | Who | Do | ✅ / → say |
|---|---|---|---|
| 11 | M1 | **M1-3** upload, **M1-4** ingest, **M1-5** look, **M1-6** re-run | 1000 docs, 20 + 20 rejects, re-run = same snapshot → "Phase 0 ingested" + snapshot id |
| 12 | M3 | **M3-5** index, **M3-6b** Word2Vec, **M3-6** PageRank, **M3-7** export v0 | all PASS (incl. 2 `[nlp]`) → "v0 exported" |
| 13 | M4 | **M4-4** pull v0, **M4-5** queries, **M4-5b** the 4 NLP switches | "Bubonic plague" first; April 5th for "april"; each twist before/after |
| 14 | M2 | **M2-4** lake report, HDFS files, `fsck`, **tag phase0** | replication 2.0, tag listed |

### Phase 1
| # | Who | Do | ✅ / → say |
|---|---|---|---|
| 15 | M1 | **M1-7** upload + ingest changes, **M1-8** re-run | 25 / 25 / 50, 975 docs, 7 dangling links → "Phase 1 ingested" |
| 16 | M3 | **M3-8** incremental index, **M3-9** Word2Vec + PageRank + export v1 | `mode: incremental`, fingerprint `54ee091e74b5` → "v1 exported" |
| 17 | M4 | **M4-6** pull v1, the same queries again | Alan Turing gone, "Bubonic plague (Phase 1)" |
| 18 | M2 | **M2-5** tag phase1, snapshots, time travel | 1000 vs 975, `deleted_in_phase1 = 50` |

### Demos
| # | Who | Do | ✅ |
|---|---|---|---|
| 19 | M3 + M2 | **M3-10** kill an executor during PageRank | `Lost executor`, same fingerprint `54ee091e74b5` |
| 20 | M3 + M4 | **M3-11** broken export, then **M4-7** pull it | `VALIDATION FAILED`, `REFUSED`, still serving v1 |
| 21 | M4 | **M4-8** serve v0, then v1 again | Alan Turing back on v0, gone on v1 |
| 22 | M2 (+ M3, M4) | **M2-6** bad DELETE + rollback; M3 runs `build_index.py --phase 1` | 878 → rollback → 975; index `up-to-date`; search unaffected throughout |
| 23 | M2 + M3 | **M2-7** stop node3's DataNode, read, restart | reads OK, `Dead datanodes (1)`, back to 2 |
| 24 | M3 | **M3-12** where the time and network go | netmeter line + history-server stages screenshot |
| 25 | all | evidence checklist below, then shutdown (M2-8) | |

## Evidence checklist (📸 = the screenshots each guide asks for)

| Member | Screenshots |
|---|---|
| M1 | `fsck` replication of pages.jsonl · Phase 0 ingest summary · rejects query · re-run (same snapshot) · Phase 1 summary · doc_changes query · Phase 1 re-run |
| M2 | `dfsadmin -report` 2 live + NameNode UI · lake report after Phase 0 and after Phase 1 (history) · time-travel queries · bad delete + rollback history · DataNode dead / back |
| M3 | Spark master UI with 2 workers · index (full, then incremental) · PageRank iterations Phase 0 + 1 · export (14 PASS + 2 NLP) · Word2Vec neighbours · fault-tolerance run + grep + UI · broken export · history server stages |
| M4 | UI with v0 results (score bars visible) · 3 NLP twists before/after (stemming, did-you-mean, Word2Vec expansion) · pull v0 · pull v1 + same queries · REFUSED pull · version switch (UI *Versions* panel) |

Also save the logs: `~/ingest-p*.log`, `~/index-p*.log`, `~/pagerank-*.log`, `~/export-*.log`
(they hold Spark's view of each run).

## Rehearsal from a clean start (the day before the demo)

M4 stops uvicorn and empties `/serving` (M4-9) → M2 wipes the pipeline data (M2-8) → rows 11–25 again.
Doing the full run once more, start to finish, is the best viva preparation there is.
