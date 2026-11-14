# Demo day: what to do and what to show

The spec asks for 6 things: **(1)** phase-based ingestion + dirty records + Iceberg, **(2)** distributed inverted index,
**(3)** PageRank (>= 10 iterations), **(4)** Spark fault tolerance, **(5)** BM25 ranking, **(6)** search service + UI
before and after the change set. This page says when each is shown, by whom, and with which step of [RUNBOOK.md](RUNBOOK.md).

> Do not discover problems on the day. **Do the full RUNBOOK once as a rehearsal 1-2 days before** (S01-S70), then reset (RUNBOOK "start over").

## The day before (every member, 15 min)
1. All 4 machines on, ZeroTier joined, `ping node1..node4` works. node3: one WSL terminal stays open the whole demo.
2. In every terminal: `source ~/bd_project/venv/bin/activate && cd ~/bd_project`.
3. `bash scripts/check.sh setup` and `bash scripts/check.sh network` on every node: both PASSED.
4. Charge laptops, plug in power, disable sleep/suspend. Use the same Wi-Fi/hotspot you rehearsed on.
5. Have a second copy of every screenshot/log in `~/bd_project/state/logs/` (you will need them if something breaks live).

## Before the examiner arrives (cluster up, nothing ingested yet)
Do S18-S28 (format only on a fresh cluster), gates 2 and 3 PASSED, search service running on node4 (S28), UI open at `http://node4:8000` showing "no version loaded".
Open these tabs: Spark master `http://172.23.10.141:8080`, history server `:18080`, HDFS NameNode `http://node2:9870`.

## The live run (about 25-35 min if nothing breaks)

| # | Show | Who | Steps | Say this / point at this |
|---|---|---|---|---|
| 1 | The cluster | M2 + M3 | S22, S25 outputs | 2 live DataNodes (node2, node3), replication 2; master UI with 2 workers ALIVE. "Node1 and node4 run no daemons." |
| 2 | **Phase 0 ingestion** (1) | M1 | S29-S31 | `hdfs fsck`: every block on 2 DataNodes. Summary: 1040 lines, 20 rejected (`unparseable_json`), 20 duplicates superseded, 1000 docs, 499 links. **Re-run: `changes merged: 0`, same snapshot id** = idempotent. Show the `rejects` query. |
| 3 | **Index + PageRank + export** (2, 3) | M3 | S33-S35 | 37,551 terms / 233,643 postings; the `groupBy` is the shuffle. PageRank: 10 iterations, ranks sum to 1.000000, fingerprint `60a6221350e7`. Export: 13 checks PASS, `_SUCCESS` written last. |
| 4 | **Search v0** (5, 6) | M4 | S37-S38 | `pull_export` verifies and switches. Query "bubonic plague", "ocean", "april". Open the UI: score bars (blue BM25, orange PageRank). Explain why April is 5th (formula, no in-links). |
| 5 | Iceberg versioning (1) | M2 | S39 | Tag `phase0` **before** Phase 1. Lake report: 1 snapshot. |
| 6 | **Phase 1** (1, 2, 6) | M1 -> M3 -> M4 | S41-S48 | Ingest: ADDED/UPDATED/DELETED = 25/25/50, 975 docs, 7 dangling links dropped, re-run = no change. Incremental index ("Iceberg says 100 docs changed"), PageRank fingerprint `54ee091e74b5`, export v1. Node4 still serves v0 until the pull; then search "bubonic plague" (now "Phase 1"), "alan turing" (gone). |
| 7 | Time travel (1) | M2 | S49 | `VERSION AS OF 'phase0'` 1000 rows vs 975 now; `deleted_in_phase1 = 50`. |
| 8 | **Spark fault tolerance** (4) | M3 + M2 | S51-S53 | Start the slow PageRank; at iteration 2-3 M2 `kill -9` the executor on node2. Show the job survives, "Lost executor", `FetchFailed`, and **the same fingerprint `54ee091e74b5`**. Say: lineage recomputed the lost partitions. |
| 9 | Failed export never served | M3 + M4 | S54-S55 | `--simulate-failure`: 3 FAIL lines, nothing published; node4 `REFUSED`, still serving v1. |
| 10 | Version switch | M4 | S56 | Back to v0 (Alan Turing returns), forward to v1. Service never stops. |
| 11 | Bad update + recovery (1) | M2 (+M4) | S57-S60 | `DELETE ... LIKE 'C%'` -> 878 rows; search on node4 unaffected; `rollback_to_snapshot` -> 975; index says `up-to-date`. |
| 12 | Optional (if time): DataNode dies | M3 + M2 | S61-S64 | Reads still work, `Under-replicated`, then back to 2.0. |
| 13 | Optional: "where do time and network go" | M3 | S65 | Meter line + history-server stages. Data is tiny; PageRank is latency-bound. |

Order is chosen so the examiner sees the spec's list in sequence; cut items 12-13 first if time is short, never items 2-11.

## Fixed queries to have ready (copy from RUNBOOK S37 / S48 so nobody types live)
`bubonic plague`, `alan turing`, `april`, `ocean`, `computer science`, `phase`, `zzzqqq` (0 matches).

## If something breaks live (stay calm, use this order)
1. Say what you see, then run `bash scripts/doctor.sh` on that node; read the FAIL line, it prints the fix.
2. The stage's gate: `bash scripts/check.sh <stage>` tells which part is wrong.
3. Look up the error in [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
4. A Spark job hangs: check `http://172.23.10.141:8080` (2 workers ALIVE?) and that no other job is running (one job at a time, `spark.cores.max 4`).
5. Fallback: show the saved screenshots/logs of the rehearsal for that step and explain from [VIVA.md](VIVA.md). Never re-format HDFS during the demo.

## Evidence to hand in (each member)
See the checklist at the end of [RUNBOOK.md](RUNBOOK.md): screenshots (📸) and the logs in `~/bd_project/state/logs/`.

## Each member must be able to explain
Their node, their steps' commands, and the matching section in [VIVA.md](VIVA.md) (viva questions at the end). Typical first question to **M4**: BM25 formula + k1/b, the two bugs in the course snippet, the final-score normalisation, how a version switch can't serve a broken export.
