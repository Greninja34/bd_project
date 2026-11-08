# 07: Solo practice: the whole pipeline on your own VM (each member, ~30–45 min)

Same code, same data, same numbers as the team run, but on one machine: Spark in `local[2]`
mode and the Iceberg warehouse in a local folder (no HDFS, no cluster, no ZeroTier). Do it
**before** the team call. It's what lets you explain the parts that don't run on your node.
Everything lives in `~/solo`, and `rm -rf ~/solo` removes it all.

Needs: Java 11, Spark 3.3.4, Python 3.10 (all from the course guides), the Iceberg jar
(setup §8), and, both needing internet once:
- for the search part: the venv from M4-1 (`~/search-venv`);
- for the Word2Vec twist: the numpy venv from setup §8b (`~/nlp-venv`).

## S-1. A local config

```bash
SOLO=~/solo; mkdir -p $SOLO
JAR=/opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar
cat > $SOLO/solo.conf << EOF
spark.master                              local[2]
spark.driver.host                         localhost
spark.driver.memory                       1g
spark.sql.shuffle.partitions              8
spark.default.parallelism                 8
spark.pyspark.python                      /usr/bin/python3.10
spark.pyspark.driver.python               /usr/bin/python3.10
spark.hadoop.fs.defaultFS                 file:///
spark.sql.extensions                      org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg_catalog         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type    hadoop
spark.sql.catalog.iceberg_catalog.warehouse  file://$SOLO/warehouse
spark.jars                                $JAR
spark.driver.extraJavaOptions             -Dderby.system.home=$SOLO/derby
spark.ui.showConsoleProgress              false
EOF
export SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost   # in case setup §7a is already done
cd ~/project_search_engine
```
In every new terminal for this practice: re-run the `SOLO=...`, `export SPARK_LOCAL_IP=...` and `cd` lines.

## S-2. Phase 0, start to finish

```bash
spark-submit --properties-file $SOLO/solo.conf code/node1_ingestion/ingest.py \
    --phase 0 --input file://$HOME/project_search_engine/data/phase-0/pages.jsonl 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf code/node3_compute/build_index.py --phase 0 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf --conf spark.pyspark.driver.python=$HOME/nlp-venv/bin/python \
    code/node3_compute/word2vec.py --phase 0 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf code/node3_compute/pagerank.py --phase 0 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf code/node3_compute/export.py \
    --phase 0 --version v0 --export-root file://$SOLO/exports 2>/dev/null
```
✅ The same numbers as the team guides: 1000 docs, 20 + 20 rejects, 499 links · 37551 terms,
233643 postings, 28485 stems · Word2Vec vocabulary 10448 (king → queen ...) · PageRank fingerprint `60a6221350e7` · `EXPORT v0: OK`, all PASS.
(Each job takes 20–50 s, Word2Vec ~80 s. If one prints nothing, drop the `2>/dev/null` to see the error.)

## S-3. Serve it

Terminal 2:
```bash
SOLO=~/solo; cd ~/project_search_engine/code/node4_search
~/search-venv/bin/python pull_export.py --version v0 --source $SOLO/exports --serving $SOLO/serving --service ''
SERVING_DIR=$SOLO/serving ~/search-venv/bin/uvicorn app:app --port 8000
```
Back in terminal 1 (or open `http://localhost:8000`):
```bash
q() { ~/search-venv/bin/python ~/project_search_engine/code/node4_search/q.py "$@"; }
q "bubonic plague"; q april 6; q "alan turing"
q phases 3 --opts=stem; q "bubnic plauge" 2 --opts=spell; q river 3 --opts=expand
q "speed of light" 1 --opts=smart
```
✅ Bubonic plague (100) first; for "april", April (1) is 5th (see M4-5 for why); Alan Turing (13) first.
The NLP lines: 28 matches for "phases" (12 without stem) · *showing results for bubonic plague* ·
*expanded with danube, yangtze* · "The biggest speed is the speed of light." · Flea 2nd.
In the UI, tick the **NLP extras** boxes one at a time (M4-5b has the full table).

## S-4. Phase 1

```bash
spark-submit --properties-file $SOLO/solo.conf code/node1_ingestion/ingest.py \
    --phase 1 --input file://$HOME/project_search_engine/data/phase-1/changes.json 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf code/node3_compute/build_index.py --phase 1 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf --conf spark.pyspark.driver.python=$HOME/nlp-venv/bin/python \
    code/node3_compute/word2vec.py --phase 1 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf code/node3_compute/pagerank.py --phase 1 2>/dev/null
spark-submit --properties-file $SOLO/solo.conf code/node3_compute/export.py \
    --phase 1 --version v1 --export-root file://$SOLO/exports 2>/dev/null
~/search-venv/bin/python code/node4_search/pull_export.py --version v1 --source $SOLO/exports --serving $SOLO/serving
q "alan turing"; q phase
```
✅ ADDED / UPDATED / DELETED 25 / 25 / 50 → 975 docs · `mode: incremental` · fingerprint
`54ee091e74b5` · `switched: True` · Alan Turing gone, the "Phase 1 Added Page" docs appear for "phase".

## S-5. Poke at it (pick what belongs to your node, then the rest)

```bash
isql() { spark-sql --properties-file $SOLO/solo.conf -e "$1" 2>/dev/null; }
```
| Try | Command | You should see |
|---|---|---|
| idempotency (M1) | re-run S-4's ingest line | `changes merged: 0`, same snapshot id twice |
| the rejects (M1) | `isql "SELECT phase, line_no, doc_id, reason, detail FROM iceberg_catalog.search.rejects ORDER BY phase, line_no"` | 20 unparseable, 20 duplicates, 7 dangling links |
| the lakehouse (M2) | `spark-submit --properties-file $SOLO/solo.conf code/node2_storage/lake_report.py 2>/dev/null` | every table, snapshots, history |
| Iceberg files (M2) | `find $SOLO/warehouse/search/docs \| head -30` | `data/*.parquet`, `metadata/v*.metadata.json`, `snap-*.avro` |
| time travel (M2) | `isql "SELECT count(*) FROM iceberg_catalog.search.docs VERSION AS OF <Phase 0 snapshot id>"` | 1000 (now: 975) |
| bad delete + rollback (M2) | M2-6's commands with `isql` as defined here | 878 → 975 |
| index up to date (M3) | re-run S-4's `build_index.py` line | `mode: up-to-date` |
| broken export (M3, M4) | `spark-submit --properties-file $SOLO/solo.conf code/node3_compute/export.py --phase 1 --version v1-broken --simulate-failure --export-root file://$SOLO/exports 2>/dev/null`, then `pull_export.py --version v1-broken --source $SOLO/exports --serving $SOLO/serving` | 3 FAIL lines; REFUSED |
| version switch (M4) | `curl -s -X POST localhost:8000/api/admin/switch/v0; q "alan turing"` | Alan Turing back |
| the toy set (M1) | the S-1 conf with a different warehouse (`sed "s#$SOLO/warehouse#$SOLO/toy-wh#" $SOLO/solo.conf > $SOLO/toy.conf`), then ingest `data/toy/phase0_toy_pages.jsonl` (phase 0) and `phase1_toy_changes.jsonl` (phase 1) with `--properties-file $SOLO/toy.conf` | 2 rejected (empty / null ids), doc 2's later copy "Soil duplicate" wins, then doc 2 is deleted and both links to it are dropped |

## S-6. Clean up

Ctrl+C the uvicorn terminal, then `rm -rf ~/solo`.
