# 04: Member 3, Node 3 (Spark master + worker, HDFS DataNode, index / PageRank / export)

**Your node in one line:** you run the Spark **master** (it hands out cores on the two workers)
and a **worker**, the second HDFS **DataNode**, and the **drivers** of the three compute jobs:
build the inverted index, compute PageRank, and validate + export everything as Parquet for node4.
You also run the Spark fault-tolerance demo.

**What you must be able to explain:** master vs worker vs driver vs executor; what the index's
`groupBy` shuffles; incremental indexing from Iceberg snapshots; the PageRank formula and why
dangling rank is spread evenly; lineage and how Spark recovers a lost executor; the export
checks and why the publish is atomic. **NLP twists (yours on the Spark side):** the stemmed
index, Word2Vec (what a word vector is, skip-gram, why it runs in Spark MLlib), TF-IDF keywords.
The code: [`build_index.py`](../code/node3_compute/build_index.py),
[`word2vec.py`](../code/node3_compute/word2vec.py),
[`pagerank.py`](../code/node3_compute/pagerank.py), [`export.py`](../code/node3_compute/export.py),
and the stemmer in [`common/textproc.py`](../code/common/textproc.py).

Before you start: [`01-SETUP-ALL-MEMBERS.md`](01-SETUP-ALL-MEMBERS.md) §0–§9 done on node3,
**including §8b** (the numpy venv for Word2Vec). Your VM should have the most RAM (4–6 GB).

---

## M3-1. HDFS DataNode on node3  (after Member 2's M2-1 "NameNode up")

```bash
jps                                         # nothing running (setup §2)
rm -rf ~/hadoop-data                        # the old single-node HDFS: its cluster ID would be rejected
mkdir -p ~/hadoop-data/datanode ~/hadoop-data/tmp
hdfs --daemon start datanode
jps
```
- **No NameNode here, and no format.** This DataNode registers with node2's NameNode and gets that
  cluster's ID. An old data dir would carry the old cluster ID, and the log would say `Incompatible clusterIDs`.

✅ `jps` shows `DataNode`; Member 2 sees `Live datanodes (2)` (M2-2).

## M3-2. Spark master and worker

```bash
start-master.sh
sleep 5; ss -tlnp | grep -E ':7077|:8080'
start-worker.sh spark://node3:7077
jps
```
- The master only keeps track of the workers and hands out executors. The **workers** start the
  executor JVMs that run the tasks. A **driver** (your `spark-submit`, or Member 1's) asks the master for executors.

✅ 7077 is bound to your **ZeroTier IP** (not 127.x); `jps` shows `Master`, `Worker`, `DataNode`.
Announce **"master up"**. After Member 2's M2-3, `http://node3:8080` shows **2 workers ALIVE**. 📸 screenshot.

## M3-3. History server (optional, recommended for "where does the time go")

```bash
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://node2:9000/spark-logs" start-history-server.sh
```
`http://node3:18080` lists every **finished** application with its jobs, stages, durations and
shuffle sizes (the live UI on port 4040 disappears when a job ends).

## M3-4. Cluster check from node3  (after Member 1's M1-2)

```bash
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/tools/cluster_check.py 2>/dev/null
```
✅ tasks on node2 **and** node3, Python 3.10, and `check.hello` now has **2 rows**
(`node1`, `node3`): two drivers on two machines see one Iceberg table on HDFS.
Then clean up: `spark-submit --properties-file ~/spark-config/search.conf code/tools/cluster_check.py --drop 2>/dev/null`

---

## Phase 0 (after Member 1 says "Phase 0 ingested")

## M3-5. Build the inverted index

```bash
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/build_index.py --phase 0 2> ~/index-p0.log
```
✅ (📸 screenshot)
```
mode: full
docs in index: 1000
distinct terms: 37551
postings (term, doc) rows: 233643
stemmed index: distinct stems / postings: 28485 / 209719
avg / max doc length (terms): 461.0 / 13903
```
How it works: a Python UDF (on the executors) runs `textproc.doc_terms(title, text)`: strip wiki
markup, lowercase, split, drop stopwords. Then `explode` turns each doc into one row per word,
and `groupBy(term, doc_id).count()` gives `tf`. That **groupBy is the shuffle**: every executor
sends each (term, doc) pair to the executor that owns that term's partition. `length` = number of
terms per doc (BM25's `len(D)`). `df` is computed later, in the export.

**NLP twist, the stemmed index:** the same terms also go through the Porter stemmer (in
`textproc.py`, pure Python, so it runs on the executors) and are grouped a second time into
`search.postings_stem`. 37,551 distinct words collapse to 28,485 stems ("phases", "phased",
"phasing" → "phase"). Both indexes have the same doc lengths (one stem per word), so `doc_stats`
serves both. The job also stores `body`, the cleaned prose of each doc, for Member 4's smart snippets.

## M3-6b. Word2Vec (NLP twist, ~1–2 min)

```bash
spark-submit --properties-file ~/spark-config/search.conf \
    --conf spark.pyspark.driver.python=$HOME/nlp-venv/bin/python \
    code/node3_compute/word2vec.py --phase 0 2> ~/word2vec-p0.log
```
✅ (📸 screenshot; the neighbours vary slightly from run to run, but should look like this):
```
training tokens: 461002
vocabulary (min count 5): 10448
vector size / window / iterations / partitions: 100 / 5 / 5 / 2
  king: queen 0.76, consort 0.75, ...
  planet: jupiter 0.80, saturn 0.80, planets 0.80, moons 0.78
  river: danube 0.74, yangtze 0.72, mississippi 0.71, mekong 0.71
  computer: hardware 0.84, computers 0.80, program 0.79, programs 0.75
```
- The executors tokenize the docs (the same `doc_terms` as the index). **Spark MLlib's Word2Vec**
  (skip-gram) then trains a 100-number vector per word over 2 partitions on the workers. Words that
  appear in similar contexts end up with similar vectors.
- The driver then takes the cosine similarity of every word with every other word (numpy, in
  chunks of 1000) and keeps the 5 nearest per word → `search.synonyms`. Node 4 uses them for `expand=1`.
- `--conf spark.pyspark.driver.python=...nlp-venv...`: only the **driver** needs numpy (`pyspark.ml`
  imports it); the executors keep plain `/usr/bin/python3.10`.
- Word2Vec is not reproducible run to run (random init + parallel updates), so the neighbours
  differ slightly between runs. That's why the export only *warns* if it is stale.

## M3-6. PageRank

```bash
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/pagerank.py --phase 0 --iterations 10 2> ~/pagerank-p0.log
```
✅ (📸 screenshot)
```
[pagerank] N=1000 docs, 499 links, 499 docs have out-links, 501 are dangling; d=0.85, 10 iterations
[pagerank] iter  1: sum=1.000000  L1 change=4.250e-01 ...
...
[pagerank] iter 10: sum=1.000000  L1 change=9.650e-04 ...
sum of ranks: 1.000000
max / min rank: 0.001752 / 0.000264  (max/min = 6.65)
docs tied at the minimum: 501
result fingerprint: 60a6221350e7
```
- Each iteration: `links.join(ranks)` → each doc sends `rank / outdegree` to every doc it links
  to → `reduceByKey` adds them up → `(1-d)/N + d * (incoming + dangling/N)`.
- **Dangling** = rank of docs with no out-link (501 of them). Spreading it evenly keeps the total at 1.
- Exactly the same `fingerprint` on your cluster means exactly the same ranks as our test run.
  Keep the number: the fault-tolerance demo (M3-10) must produce it again.

## M3-7. Validate and export v0

```bash
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/export.py \
    --phase 0 --version v0 --export-root hdfs://node2:9000/search/exports 2> ~/export-v0.log
hdfs dfs -ls -R /search/exports/v0
hdfs dfs -cat /search/exports/v0/manifest.json | head -30
```
✅ 14 blocking `PASS ...` lines, 2 more `PASS [nlp] ...` lines (keywords for every doc, synonyms
from the current snapshot), and `EXPORT v0: OK`. In HDFS: `docs/` (~2.5 MB: now with the cleaned
`body` and `keywords`), `postings/` (~850 KB), `postings_stem/` (~720 KB), `synonyms/` (~600 KB),
`manifest.json`, `_SUCCESS`. 📸 screenshot.
Announce **"v0 exported"**. Member 4 pulls it (M4-4).

**NLP twist, keywords:** for every doc, the export picks the 5 terms with the highest TF-IDF =
`(tf / length) · ln(N / df)`: frequent in this doc, rare in the corpus. It's a distributed window
function (`row_number() over (partition by doc_id order by tfidf desc)`). Terms must be alphabetic,
3+ letters, in ≥ 2 docs. Node 4 shows them as tags.

How it stays safe: everything is written to `/search/exports/_tmp_v0_<time>/` first. The blocking
checks run (row counts match Iceberg, no empty/duplicate ids, every doc has a length and a
PageRank, PageRank sums to 1, no posting points at a missing doc, index **and** PageRank were
computed from the **current** docs snapshot, ≥ 10 iterations, no orphans in the stemmed index).
The 2 `[nlp]` checks only print WARN: a stale or missing Word2Vec model disables one optional
feature, which is no reason to block a search version. Only if all blocking checks pass, `manifest.json`
(status OK) and `_SUCCESS` are written, and the folder is **renamed** to `v0`. An HDFS rename is
atomic, so node4 can never see a half-written export.

---

## Phase 1 (after Member 1 says "Phase 1 ingested")

## M3-8. Update the index incrementally

```bash
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/build_index.py --phase 1 2> ~/index-p1.log
```
✅ (📸 screenshot)
```
[index] Iceberg says 100 docs changed since snapshot <Phase 0 id>: {'UPDATED': 25, 'DELETED': 50, 'ADDED': 25}
mode: incremental
docs in index: 975
distinct terms: 36521
postings (term, doc) rows: 220622
stemmed index: distinct stems / postings: 27706 / 198039
```
The job looked up the docs snapshot it indexed last time (in `pipeline_runs`), asked Iceberg for
the diff between that snapshot and the current one (`snapshot_diff` = time travel + hash compare),
`DELETE`d the postings (both indexes) of those 100 docs, and added new postings for the 50 that still exist.
We checked that this gives **exactly** the same table as a full rebuild (same checksum).
Run it again: `mode: up-to-date` (idempotent).

## M3-9. Word2Vec, PageRank and export v1

```bash
spark-submit --properties-file ~/spark-config/search.conf \
    --conf spark.pyspark.driver.python=$HOME/nlp-venv/bin/python \
    code/node3_compute/word2vec.py --phase 1 2> ~/word2vec-p1.log
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/pagerank.py --phase 1 2> ~/pagerank-p1.log
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/export.py \
    --phase 1 --version v1 --export-root hdfs://node2:9000/search/exports 2> ~/export-v1.log
```
✅ PageRank: `N=975 docs, 467 links`, `max/min = 14.08`, `docs tied at the minimum: 548`,
`fingerprint: 54ee091e74b5` · Word2Vec: vocabulary ~10126 · export: all PASS, `docs / postings / terms: 975 / 220622 / 36521`.
(If you skip Word2Vec here, the export still succeeds with `WARN [nlp] synonyms built from the
current docs snapshot`: query expansion then uses the Phase 0 model.)
Announce **"v1 exported"**. 📸 screenshots.

PageRank is recomputed in full (not incrementally): one changed link can change every rank, and it takes about a minute.

---

## Demos

## M3-10. Spark fault tolerance: kill an executor in the middle of PageRank  (with Member 2)

`--slow-seconds 3` makes every task sleep 3 s, so the job lasts ~2 minutes and you have time to
kill something while tasks are running. The result is the same as without it.

```bash
# [M3 on node3] terminal 1
cd ~/project_search_engine
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/pagerank.py \
    --phase 1 --slow-seconds 3 2> ~/pagerank-fail.log
```
```bash
# [M2 on node2] when M3's screen shows "iter 2" or "iter 3":
jps | grep CoarseGrainedExecutorBackend        # the executor JVM of this job on node2
kill -9 <that pid>
```
```bash
# [M3] terminal 2, after the job finished
grep -E "Lost executor|ExecutorLostFailure|No more replicas|FetchFailed" ~/pagerank-fail.log | cut -c1-160 | head -12
```
✅ (📸 screenshot the job output + the grep + the application's page on `http://node3:8080`):
- the iteration during which you killed it takes much longer (in our test: 19 s instead of 7 s),
  then the iterations continue normally;
- the log shows `Lost executor <n> on node2`, `Lost task ... ExecutorLostFailure`, `No more replicas
  available for rdd_...` (cached partitions lost) and `FetchFailed` (shuffle files lost);
- on the master UI, the killed executor is listed as *EXITED/KILLED*; the master then normally
  asks node2's worker (still alive) for a replacement executor with a new ID;
- `sum of ranks: 1.000000` and **`result fingerprint: 54ee091e74b5`**, identical to the clean run.

What Spark did: the cached `ranks` partitions and the shuffle output on that executor were lost.
The scheduler noticed (`ExecutorLostFailure` / `FetchFailed`), re-ran the parent stages that had
produced the missing data, using the **lineage** (the recorded chain of transformations: links ⋈
ranks → flatMap → reduceByKey → ...), and then retried the failed tasks. Nothing was checkpointed
or replicated. The data was simply recomputed from its inputs, which still exist in Iceberg/HDFS.

**Variant B (stronger):** instead of `kill -9`, Member 2 runs `stop-worker.sh` on node2. Then the
whole worker is gone, the job finishes on node3's worker alone (slower), with the same fingerprint.
Then `start-worker.sh spark://node3:7077` again.

## M3-11. An export that fails validation never reaches the search  (with Member 4)

```bash
spark-submit --properties-file ~/spark-config/search.conf code/node3_compute/export.py \
    --phase 1 --version v1-broken --simulate-failure --export-root hdfs://node2:9000/search/exports 2> ~/export-broken.log
echo "exit code $?"
hdfs dfs -ls /search/exports
```
✅ `FAIL every doc has a PageRank: 5 missing`, `FAIL PageRank sums to 1: 0.998...`, `FAIL every
posting points at an exported doc: 1 orphan postings`, `VALIDATION FAILED - nothing published`,
`exit code 1`; HDFS has `_failed_v1-broken_<time>` but **no** `v1-broken`. Member 4 tries to
pull it (M4-7) and is refused; node4 keeps serving v1. 📸 screenshot.

## M3-12. Where the time and network traffic go

Run one stage under the meter (on the driver node; for all traffic, also run `netmeter.sh sleep 120` on node2 at the same time):
```bash
bash code/tools/netmeter.sh spark-submit --properties-file ~/spark-config/search.conf \
    code/node3_compute/pagerank.py --phase 1 2>/dev/null | tail -3
```
Then open `http://node3:18080` → the application → *Stages*: sort by *Duration*, and look at
*Shuffle Read / Write*. What you'll find (and what to say):
- The data is tiny (10 MB of input, a 1 MB export). Bandwidth is never the problem.
- **PageRank is latency-bound:** 10 iterations × several small jobs × a shuffle each. Each
  stage costs scheduling + network round trips over ZeroTier (more if RELAYED). That's why it
  takes longer than the much bigger tokenizing work of the index.
- The index's big cost is CPU (regex over 10 MB of wiki markup) plus one shuffle of ~230 k (term, doc) pairs.
- That's why we use 8 shuffle partitions instead of 200, and why fewer, bigger tasks would win on a slow link.

---

## If it breaks

| Symptom | Fix |
|---|---|
| `Initial job has not accepted any resources` | 2 workers ALIVE on :8080? another app running? ufw / `SPARK_LOCAL_IP` (setup §5, §7a) |
| export: `FAIL index built from the current docs snapshot` | Iceberg `docs` changed after the index ran (a new ingest or a rollback): run `build_index.py` and `pagerank.py` again, then export |
| `.../v0 already exists` | add `--overwrite` (the old one is kept as `_replaced_v0_<time>`) or use a new version name |
| `ModuleNotFoundError: textproc` on an executor | run from `~/project_search_engine` with the paths shown; the jobs ship `code/common/*.py` themselves |
| `word2vec.py`: `No module named 'numpy'` | setup §8b not done, or the `--conf spark.pyspark.driver.python=...` line is missing |
| a job is very slow | `sudo zerotier-cli peers`: RELAY between node2 and node3? Try another network / hotspot |
