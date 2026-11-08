# 05: Member 4, Node 4 (Search service + UI)

**Your node in one line:** you run the FastAPI search service and its web UI. It loads one
**exported, validated Parquet version** (`/serving/vN/`) into memory, scores queries with
**BM25** and combines that with the precomputed **PageRank**. No Spark, no HDFS daemon, no Iceberg
here. You pull exports from HDFS as a plain client, and switch versions only when they are complete and valid.

**What you must be able to explain:** the BM25 formula and its parameters (k1 = 1.2, b = 0.75);
the two bugs in the course snippet and how we fixed them; the final score and its normalisation;
why PageRank pushes "April" down to rank 5; how a version switch can't serve a broken or
half-copied export. **NLP twists (you demo all of them):** stemming, edit-distance spelling
correction, Word2Vec expansion with weights, query-aware snippets, sentence-embedding rerank.
The code: [`app.py`](../code/node4_search/app.py), [`bm25.py`](../code/node4_search/bm25.py),
[`nlp.py`](../code/node4_search/nlp.py), [`semantic.py`](../code/node4_search/semantic.py),
[`serving.py`](../code/node4_search/serving.py), [`pull_export.py`](../code/node4_search/pull_export.py),
[`static/index.html`](../code/node4_search/static/index.html), and the tokenizer shared with Spark,
[`common/textproc.py`](../code/common/textproc.py).

Before you start: [`01-SETUP-ALL-MEMBERS.md`](01-SETUP-ALL-MEMBERS.md) §0–§6 and §9 on node4
(§7–§8 are Spark/Iceberg: **not needed on node4**). Give the VM **3 GB** RAM (MiniLM needs ~500 MB).

---

## M4-1. Python environment and the serving folder  (do this at home: it needs internet)

```bash
sudo apt install -y python3-venv
python3 -m venv ~/search-venv
~/search-venv/bin/pip install -r ~/project_search_engine/code/node4_search/requirements.txt
~/search-venv/bin/python -c "import fastapi, uvicorn, pandas, pyarrow, fastembed; print('deps ok')"
sudo mkdir -p /serving && sudo chown "$USER":"$USER" /serving
cd ~/project_search_engine/code/node4_search && ~/search-venv/bin/python bm25.py
~/search-venv/bin/python semantic.py --download
```
- A venv keeps FastAPI/pandas away from the system Python. We always call `~/search-venv/bin/python`
  explicitly, so there's no doubt which Python runs. (Node 4 can use Ubuntu's Python 3.12; the
  3.10 pin is only for PySpark.)
- `bm25.py`'s self-test scores a 3-doc toy index. Doc 3 matches no query word, so it's **not** in the result (bug fix 2).
- `fastembed` (NLP twist, semantic rerank) runs the MiniLM model with **ONNX Runtime**: no
  PyTorch, ~50 MB of packages. `semantic.py --download` fetches the model (~90 MB) into
  `~/.cache/fastembed` **now**, so on demo day it works with no internet.

✅ `deps ok`, the self-test prints two dicts without an `AssertionError`, and
`model sentence-transformers/all-MiniLM-L6-v2 ready in /home/<you>/.cache/fastembed`.

## M4-2. HDFS client works  (after Member 2's M2-2)

```bash
hdfs dfs -ls /search
```
✅ `/search/exports` and `/search/input`. Node 4 talks to the NameNode (node2:9000) and the
DataNodes (node2/node3:9866) as a **client only**. `jps` stays empty.

## M4-3. Start the service  (terminal 1, keep it open for the whole session)

```bash
cd ~/project_search_engine/code/node4_search
SERVING_DIR=/serving ~/search-venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
```
In terminal 2:
```bash
curl -s localhost:8000/api/health; echo
```
✅ `{"status":"no-version", ... "last_error":null}`. Open `http://localhost:8000` in node4's
browser (or `http://node4:8000` from any team VM): the badge says *no version loaded*.

A query helper for terminal 2 (one line per result; `q "words" [k]`; paste it into each new terminal):
```bash
q() { ~/search-venv/bin/python ~/project_search_engine/code/node4_search/q.py "$@"; }
```
The raw API is just as usable: `curl -s "localhost:8000/api/search?q=ocean&k=3" | python3 -m json.tool`.

---

## Phase 0

## M4-4. Pull v0 and switch to it  (after Member 3 says "v0 exported")

```bash
cd ~/project_search_engine/code/node4_search
~/search-venv/bin/python pull_export.py --version v0 --source hdfs://node2:9000/search/exports --serving /serving
ls -la /serving /serving/v0; cat /serving/CURRENT
```
✅ (📸 screenshot)
```
+ hdfs dfs -test -e hdfs://node2:9000/search/exports/v0/_SUCCESS
+ hdfs dfs -get hdfs://node2:9000/search/exports/v0 /serving/.incoming-v0
verified v0: 1000 docs, 233643 postings, 16/16 checks passed, docs snapshot <Phase 0 id>
embeddings: 1000 docs embedded with sentence-transformers/all-MiniLM-L6-v2 in ~25s
CURRENT: None -> v0
service: {'switched': True, 'version': 'v0'}
```
and terminal 1 prints `[serving] now serving v0 (1000 docs, loaded in ...s, nlp {'stem': True,
'spell': True, 'expand': True, 'smart': True, 'rerank': True})`.

What `pull_export.py` did, in order (each step can stop the switch):
1. `_SUCCESS` exists in HDFS? It's the last file Member 3's export writes, so "no `_SUCCESS`" = not finished.
2. `hdfs dfs -get` into `/serving/.incoming-v0` (a hidden name, so nothing loads it half-copied).
3. Local verification: manifest status OK, all blocking checks OK, every file's size equals the manifest, Parquet row counts equal the manifest.
   Then (NLP twist) it embeds every doc (title + start of the body) with MiniLM → `embeddings.npy`.
   This runs **once per version, here**, not per query. If it fails, only `rerank=1` is off.
4. Rename `.incoming-v0` → `v0` (atomic on one disk).
5. `CURRENT` ← `v0` (written to a temp file, then renamed), then `POST /api/admin/reload`. The
   service loads **and verifies again** the complete new version *before* swapping it in under a
   lock. If that fails, it keeps the old one and the script puts `CURRENT` back.

## M4-5. Search the Phase 0 corpus

```bash
q "bubonic plague"; q "alan turing"; q april; q ocean; q "computer science"; q "solar system planets"; q zzzqqq
```
✅ (📸 screenshot the UI for two queries + the terminal):

| query | v0 top results |
|---|---|
| bubonic plague | **Bubonic plague** (100) 1.000 · Flea (3523) 0.622 · London (460) 0.495 |
| alan turing | **Alan Turing** (13) 0.907 · Mathematics (467) · List of mathematicians (3433) |
| april | March (468) 0.870 · May (469) 0.807 · Time limit (828) · … · **April (1) is 5th** |
| ocean | Indian Ocean (1978) 1.000 · Arctic Ocean (1980) · Pacific Ocean (1975) |
| computer science | **Computer science** (110) 1.000 · 1 (number) (571) · Computer (112) |
| zzzqqq | 0 matches, empty list (no doc gets a score just for its PageRank) |

How a result is scored (`bm25.py`, `app.py → LoadedVersion.search`):
- The query goes through **the same `textproc.tokenize`** the Spark index used (lowercase, stopwords removed).
- `IDF(t) = ln(1 + (N - df + 0.5)/(df + 0.5))`; for each doc with the term:
  `IDF · tf·(k1+1) / (tf + k1·(1 - b + b·len/avgdl))`, summed over the query's distinct terms.
- `final = 0.7 · BM25/max BM25 (this query's matches) + 0.3 · PR/max PR (whole corpus)`.
  In the UI, the bar under each result shows the two parts (blue BM25, orange PageRank).

**Why is April 5th for "april"?** April has the best BM25 (`bm25 = 1.000`) but nobody links to it,
so `pr = 0.15`: `0.7·1.0 + 0.3·0.15 = 0.745`. March has `bm25 = 0.815` and an in-link, so `pr = 1.0`:
`0.7·0.815 + 0.3 = 0.870`. Our link graph is so sparse that normalised PageRank is
almost binary (501 docs at 0.15, 490 at 1.0), so "has an in-link" is worth +0.255. Good viva material:
it's the spec's formula, working as defined on this data.

## M4-5b. The NLP twists, one by one (on v0)

Every switch is a checkbox in the UI (**NLP extras** row) and a URL parameter. Show each query
first with everything off (the spec's ranking), then with the switch on. The `q.py` helper takes
`--opts=`:
```bash
q() { ~/search-venv/bin/python ~/project_search_engine/code/node4_search/q.py "$@"; }
```
```bash
q phases 3;  q phases 3 --opts=stem
q "bubnic plauge" 2;  q "bubnic plauge" 2 --opts=spell
q river 3 --opts=expand;  q computer 3 --opts=expand;  q king 3 --opts=expand
q "speed of light" 1;  q "speed of light" 1 --opts=smart
q "disease spread by fleas" 6;  q "disease spread by fleas" 6 --opts=rerank
q "disease spred by fleas" 3 --opts=stem,spell,expand,smart,rerank
```
(or the UI: `http://localhost:8000/?q=phases&stem=1` and so on; the checkboxes stay in the URL)

| Switch | Show | Expected (measured on v0) | What to say |
|---|---|---|---|
| **stemming** `stem=1` | "phases" | 12 → 28 matches (on v1: 12 → 75, the Phase 1 pages say "phase") | Porter stemmer (1980) in `textproc.py`; Spark built a 2nd index of stems (`postings_stem`); the query is stemmed the same way |
| **did you mean** (always) / **auto-correct** `spell=1` | "bubnic plauge", "computr sciense" | 0 results + *Did you mean bubonic plague?* → with `spell=1`: Bubonic plague first | Norvig-style: all strings 1–2 edits away, keep those in the index vocabulary, pick the most frequent (highest df). Known words are never changed |
| **Word2Vec expansion** `expand=1` | "river", "computer", "king" | chips *danube, yangtze* / *hardware, computers* / *queen, consort*; more matches | neighbours from Member 3's Spark MLlib model, added at weight 0.3 × similarity. "king" → *Queen* can move up: that's **query drift**, the price of recall |
| **smart snippets** `smart=1` | "speed of light" | "*The biggest **speed** is the **speed** of **light**.*" instead of the article's first line | best sentence = most distinct query words; highlights by stem when stemming is on. The keyword tags (TF-IDF, from Spark) are always shown |
| **semantic rerank** `rerank=1` | "disease spread by fleas", "animals that live in the sea" | *Flea* 6th → 2nd; *Fish* to 1st; a `semantic` score next to the spec score | the top 20 by the spec score get `0.5 · spec + 0.5 · cosine(query, doc)` with MiniLM sentence embeddings. Meaning, not just shared words |

All on together: "disease spred by fleas" + all 5 boxes → *showing results for disease spread by
fleas*, expanded with *fever, diseases, bite, flea*, then *Bubonic plague* and *Flea* on top with
highlighted sentences, ~50 ms. 📸 screenshot the UI for 3 of these (before/after).

`curl -s localhost:8000/api/health` shows which twists the loaded version supports (`"nlp": {...}`).
If one says `false`, its checkbox is greyed out: e.g. `rerank` if fastembed is missing,
`expand` if Member 3 didn't run `word2vec.py`.

---

## Phase 1

## M4-6. Pull v1, and the same queries again  (after "v1 exported")

```bash
q april; q "alan turing"     # before, still v0
~/search-venv/bin/python pull_export.py --version v1 --source hdfs://node2:9000/search/exports --serving /serving
q "bubonic plague"; q "alan turing"; q april; q phase; q "computer science"
curl -s "localhost:8000/api/search?q=phases&k=3&stem=1" | python3 -m json.tool | grep -E '"(matches|title)"'
```
✅ `CURRENT: v0 -> v1`, `switched: True`, 975 docs (📸 screenshot):

| query | v1 (after the change set) |
|---|---|
| bubonic plague | **Bubonic plague (Phase 1)** (100): the UPDATED doc, new title |
| alan turing | Alan Turing (13) is **gone** (DELETED in Phase 1); top is now List of mathematicians (3433) |
| april | 66 matches instead of 71; April (1) is gone |
| phase | 69 matches (was 20): the 25 ADDED pages ("Phase 1 Added Page N") and the 25 updated ones now match |
| phases, `stem=1` | 75 matches (12 without stemming): Phases of the Moon, Contact network (Phase 1), Phase 1 Added Page 1 |
| computer science | 174 matches; "Computer science (Phase 1)" (110) |

The service never stopped: queries kept working during the switch (the swap is one assignment under a lock).

## M4-7. A broken export is refused  (after Member 3's M3-11)

```bash
~/search-venv/bin/python pull_export.py --version v1-broken --source hdfs://node2:9000/search/exports --serving /serving
echo "exit code $?"; curl -s localhost:8000/api/health; echo
```
✅ `REFUSED: .../v1-broken/_SUCCESS not found - export missing or not finished. Still serving v1.`,
`exit code 1`, health still `"version":"v1"`. 📸 screenshot. This is the spec's "keep the previous
search version available if validation or export fails". Here the failed export never even
got published. If a copy had arrived damaged, step 3 (local verification) would refuse it the same way.

## M4-8. Roll the search back to v0, and forward again

In the UI: *Versions on this node* → **serve this** next to v0. Or:
```bash
curl -s -X POST localhost:8000/api/admin/switch/v0; echo; q "alan turing"
curl -s -X POST localhost:8000/api/admin/switch/v1; echo; q "alan turing"
curl -s localhost:8000/api/versions | python3 -m json.tool
```
✅ "Alan Turing" is back on v0 and gone on v1; `/api/versions` lists v0 and v1 as valid, with
their Iceberg snapshot ids. Each version on node4 corresponds to one Iceberg snapshot of the lakehouse.

## M4-9. Reset (for a clean rehearsal)

Stop uvicorn (Ctrl+C), then `rm -rf /serving/* /serving/.[!.]*`, and start it again (M4-3).

---

## If it breaks

| Symptom | Fix |
|---|---|
| `hdfs: command not found` | `source ~/.bashrc` (the Hadoop guide's PATH) |
| `-get` hangs or `Connection refused node3:9866` | ufw on node2/node3 (setup §5), or that DataNode is down (`doctor.sh`) |
| `embeddings: fastembed not installed` / `rerank: no embeddings` | `pip install -r requirements.txt` again (M4-1); then re-pull the version (`pull_export.py` embeds on pull) |
| first rerank query is slow | MiniLM loads in the background at startup (~10 s); later queries take ~20 ms |
| `service not reachable` in pull_export | uvicorn isn't running. Start it (M4-3); it loads `CURRENT` on startup |
| `409 kept v0: ...` | the service refused the new version; the message says which check failed |
| Can't use HDFS from node4 at all (last resort) | Member 3: `hdfs dfs -get /search/exports/v1 ~/v1 && scp -r ~/v1 <m4-user>@node4:~/incoming/` → you: `pull_export.py --version v1 --source ~/incoming` |

**Viva quick answers**
- *Bug 1 in the course snippet?* It reads `row["length"]` from the index, which has no length
  column → `KeyError`. We look the length up in `docs_df` by `doc_id` (`reindex`).
- *Bug 2?* It starts **every** doc at 0.0, so with `+ 0.3·PageRank` docs that match nothing would
  rank. We only score docs containing at least one query term.
- *Why max BM25 per query but max PageRank over the corpus?* BM25 depends on the query; its scale
  changes per query. PageRank is a fixed property of the corpus.
- *Why load everything into memory?* 975 docs and 220 k postings are a few tens of MB in pandas.
  A query takes a few milliseconds and needs no Spark.
- *Repeated query words?* Counted once (`dict.fromkeys`), the usual BM25 practice.
- *Why are the NLP twists off by default?* The spec fixes the ranking formula. With everything off
  we return exactly that; each twist is an add-on you can switch on and compare.
- *Why rerank only the top 20?* Embedding similarity is good at meaning but bad at "must contain
  this word"; BM25 finds the candidates, MiniLM reorders them. It also keeps a query at ~20 ms.
- *Why fastembed and not an API?* Offline on demo day, no keys, no rate limits; ONNX Runtime avoids
  PyTorch (~1–2 GB). The docs are embedded once per version, so a query costs one small embedding.
- *Why does spell-correction use df?* Among candidates at the same edit distance, the more common
  word is the likelier intent, and it is guaranteed to have results.
