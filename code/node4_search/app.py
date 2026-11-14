"""Node 4 - FastAPI search service + UI.  No Spark, no HDFS daemon, no Iceberg: it only reads the
plain Parquet export that pull_export.py placed in /serving/<version>/.

    cd code/node4_search
    SERVING_DIR=/serving ~/search-venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000

Endpoints
    GET  /                       the search UI (static/index.html)
    GET  /api/search?q=...&k=10  ranked results: doc_id, url, title, snippet, bm25, pagerank, score
    GET  /api/health             which version is loaded and how many docs
    GET  /api/versions           every version in /serving and whether it is valid
    POST /api/admin/reload       re-read /serving/CURRENT and switch to it (if it is valid)
    POST /api/admin/switch/{v}   switch to version v (e.g. back to v0) and update CURRENT

Switching is safe: the new version is fully loaded and verified first, then swapped in with one
assignment under a lock. If loading fails, the old version keeps serving and the API says why.
"""
import os
import sys
import threading
import time

import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "common"))
import textproc  # noqa: E402
from bm25 import compute_bm25, final_scores  # noqa: E402
import serving  # noqa: E402


def _read(path, name):
    p = os.path.join(path, name)
    return pq.ParquetDataset(p).read().to_pandas() if os.path.isdir(p) else None


class LoadedVersion:
    """One export, fully in memory."""

    def __init__(self, name):
        path = os.path.join(serving.SERVING_DIR, name)
        t0 = time.time()
        self.manifest = serving.verify_export(path)      # raises ExportInvalid
        self.name = name
        self.docs = _read(path, "docs").set_index("doc_id")
        self.index = _read(path, "postings").set_index("term").sort_index()
        self.pagerank = self.docs["pagerank"].to_dict()
        self.max_pagerank = float(self.docs["pagerank"].max())
        r = self.manifest["ranking"]
        self.k1, self.b, self.w_bm25, self.w_pr = r["k1"], r["b"], r["w_bm25"], r["w_pagerank"]
        self.load_seconds = round(time.time() - t0, 2)

    def search(self, q, k):
        terms = textproc.tokenize(q)
        q_terms = list(dict.fromkeys(terms))
        bm = compute_bm25(q_terms, self.docs, self.index, self.k1, self.b)
        final = final_scores(bm, self.pagerank, self.max_pagerank, self.w_bm25, self.w_pr)
        max_bm = max(bm.values()) if bm else 1.0
        ranked = sorted(final.items(), key=lambda kv: (-kv[1], kv[0]))
        out = []
        for pos, (doc_id, score) in enumerate(ranked[:k], 1):
            d = self.docs.loc[doc_id]
            out.append({"rank": pos, "doc_id": doc_id, "url": d["url"], "title": d["title"],
                        "snippet": d["snippet"], "score": round(score, 6),
                        "bm25": round(bm[doc_id], 4), "bm25_norm": round(bm[doc_id] / max_bm, 4),
                        "pagerank": round(float(d["pagerank"]), 8),
                        "pagerank_norm": round(float(d["pagerank"]) / self.max_pagerank, 4)})
        return {"terms": terms, "matches": len(final), "results": out}


app = FastAPI(title="Team 32 search")
_lock = threading.Lock()
_state = {"current": None, "last_error": None}


def _switch(name):
    """Load `name` completely, then swap it in. On failure, keep what we have."""
    try:
        new = LoadedVersion(name)
    except Exception as e:   # ExportInvalid, missing files, bad parquet ...
        _state["last_error"] = f"could not load {name}: {e}"
        raise
    with _lock:
        old = _state["current"]
        _state["current"] = new
        _state["last_error"] = None
    print(f"[serving] now serving {name} ({len(new.docs)} docs, loaded in {new.load_seconds}s)"
          + (f", was {old.name}" if old else ""), flush=True)
    return new


@app.on_event("startup")
def _startup():
    name = serving.read_current()
    if name:
        try:
            _switch(name)
        except Exception as e:
            print(f"[serving] startup: {e}", flush=True)


def _need():
    cur = _state["current"]
    if cur is None:
        raise HTTPException(503, f"no version loaded yet ({_state['last_error'] or 'run pull_export.py'})")
    return cur


@app.get("/api/search")
def search(q: str = Query(..., min_length=1), k: int = Query(10, ge=1, le=100)):
    cur = _need()
    t0 = time.time()
    res = cur.search(q, k)
    res.update({"query": q, "version": cur.name, "took_ms": round((time.time() - t0) * 1000, 1)})
    return res


@app.get("/api/health")
def health():
    cur = _state["current"]
    return {"status": "ok" if cur else "no-version", "version": cur and cur.name,
            "docs": cur and len(cur.docs), "docs_snapshot_id": cur and cur.manifest.get("docs_snapshot_id"),
            "current_file": serving.read_current(),
            "last_error": _state["last_error"]}


@app.get("/api/versions")
def versions():
    cur = _state["current"]
    return {"serving": cur and cur.name, "versions": serving.list_versions()}


@app.post("/api/admin/reload")
def reload():
    name = serving.read_current()
    if not name:
        raise HTTPException(409, "CURRENT file is missing")
    cur = _state["current"]
    if cur and cur.name == name:
        return {"switched": False, "version": name, "note": "already serving it"}
    try:
        _switch(name)
    except Exception as e:
        raise HTTPException(409, f"kept {cur and cur.name}: {e}")
    return {"switched": True, "version": name}


@app.post("/api/admin/switch/{name}")
def switch(name: str):
    if "/" in name or name.startswith("."):
        raise HTTPException(400, "bad version name")
    cur = _state["current"]
    try:
        _switch(name)
    except Exception as e:
        raise HTTPException(409, f"kept {cur and cur.name}: {e}")
    serving.write_current(name)
    return {"switched": True, "version": name}


@app.get("/")
def ui():
    return FileResponse(os.path.join(HERE, "static", "index.html"))
