"""NLP twist: semantic re-ranking with a small sentence-embedding model (all-MiniLM-L6-v2, 384 dims).

Runs with fastembed (ONNX Runtime): no PyTorch, ~90 MB model, ~500 MB RAM, CPU only, offline once
the model is downloaded (do that at home: `python semantic.py --download`).

    embed_version(path)   once per version, in pull_export.py: embeds title + start of the body of
                          every doc -> <version>/embeddings.npy + embeddings.json (doc order, model)
    Reranker              in the service: embeds the query, cosine with the top-N candidates

Everything here is optional: if fastembed or the embeddings are missing, `rerank=1` is just
reported as unavailable and the spec ranking is used.
"""
import json
import os
import sys
import time

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
CACHE = os.path.expanduser(os.environ.get("FASTEMBED_CACHE", "~/.cache/fastembed"))
_model = None


def available():
    try:
        import fastembed  # noqa: F401
        return True
    except ImportError:
        return False


def model():
    global _model
    if _model is None:
        from fastembed import TextEmbedding
        _model = TextEmbedding(MODEL, cache_dir=CACHE)
    return _model


def doc_text(title, body):
    return f"{title}. {(body or '')[:1500]}"


def embed_version(path):
    """Write embeddings.npy (float32, unit length, one row per doc) next to the export's docs/."""
    import numpy as np
    import pyarrow.parquet as pq
    t0 = time.time()
    docs = pq.ParquetDataset(os.path.join(path, "docs")).read(columns=["doc_id", "title", "body"]).to_pandas()
    vecs = np.vstack(list(model().embed([doc_text(t, b) for t, b in zip(docs.title, docs.body)],
                                        batch_size=64))).astype("float32")
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True) + 1e-9
    np.save(os.path.join(path, "embeddings.npy"), vecs)
    with open(os.path.join(path, "embeddings.json"), "w") as f:
        json.dump({"model": MODEL, "dim": int(vecs.shape[1]), "doc_ids": docs.doc_id.tolist()}, f)
    return len(docs), round(time.time() - t0, 1)


class Reranker:
    def __init__(self, path):
        import numpy as np
        with open(os.path.join(path, "embeddings.json")) as f:
            meta = json.load(f)
        self.vecs = np.load(os.path.join(path, "embeddings.npy"))
        self.row = {d: i for i, d in enumerate(meta["doc_ids"])}
        if self.vecs.shape[0] != len(self.row):
            raise ValueError("embeddings.npy and embeddings.json disagree")

    def similarities(self, query, doc_ids):
        """cosine(query, doc) for each doc id (embeddings are unit vectors -> a dot product)."""
        q = next(iter(model().query_embed([query])))
        q = q / (float((q ** 2).sum()) ** 0.5 + 1e-9)
        return {d: float(self.vecs[self.row[d]] @ q) for d in doc_ids if d in self.row}


if __name__ == "__main__":
    if "--download" in sys.argv:
        model()
        print(f"model {MODEL} ready in {CACHE}")
    elif len(sys.argv) > 1:
        print(embed_version(sys.argv[1]))
