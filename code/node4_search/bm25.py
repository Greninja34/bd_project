"""BM25, based on the course's BM25 snippet, with its two bugs fixed.

Fix 1: the snippet reads row["length"] from index_df, but index_df has no length column, so it
       raises KeyError. Here the doc length comes from docs_df (looked up by doc_id).
Fix 2: the snippet starts every document at 0.0, so docs that match NO query word would still be
       ranked (and pushed up by PageRank). Here only docs with at least one matching term get a score.

Same formula and parameters as the spec (k1 = 1.2, b = 0.75, Lucene IDF that is always > 0):

    IDF(t)   = ln(1 + (N - df + 0.5) / (df + 0.5))
    BM25(D)  = sum over query terms t in D of  IDF(t) * tf*(k1+1) / (tf + k1*(1 - b + b*len(D)/avgdl))

Vectorised with numpy per term (one term's postings at a time) instead of iterrows(); the maths is
identical, just faster.
"""
import math

import numpy as np


def compute_bm25(query_terms, docs_df, index_df, k1=1.2, b=0.75, weights=None):
    """
    query_terms: list of already-tokenized terms (textproc.tokenize)
    docs_df:     pandas DataFrame indexed by doc_id, with column 'length'
    index_df:    pandas DataFrame indexed by term (sorted), columns ['doc_id', 'tf', 'df']
    weights:     optional {term: weight}; default 1.0 per term. Used by the NLP twist "query
                 expansion": Word2Vec neighbours are added with a weight below 1, so they can help
                 a doc but never count as much as a word the user actually typed.
    returns:     {doc_id: bm25_score} for docs matching at least one term
    """
    n_docs = len(docs_df)
    avgdl = max(float(docs_df["length"].mean()), 1.0)
    scores = {}
    for term in dict.fromkeys(query_terms):            # each distinct query term once, keep order
        if term not in index_df.index:
            continue
        rows = index_df.loc[[term]]                     # this term's postings list
        df = int(rows["df"].iloc[0])
        idf = math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))
        tf = rows["tf"].to_numpy(dtype=float)
        doc_len = docs_df["length"].reindex(rows["doc_id"]).to_numpy(dtype=float)   # fix 1
        part = idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (doc_len / avgdl)))
        if weights:
            part = part * weights.get(term, 1.0)
        for doc_id, s in zip(rows["doc_id"], part):
            scores[doc_id] = scores.get(doc_id, 0.0) + float(s)                      # fix 2
    return scores


def final_scores(bm25, pagerank, max_pagerank, w_bm25=0.7, w_pr=0.3):
    """Final = 0.7 * BM25 / max BM25 (over this query's matches) + 0.3 * PR / max PR (over the corpus)."""
    if not bm25:
        return {}
    max_bm25 = max(bm25.values()) or 1.0
    max_pr = max_pagerank or 1.0
    return {d: w_bm25 * (s / max_bm25) + w_pr * (pagerank.get(d, 0.0) / max_pr) for d, s in bm25.items()}


if __name__ == "__main__":
    import pandas as pd
    docs = pd.DataFrame({"doc_id": ["1", "2", "3"], "length": [4, 4, 5]}).set_index("doc_id")
    idx = pd.DataFrame({"term": ["plant", "plant", "soil", "seed"], "doc_id": ["1", "2", "2", "3"],
                        "tf": [1, 1, 1, 1], "df": [2, 2, 1, 1]}).set_index("term").sort_index()
    s = compute_bm25(["plant", "soil"], docs, idx)
    print(s)                                         # doc 3 does not match, so it is not in the result
    print(final_scores(s, {"1": 0.2, "2": 0.5, "3": 0.3}, 0.5))
    assert "3" not in s and s["2"] > s["1"]
