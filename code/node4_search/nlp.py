"""NLP twists on Node 4: spelling correction, Word2Vec query expansion, query-aware snippets.
All pure Python over data that is already in the export. Each one is switched on per query
(see app.py); with everything off the service behaves exactly as the spec says.
"""
import re
import string

import textproc

_LETTERS = string.ascii_lowercase + string.digits


# ---- twist: "did you mean" -------------------------------------------------------------------
def _edits1(w):
    """Every string one edit away: delete, swap neighbours, replace, insert (Norvig's spell checker)."""
    splits = [(w[:i], w[i:]) for i in range(len(w) + 1)]
    deletes = [a + b[1:] for a, b in splits if b]
    swaps = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b) > 1]
    replaces = [a + c + b[1:] for a, b in splits if b for c in _LETTERS]
    inserts = [a + c + b for a, b in splits for c in _LETTERS]
    return set(deletes + swaps + replaces + inserts)


class Speller:
    """Corrects words that are NOT in the index vocabulary to the most frequent (highest df) known
    word within edit distance 1, else 2. Known words are never touched, so it can't make a good
    query worse. The vocabulary is the export's own terms: it only suggests words that will match."""

    def __init__(self, df_by_term):
        self.df = df_by_term                                   # {term: df}

    def correct(self, word):
        if word in self.df or len(word) < 3 or any(ch.isdigit() for ch in word):
            return word
        e1 = _edits1(word)
        cands = [c for c in e1 if c in self.df]
        if not cands and len(word) <= 12:
            cands = [c for e in e1 for c in _edits1(e) if c in self.df]
        return max(cands, key=lambda c: (self.df[c], c)) if cands else word

    def suggest(self, query):
        """-> (corrected terms, 'did you mean' string or None)"""
        terms = textproc.tokenize(query)
        fixed = [self.correct(w) for w in terms]
        if fixed == terms:
            return terms, None
        out = query.lower()
        for a, b in zip(terms, fixed):
            if a != b:
                out = re.sub(rf"\b{re.escape(a)}\b", b, out)
        return fixed, out


# ---- twist: query expansion with Word2Vec neighbours -----------------------------------------
class Expander:
    """Adds up to `per_term` Word2Vec neighbours per query word (similarity >= min_sim) with
    weight `weight` x similarity, so expansions help but never outweigh the typed words."""

    def __init__(self, synonyms_df, vocab, per_term=2, min_sim=0.55, weight=0.3):
        self.nb = {}
        for term, grp in synonyms_df.sort_values(["term", "rank"]).groupby("term", sort=False):
            self.nb[term] = [(n, s) for n, s in zip(grp["neighbour"], grp["similarity"])
                             if s >= min_sim and n in vocab]
        self.per_term, self.weight = per_term, weight

    def expand(self, terms):
        """-> {expansion term: weight}, [(query word, expansion, similarity)]"""
        weights, shown = {}, []
        for w in terms:
            for n, s in self.nb.get(w, [])[: self.per_term]:
                if n not in terms and n not in weights:
                    weights[n] = self.weight * s
                    shown.append((w, n, round(s, 3)))
        return weights, shown


# ---- twist: query-aware snippet with highlights ----------------------------------------------
_SENT = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(])")
_WORD = re.compile(r"[A-Za-z0-9]+")


def smart_snippet(body, terms, stemmed, limit=280):
    """Pick the sentence (plus the next one if short) that contains the most distinct query
    words; return (text, [[start, end], ...] spans to highlight)."""
    if not body:
        return "", []
    want = set(terms)
    norm = (lambda w: textproc.stem(w)) if stemmed else (lambda w: w)
    sents = _SENT.split(body[:20000])
    best, best_score = 0, -1.0
    for i, s in enumerate(sents):
        hits = {norm(w.lower()) for w in _WORD.findall(s)} & want
        score = len(hits) - 0.001 * i                          # ties -> earlier sentence
        if score > best_score:
            best, best_score = i, score
    text = sents[best]
    if len(text) < 120 and best + 1 < len(sents):
        text = text + " " + sents[best + 1]
    if len(text) > limit:                                      # centre the window on the first hit
        first = next((m.start() for m in _WORD.finditer(text) if norm(m.group().lower()) in want), 0)
        start = max(0, first - limit // 3)
        text = ("... " if start else "") + text[start:start + limit].rsplit(" ", 1)[0] + " ..."
    marks = [[m.start(), m.end()] for m in _WORD.finditer(text) if norm(m.group().lower()) in want]
    return text, marks
