"""Text processing shared by the Spark index job (Node 3) and the search service (Node 4).

Both sides MUST tokenize the same way, otherwise a query word never matches the index.
That is why this one file is used in both places: Spark ships it to the executors with
sc.addPyFile(), and Node 4 imports it directly.

Pipeline:  raw MediaWiki markup --strip_wiki()--> plain text --tokenize()--> list of terms
Pure standard library on purpose: the Spark executors on Nodes 2 and 3 only have Python 3.10,
no extra packages.
"""
import html
import re

# A small English stopword list. Stopwords appear in nearly every document, so their IDF is
# close to 0 and they only add noise and index size. No stemming (kept simple and predictable).
STOPWORDS = frozenset("""
a about above after again against all also am an and any are as at be because been before
being below between both but by can could did do does doing down during each few for from
further had has have having he her here hers herself him himself his how i if in into is it
its itself just me more most my myself no nor not now of off on once only or other our ours
ourselves out over own same she should so some such than that the their theirs them
themselves then there these they this those through to too under until up very was we were
what when where which while who whom why will with would you your yours yourself yourselves
""".split())

_COMMENT = re.compile(r"<!--.*?-->", re.S)
_REF_PAIR = re.compile(r"<ref[^>/]*>.*?</ref>", re.S | re.I)
_REF_SELF = re.compile(r"<ref[^>]*/>", re.I)
_TEMPLATE = re.compile(r"\{\{[^{}]*\}\}")            # innermost {{...}}
_TABLE = re.compile(r"\{\|.*?\|\}", re.S)            # {| ... |} wiki tables
_LINK = re.compile(r"\[\[([^\[\]]*)\]\]")            # innermost [[...]]
_EXT_LINK = re.compile(r"\[(?:https?:)?//[^\s\]]+\s*([^\]]*)\]")
_URL = re.compile(r"https?://\S+")
_TAG = re.compile(r"<[^>]+>")
_QUOTES = re.compile(r"'{2,}")
_TOKEN = re.compile(r"[a-z0-9]+")
_DROP_LINK_PREFIXES = ("file:", "image:", "category:", "media:")


def _link_label(match):
    inner = match.group(1)
    if inner.strip().lower().startswith(_DROP_LINK_PREFIXES):
        return " "                                   # images / categories carry no prose
    return inner.split("|")[-1]                      # [[target|label]] -> label, [[target]] -> target


def strip_wiki(text):
    """Turn raw MediaWiki markup into plain prose."""
    if not text:
        return ""
    t = _COMMENT.sub(" ", text)
    t = _REF_PAIR.sub(" ", t)
    t = _REF_SELF.sub(" ", t)
    # Templates nest ({{a|{{b}}}}): remove the innermost ones repeatedly until none are left.
    for _ in range(20):
        t, n = _TEMPLATE.subn(" ", t)
        if n == 0:
            break
    t = _TABLE.sub(" ", t)
    # Links nest too (an image caption can contain links): resolve innermost first.
    for _ in range(10):
        t, n = _LINK.subn(_link_label, t)
        if n == 0:
            break
    t = _EXT_LINK.sub(r" \1 ", t)
    t = _URL.sub(" ", t)
    t = _TAG.sub(" ", t)
    t = _QUOTES.sub("", t)
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def tokenize(text):
    """Lowercase, split on anything that is not a letter or digit, drop stopwords and 1-char tokens."""
    if not text:
        return []
    return [w for w in _TOKEN.findall(text.lower()) if len(w) > 1 and w not in STOPWORDS]


def doc_terms(title, raw_text):
    """The terms indexed for one document: its title plus its cleaned body."""
    return tokenize((title or "") + " " + strip_wiki(raw_text or ""))


# ---------------------------------------------------------------------------------------------
# NLP twist 1: Porter stemming (M. F. Porter, "An algorithm for suffix stripping", 1980).
# Pure Python so it runs on the Spark executors too. Maps "phases", "phased", "phasing" -> "phase".
# ---------------------------------------------------------------------------------------------
_VOWELS = "aeiou"


def _cons(w, i):
    """Is w[i] a consonant? 'y' is a consonant at the start or after a vowel."""
    c = w[i]
    if c in _VOWELS:
        return False
    if c == "y":
        return i == 0 or not _cons(w, i - 1)
    return True


def _m(stem):
    """Porter's measure: the number of vowel-consonant sequences, [C](VC)^m[V]."""
    m, i, n = 0, 0, len(stem)
    while i < n and _cons(stem, i):
        i += 1
    while i < n:
        while i < n and not _cons(stem, i):
            i += 1
        if i >= n:
            break
        while i < n and _cons(stem, i):
            i += 1
        m += 1
    return m


def _has_vowel(stem):
    return any(not _cons(stem, i) for i in range(len(stem)))


def _double_cons(w):
    return len(w) >= 2 and w[-1] == w[-2] and _cons(w, len(w) - 1)


def _cvc(w):
    """consonant-vowel-consonant at the end, the last one not w, x or y (e.g. -hop, -fil)."""
    return (len(w) >= 3 and _cons(w, len(w) - 3) and not _cons(w, len(w) - 2)
            and _cons(w, len(w) - 1) and w[-1] not in "wxy")


_STEP2 = [("ational", "ate"), ("tional", "tion"), ("enci", "ence"), ("anci", "ance"), ("izer", "ize"),
          ("abli", "able"), ("alli", "al"), ("entli", "ent"), ("eli", "e"), ("ousli", "ous"),
          ("ization", "ize"), ("ation", "ate"), ("ator", "ate"), ("alism", "al"), ("iveness", "ive"),
          ("fulness", "ful"), ("ousness", "ous"), ("aliti", "al"), ("iviti", "ive"), ("biliti", "ble")]
_STEP3 = [("icate", "ic"), ("ative", ""), ("alize", "al"), ("iciti", "ic"), ("ical", "ic"),
          ("ful", ""), ("ness", "")]
_STEP4 = ["al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ion",
          "ou", "ism", "ate", "iti", "ous", "ive", "ize"]


def _replace(w, rules, min_m):
    for suf, rep in sorted(rules, key=lambda r: -len(r[0])):     # the longest matching suffix decides
        if w.endswith(suf):
            stem = w[: len(w) - len(suf)]
            return stem + rep if _m(stem) > min_m else w
    return w


def _porter(w):
    if len(w) <= 2:
        return w
    # step 1a: plurals
    if w.endswith("sses"):
        w = w[:-2]
    elif w.endswith("ies"):
        w = w[:-2]
    elif w.endswith("ss"):
        pass
    elif w.endswith("s"):
        w = w[:-1]
    # step 1b: -eed, -ed, -ing
    if w.endswith("eed"):
        if _m(w[:-3]) > 0:
            w = w[:-1]
    else:
        for suf in ("ed", "ing"):
            if w.endswith(suf) and _has_vowel(w[: -len(suf)]):
                w = w[: -len(suf)]
                if w.endswith(("at", "bl", "iz")):
                    w += "e"
                elif _double_cons(w) and w[-1] not in "lsz":
                    w = w[:-1]
                elif _m(w) == 1 and _cvc(w):
                    w += "e"
                break
    # step 1c: y -> i
    if w.endswith("y") and _has_vowel(w[:-1]):
        w = w[:-1] + "i"
    w = _replace(w, _STEP2, 0)
    w = _replace(w, _STEP3, 0)
    # step 4: drop suffixes when m > 1 (-ion only after s or t)
    for suf in sorted(_STEP4, key=len, reverse=True):
        if w.endswith(suf):
            stem = w[: -len(suf)]
            if _m(stem) > 1 and (suf != "ion" or stem.endswith(("s", "t"))):
                w = stem
            break
    # step 5: final -e, final double l
    if w.endswith("e"):
        stem = w[:-1]
        if _m(stem) > 1 or (_m(stem) == 1 and not _cvc(stem)):
            w = stem
    if _m(w) > 1 and _double_cons(w) and w.endswith("l"):
        w = w[:-1]
    return w


_STEM_CACHE = {}


def stem(word):
    """Porter stem of one lowercase token (tokens with digits are left alone)."""
    s = _STEM_CACHE.get(word)
    if s is None:
        s = word if any(ch.isdigit() for ch in word) else _porter(word)
        if len(_STEM_CACHE) < 500000:
            _STEM_CACHE[word] = s
    return s


def stem_terms(terms):
    return [stem(w) for w in terms]


def clean_body(raw_text, limit=30000):
    """Cleaned prose of a doc (wiki markup and section headings removed), for query-aware snippets."""
    clean = strip_wiki(raw_text or "")
    clean = re.sub(r"=+[^=]+=+", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[:limit]


def make_snippet(raw_text, limit=240):
    """First `limit` characters of the cleaned text, for the result list in the UI."""
    clean = strip_wiki(raw_text or "")
    clean = re.sub(r"=+[^=]+=+", " ", clean)         # drop "== Section ==" headings
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean if len(clean) <= limit else clean[:limit].rsplit(" ", 1)[0] + " ..."


if __name__ == "__main__":
    sample = ("{{monththisyear|4}}\n'''April''' is the fourth [[month]] of the "
              "[[Julian calendar|Julian]] year. [[File:x.jpg|thumb|[[Spring (season)|Spring]] flowers]]"
              "<ref>a citation</ref>")
    print(strip_wiki(sample))
    print(doc_terms("April", sample))
