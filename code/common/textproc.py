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
# close to 0 and they only add noise and index size.
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
