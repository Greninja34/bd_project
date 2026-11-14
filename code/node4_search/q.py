"""Query the running search service from a terminal, one line per result.

    ~/search-venv/bin/python q.py "bubonic plague" [k] [--service=http://localhost:8000]
"""
import json
import sys
import urllib.parse
import urllib.request


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    service = "http://localhost:8000"
    for a in sys.argv[1:]:
        if a.startswith("--service="):
            service = a.split("=", 1)[1]
    if not args:
        sys.exit(__doc__)
    query, k = args[0], (args[1] if len(args) > 1 else "5")
    url = f"{service}/api/search?" + urllib.parse.urlencode({"q": query, "k": k})
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            d = json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code}: {e.read().decode()}")
    print(f"[{d['version']}] {d['query']!r}: {d['matches']} matches, "
          f"terms {d['terms']}, {d['took_ms']} ms")
    for r in d["results"]:
        print(f"  {r['rank']:>2}. {r['title'][:34]:<34} id={r['doc_id']:<15} score={r['score']:.3f}"
              f"  bm25={r['bm25_norm']:.3f}  pr={r['pagerank_norm']:.3f}")


if __name__ == "__main__":
    main()
