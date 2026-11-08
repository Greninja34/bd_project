"""Query the running search service from a terminal, one line per result.

    ~/search-venv/bin/python q.py "bubonic plague" [k] [--opts=stem,spell,expand,smart]
                                  [--service=http://localhost:8000]
"""
import json
import sys
import urllib.parse
import urllib.request


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    service, opts = "http://localhost:8000", []
    for a in sys.argv[1:]:
        if a.startswith("--service="):
            service = a.split("=", 1)[1]
        elif a.startswith("--opts="):
            opts = [o for o in a.split("=", 1)[1].split(",") if o]
    if not args:
        sys.exit(__doc__)
    query, k = args[0], (args[1] if len(args) > 1 else "5")
    url = f"{service}/api/search?" + urllib.parse.urlencode({"q": query, "k": k, **{o: 1 for o in opts}})
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            d = json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code}: {e.read().decode()}")
    print(f"[{d['version']}] {d['query']!r} {'+'.join(opts) or 'spec'}: {d['matches']} matches, "
          f"terms {d['terms']}, {d['took_ms']} ms")
    if d.get("did_you_mean"):
        print(("  showing results for: " if d["corrected"] else "  did you mean: ") + d["did_you_mean"])
    if d.get("expansions"):
        print("  expanded with: " + ", ".join(f"{e} ({s})" for _, e, s in d["expansions"]))
    for n in d.get("notes", []):
        print("  note: " + n)
    for r in d["results"]:
        print(f"  {r['rank']:>2}. {r['title'][:34]:<34} id={r['doc_id']:<15} score={r['score']:.3f}"
              f"  bm25={r['bm25_norm']:.3f}  pr={r['pagerank_norm']:.3f}")
        if r.get("marks"):
            print(f"      {r['snippet'][:150]}")


if __name__ == "__main__":
    main()
