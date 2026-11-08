"""Node 4 - pull a finished export from HDFS into /serving and switch the service to it.

    ~/search-venv/bin/python pull_export.py --version v0 \
        --source hdfs://node2:9000/search/exports --serving /serving

Steps (any failure stops here, and the service keeps serving the version it already has):
  1. is <source>/<version>/_SUCCESS there?        no  -> export not finished, stop
  2. copy the export to /serving/.incoming-<version>  (hdfs dfs -get: Node 4 is only an HDFS *client*)
  3. verify it locally: manifest status OK, every check passed, file sizes and row counts match
  3b. (NLP twist) if fastembed is installed: embed every doc with MiniLM -> embeddings.npy
      (~10 s; failure here only disables rerank=1, it never blocks the switch)
  4. rename .incoming-<version> -> <version>     (atomic on one filesystem)
  5. point /serving/CURRENT at <version>, then ask the service to reload
     if the service refuses it, CURRENT is put back to the previous version

--source can also be a local folder (copied with scp from Node 3), e.g. --source ~/incoming
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def run(cmd):
    print("+ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd).returncode


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--source", required=True, help="hdfs://node2:9000/search/exports or a local folder")
    ap.add_argument("--serving", default=os.environ.get("SERVING_DIR", "/serving"))
    ap.add_argument("--service", default="http://localhost:8000", help="'' to skip the reload call")
    ap.add_argument("--no-embed", action="store_true", help="skip the MiniLM embeddings (rerank=1 off)")
    args = ap.parse_args()
    os.environ["SERVING_DIR"] = args.serving
    import serving                                    # after SERVING_DIR is set

    src = args.source.rstrip("/") + "/" + args.version
    is_hdfs = args.source.startswith("hdfs://")
    final = os.path.join(args.serving, args.version)
    incoming = os.path.join(args.serving, ".incoming-" + args.version)
    os.makedirs(args.serving, exist_ok=True)

    # 1. finished?
    done = (run(["hdfs", "dfs", "-test", "-e", src + "/_SUCCESS"]) == 0) if is_hdfs \
        else os.path.exists(os.path.join(os.path.expanduser(src), "_SUCCESS"))
    if not done:
        sys.exit(f"REFUSED: {src}/_SUCCESS not found - export missing or not finished. "
                 f"Still serving {serving.read_current()}.")

    # 2. copy
    shutil.rmtree(incoming, ignore_errors=True)
    if is_hdfs:
        if run(["hdfs", "dfs", "-get", src, incoming]) != 0:
            shutil.rmtree(incoming, ignore_errors=True)
            sys.exit(f"REFUSED: copy from HDFS failed. Still serving {serving.read_current()}.")
    else:
        shutil.copytree(os.path.expanduser(src), incoming)

    # 3. verify
    try:
        m = serving.verify_export(incoming)
    except serving.ExportInvalid as e:
        bad = os.path.join(args.serving, f".rejected-{args.version}-{datetime.now():%H%M%S}")
        os.replace(incoming, bad)
        sys.exit(f"REFUSED: {e}. Kept in {bad}. Still serving {serving.read_current()}.")
    print(f"verified {args.version}: {m['counts']['docs']} docs, {m['counts']['postings']} postings, "
          f"{sum(c['ok'] for c in m['checks'])}/{len(m['checks'])} checks passed, "
          f"docs snapshot {m['docs_snapshot_id']}")

    # 3b. semantic embeddings (optional)
    import semantic
    if args.no_embed:
        print("embeddings: skipped (--no-embed)")
    elif not semantic.available():
        print("embeddings: fastembed not installed -> rerank=1 unavailable for this version")
    else:
        try:
            n, secs = semantic.embed_version(incoming)
            print(f"embeddings: {n} docs embedded with {semantic.MODEL} in {secs}s")
        except Exception as e:
            print(f"embeddings: FAILED ({e}) -> rerank=1 unavailable for this version")

    # 4. publish locally
    if os.path.exists(final):
        os.replace(final, os.path.join(args.serving, f".old-{args.version}-{datetime.now():%H%M%S}"))
    os.replace(incoming, final)

    # 5. switch
    previous = serving.read_current()
    serving.write_current(args.version)
    print(f"CURRENT: {previous} -> {args.version}")
    if args.service:
        req = urllib.request.Request(args.service.rstrip("/") + "/api/admin/reload", method="POST")
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                print("service:", json.loads(r.read()))
        except urllib.error.HTTPError as e:
            if previous:
                serving.write_current(previous)
            sys.exit(f"service refused {args.version}: {e.read().decode()} -> CURRENT back to {previous}")
        except OSError as e:
            print(f"(service not reachable: {e}; it will load {args.version} when it starts)")


if __name__ == "__main__":
    main()
