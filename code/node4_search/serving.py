"""Loading and validating one export version from the local serving directory (Node 4).

Serving directory layout (default /serving, override with SERVING_DIR):
    /serving/v0/          a complete, verified export (docs/, postings/, manifest.json, _SUCCESS)
    /serving/v1/
    /serving/CURRENT      one line: the version the service should serve, e.g. "v1"

verify_export() is used twice: by pull_export.py before a version is accepted into /serving,
and by the service right before it switches to that version. If either check fails, the service
keeps serving the version it already has.
"""
import json
import os

import pyarrow.parquet as pq

SERVING_DIR = os.environ.get("SERVING_DIR", "/serving")


class ExportInvalid(Exception):
    pass


def verify_export(path):
    """Raise ExportInvalid unless `path` is a complete export whose manifest says OK. Returns the manifest."""
    if not os.path.isdir(path):
        raise ExportInvalid(f"{path} does not exist")
    if not os.path.exists(os.path.join(path, "_SUCCESS")):
        raise ExportInvalid("no _SUCCESS marker: export not finished")
    try:
        with open(os.path.join(path, "manifest.json")) as f:
            m = json.load(f)
    except (OSError, ValueError) as e:
        raise ExportInvalid(f"manifest.json unreadable: {e}")
    if m.get("status") != "OK":
        raise ExportInvalid(f"manifest status is {m.get('status')}")
    failed = [c["check"] for c in m.get("checks", [])
              if not c.get("ok") and c.get("level", "error") == "error"]   # "warn" = optional NLP extras
    if failed:
        raise ExportInvalid(f"failed checks: {failed}")
    for f in m.get("files", []):
        p = os.path.join(path, f["path"])
        if not os.path.exists(p) or os.path.getsize(p) != f["bytes"]:
            raise ExportInvalid(f"file missing or wrong size: {f['path']}")
    n_docs = pq.ParquetDataset(os.path.join(path, "docs")).read(columns=["doc_id"]).num_rows
    n_post = pq.ParquetDataset(os.path.join(path, "postings")).read(columns=["doc_id"]).num_rows
    if n_docs != m["counts"]["docs"] or n_post != m["counts"]["postings"]:
        raise ExportInvalid(f"row counts {n_docs}/{n_post} do not match manifest "
                            f"{m['counts']['docs']}/{m['counts']['postings']}")
    for extra in ("postings_stem", "synonyms"):            # NLP twist tables, when the export has them
        if m["counts"].get(extra):
            n = pq.ParquetDataset(os.path.join(path, extra)).read(columns=["term"]).num_rows
            if n != m["counts"][extra]:
                raise ExportInvalid(f"{extra}: {n} rows, manifest says {m['counts'][extra]}")
    return m


def read_current(serving_dir=SERVING_DIR):
    try:
        with open(os.path.join(serving_dir, "CURRENT")) as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def write_current(version, serving_dir=SERVING_DIR):
    """Atomically point CURRENT at `version` (write a temp file, then rename over the old one)."""
    tmp = os.path.join(serving_dir, ".CURRENT.tmp")
    with open(tmp, "w") as f:
        f.write(version + "\n")
    os.replace(tmp, os.path.join(serving_dir, "CURRENT"))


def list_versions(serving_dir=SERVING_DIR):
    out = []
    if not os.path.isdir(serving_dir):
        return out
    for name in sorted(os.listdir(serving_dir)):
        p = os.path.join(serving_dir, name)
        if name.startswith(".") or not os.path.isdir(p):
            continue
        try:
            m = verify_export(p)
            out.append({"version": name, "valid": True, "docs": m["counts"]["docs"],
                        "created_at": m.get("created_at"), "phase": m.get("phase"),
                        "docs_snapshot_id": m.get("docs_snapshot_id")})
        except ExportInvalid as e:
            out.append({"version": name, "valid": False, "error": str(e)})
    return out
