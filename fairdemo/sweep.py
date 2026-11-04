"""The backlog: folders of data already on the system that nobody has made FAIR.

VAST Catalog lists every file and directory in the cluster's namespace (refreshed on a schedule), so one
query finds them: group files by folder, keep folders that hold data, and mark each one

  curated   it has an ro-crate-metadata.json (published)
  review    the pipeline has drafted a record (_fair/record.json) that waits for a person
  queued    it has a _READY marker but no record yet (being curated)
  dark      none of these: data nobody has described

Each folder carries what a curator needs to prioritise it: how much data, how old, which formats, and
whether any README, CITATION.cff or LICENSE is there to start from.
"""
from __future__ import annotations

import datetime as dt
import re
from collections import defaultdict

DATA_EXTENSIONS = {"nc", "nc4", "cdf", "h5", "hdf5", "he5", "fits", "fit", "fts", "root", "vcf", "bgz", "tif",
                   "tiff", "csv", "tsv", "parquet", "zarr", "grib", "grb", "grib2", "sav", "mat", "npy", "npz"}
README = re.compile(r"^(readme|about|description)(\.(md|txt|rst))?$", re.I)
CITATION = re.compile(r"^(citation\.cff|codemeta\.json|\.?zenodo\.json)$", re.I)
LICENSE = re.compile(r"^(licen[cs]e|copying)(\.(md|txt))?$", re.I)
OUTPUTS = {"ro-crate-metadata.json", "croissant.json", "ro-crate-preview.html"}


def _ext(name: str, ext: str | None) -> str:
    if name.lower().endswith(".vcf.gz") or name.lower().endswith(".vcf.bgz"):
        return "vcf"
    if ext:
        return ext.lower()
    return name.rsplit(".", 1)[-1].lower() if "." in name else ""


def backlog(elements: list[dict], skip=("/.vast/", "/_fair/")) -> list[dict]:
    """elements: Catalog rows with parent_path, name, extension, size, mtime, element_type."""
    folders = defaultdict(lambda: {"files": 0, "data_files": 0, "bytes": 0, "newest": None, "oldest": None,
                                   "formats": defaultdict(int), "readme": False, "citation": False,
                                   "license": False, "crate": False, "ready": False, "record": False})
    for e in elements:
        if e.get("element_type") != "FILE":
            continue
        parent = e["parent_path"] if e["parent_path"].endswith("/") else e["parent_path"] + "/"
        if parent.endswith("/_fair/") and e["name"] == "record.json":
            folders[parent[: -len("_fair/")]]["record"] = True
            continue
        if any(s in parent for s in skip):
            continue
        f = folders[parent]
        name = e["name"]
        f["files"] += 1
        f["bytes"] += e.get("size") or 0
        m = e.get("mtime")
        if m:
            f["newest"] = max(f["newest"] or m, m)
            f["oldest"] = min(f["oldest"] or m, m)
        x = _ext(name, e.get("extension"))
        if x in DATA_EXTENSIONS:
            f["data_files"] += 1
            f["formats"][x] += 1
        f["readme"] |= bool(README.match(name))
        f["citation"] |= bool(CITATION.match(name))
        f["license"] |= bool(LICENSE.match(name))
        f["crate"] |= name in OUTPUTS
        f["ready"] |= name == "_READY"
    out = []
    for path, f in folders.items():
        if not f["data_files"]:
            continue
        bucket, _, prefix = path.strip("/").partition("/")
        status = "curated" if f["crate"] else "review" if f["record"] else "queued" if f["ready"] else "dark"
        out.append({"path": path, "bucket": bucket, "prefix": prefix, "status": status,
                    "files": f["files"], "data_files": f["data_files"], "bytes": f["bytes"],
                    "newest": _iso(f["newest"]), "oldest": _iso(f["oldest"]),
                    "formats": dict(sorted(f["formats"].items(), key=lambda kv: -kv[1])),
                    "docs": [k for k in ("readme", "citation", "license") if f[k]]})
    # The work queue: dark first, then by size (where the value is).
    out.sort(key=lambda r: ({"dark": 0, "queued": 1, "review": 2, "curated": 3}[r["status"]], -r["bytes"]))
    return out


def summary(rows: list[dict]) -> dict:
    by = defaultdict(lambda: {"folders": 0, "bytes": 0})
    for r in rows:
        by[r["status"]]["folders"] += 1
        by[r["status"]]["bytes"] += r["bytes"]
    return dict(by)


def _iso(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):  # ns since the epoch
        return dt.datetime.fromtimestamp(v / 1e9, dt.timezone.utc).isoformat(timespec="seconds")
    return str(v)
