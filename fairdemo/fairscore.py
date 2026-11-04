"""Score a dataset record against fixed FAIR checks. No model is involved, so the same record always gets
the same score, and every point can be traced to one check.

The checks follow the RDA FAIR Data Maturity Model indicators (RDA-F1-01M …) as F-UJI interprets them,
simplified to what one record can show. Each check has a weight; a principle's score is the share of its
weight that passes.

score(record, published=False) scores the record as it stands. Before publish, checks that need a
persistent identifier, a landing page or a catalog entry fail, which is the honest "before" picture.
"""
from __future__ import annotations

import re

from .curate import accepted

OPEN_LICENSES = {"CC0-1.0", "CC-BY-4.0", "CC-BY-3.0", "OGL-UK-3.0", "ODC-By-1.0", "PDDL-1.0", "Apache-2.0", "MIT",
                 "CC-BY-SA-4.0", "CC-BY-NC-4.0"}
COMMUNITY_FORMATS = {"application/x-netcdf", "application/x-hdf5", "application/fits", "application/x-root",
                     "text/x-vcf", "image/tiff", "application/vnd.apache.parquet", "text/csv"}


def _words(text) -> int:
    return len(re.findall(r"\w+", str(text or "")))


def checks(record: dict, published: bool) -> list[dict]:
    a = accepted(record)
    files = record.get("files", [])
    variables = record.get("variables", [])
    pid = record.get("pid") if published else None
    described = [v for v in variables if v.get("long_name") or v.get("standard_name")
                 or (v.get("description") or {}).get("status") == "accepted"]
    with_units = [v for v in variables if v.get("units") or v.get("unit_inferred")]
    standard = [v for v in variables if v.get("standard_name")]

    def c(principle, rid, label, ok, weight=1, hint=""):
        return {"principle": principle, "id": rid, "label": label, "pass": bool(ok), "weight": weight, "hint": hint}

    return [
        # Findable
        c("F", "RDA-F1-01D", "Data has a globally unique, persistent identifier", pid, 2, "minted at publish"),
        c("F", "RDA-F2-01M", "Title and a description of at least 30 words",
          a.get("title") and _words(a.get("description")) >= 30, 2),
        c("F", "RDA-F2-01M-k", "At least 5 keywords", len(a.get("keywords") or []) >= 5),
        c("F", "RDA-F2-01M-c", "Creators are named", a.get("creators")),
        c("F", "RDA-F3-01M", "Metadata lists every data file, with its checksum",
          files and all(f.get("sha256") for f in files)),
        c("F", "RDA-F4-01M", "Metadata is indexed in a searchable catalog", published and record.get("indexed")),
        # Accessible
        c("A", "RDA-A1-02M", "Metadata says how to get the data (protocol and URL)",
          published and record.get("access_url"), 2, "S3 and HTTPS URLs added at publish"),
        c("A", "RDA-A1-01M", "Access conditions are stated (licence or access rights)", a.get("license")),
        c("A", "RDA-A2-01M", "Metadata stays available apart from the data (landing page / catalog record)",
          published and record.get("landing_page")),
        # Interoperable
        c("I", "RDA-I1-01M", "Metadata uses a formal, shared language (JSON-LD, schema.org / RO-Crate)",
          published, 2, "RO-Crate written at publish"),
        c("I", "RDA-I1-02D", "Data is in a community file format", files and
          all(f.get("media_type") in COMMUNITY_FORMATS for f in files)),
        c("I", "RDA-I2-01M", "Variables are described with a controlled vocabulary (e.g. CF standard names)",
          variables and len(standard) / len(variables) >= 0.5),
        c("I", "RDA-I3-01M", "Metadata links to related work (citations, DOIs, source scripts)",
          a.get("citation") or a.get("isBasedOn")),
        # Reusable
        c("R", "RDA-R1-01M", "Most variables are described, with units",
          variables and len(described) / len(variables) >= 0.8 and len(with_units) / len(variables) >= 0.5, 2),
        c("R", "RDA-R1-01M-cov", "Coverage is stated (when and/or where)",
          a.get("temporalCoverage") or a.get("spatialCoverage")),
        c("R", "RDA-R1.1-01M", "The licence is a recognised, machine-readable licence (SPDX)",
          a.get("license") in OPEN_LICENSES, 2),
        c("R", "RDA-R1.2-01M", "Provenance: who made it, when, and how",
          a.get("creators") and a.get("datePublished") and (a.get("isBasedOn") or a.get("measurementTechnique")
                                                              or a.get("instrument")), 2),
        c("R", "RDA-R1.3-01M", "Metadata follows a community standard (Croissant / RO-Crate)", published),
    ]


def score(record: dict, published: bool = False) -> dict:
    results = checks(record, published)
    by = {}
    for p in "FAIR":
        rs = [r for r in results if r["principle"] == p]
        total = sum(r["weight"] for r in rs)
        got = sum(r["weight"] for r in rs if r["pass"])
        by[p] = round(100 * got / total) if total else 0
    total = sum(r["weight"] for r in results)
    got = sum(r["weight"] for r in results if r["pass"])
    return {"total": round(100 * got / total), "by_principle": by, "checks": results}
