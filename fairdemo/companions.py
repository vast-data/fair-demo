"""Companion documents in a dataset folder: README, CITATION.cff, LICENSE, scripts.

These are the facts a researcher already wrote down. Curation trusts them above anything the LLM infers,
and authors, licence and identifiers may only come from here, from file headers, or from a human.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

README = re.compile(r"^(readme|about|description|notes?)(\.(md|txt|rst))?$", re.I)
CITATION = re.compile(r"^(citation\.cff|citation(\.(txt|md|bib))?|.*\.bib|codemeta\.json|zenodo\.json|\.zenodo\.json)$", re.I)
LICENSE = re.compile(r"^(licen[cs]e|copying)(\.(md|txt))?$", re.I)
SCRIPT = re.compile(r".*\.(py|r|jl|m|sh|slurm|pbs|ipynb|f90|c|cpp)$", re.I)

MAX_TEXT = 6000

# Licence texts we recognise, mapped to SPDX ids. Order matters: most specific first.
LICENSE_PATTERNS = [
    (r"creative\s+commons\s+zero|CC0", "CC0-1.0"),
    (r"attribution[- ]noncommercial|CC[- ]BY[- ]NC", "CC-BY-NC-4.0"),
    (r"attribution[- ]sharealike|CC[- ]BY[- ]SA", "CC-BY-SA-4.0"),
    (r"attribution 4\.0|CC[- ]BY[- ]4\.0|CC[- ]BY\b", "CC-BY-4.0"),
    (r"open government licen[cs]e", "OGL-UK-3.0"),
    (r"open data commons attribution|ODC[- ]BY", "ODC-By-1.0"),
    (r"public domain dedication and licen[cs]e|PDDL", "PDDL-1.0"),
    (r"apache license", "Apache-2.0"),
    (r"\bMIT License\b|permission is hereby granted, free of charge", "MIT"),
]


def _read(path: Path, limit: int = MAX_TEXT) -> str:
    for enc in ("utf-8", "latin-1"):
        try:
            return path.read_text(encoding=enc)[:limit]
        except UnicodeDecodeError:
            continue
    return ""


def spdx_from_text(text: str) -> str | None:
    for rx, spdx in LICENSE_PATTERNS:
        if re.search(rx, text, re.I):
            return spdx
    return None


def parse_cff(path: Path) -> dict:
    """CITATION.cff -> creators, title, doi, licence, keywords, date. Only what the file states."""
    try:
        cff = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        return {"error": "CITATION.cff not parsed: %s" % e}
    out = {}
    authors = []
    for a in cff.get("authors") or []:
        name = a.get("name") or " ".join(x for x in (a.get("given-names"), a.get("family-names")) if x)
        if name:
            authors.append({k: v for k, v in {
                "name": name, "orcid": a.get("orcid"), "affiliation": a.get("affiliation"), "email": a.get("email"),
            }.items() if v})
    if authors:
        out["creators"] = authors
    for src, dst in (("title", "title"), ("abstract", "description"), ("doi", "identifier"),
                     ("license", "license"), ("date-released", "datePublished"), ("version", "version"),
                     ("url", "url"), ("repository-code", "codeRepository")):
        if cff.get(src):
            out[dst] = str(cff[src])
    if cff.get("keywords"):
        out["keywords"] = [str(k) for k in cff["keywords"]]
    for ident in cff.get("identifiers") or []:
        if ident.get("type") == "doi" and "identifier" not in out:
            out["identifier"] = str(ident.get("value"))
    refs = cff.get("references") or []
    if refs:
        out["references"] = [r.get("doi") or r.get("url") or r.get("title") for r in refs if isinstance(r, dict)]
    return out


def find(folder: str | Path, data_files: set[str] | None = None) -> dict:
    """Companion documents in the folder (top level and one level down)."""
    folder = Path(folder)
    data_files = data_files or set()
    found = {"readmes": [], "citations": [], "licenses": [], "scripts": [], "other_docs": []}
    candidates = [p for p in folder.iterdir() if p.is_file()]
    for sub in (p for p in folder.iterdir() if p.is_dir() and not p.name.startswith(".")):
        candidates += [p for p in sub.iterdir() if p.is_file()]
    for p in sorted(candidates):
        if p.name.startswith(".") or str(p) in data_files or p.name in ("ro-crate-metadata.json", "croissant.json"):
            continue
        rel = str(p.relative_to(folder))
        if README.match(p.name):
            text = _read(p)
            found["readmes"].append({"file": rel, "text": text, "doi": _dois(text), "license": spdx_from_text(text)})
        elif CITATION.match(p.name):
            entry = {"file": rel}
            if p.name.lower() == "citation.cff":
                entry["cff"] = parse_cff(p)
            else:
                text = _read(p)
                entry["text"] = text
                entry["doi"] = _dois(text)
            found["citations"].append(entry)
        elif LICENSE.match(p.name):
            text = _read(p, 3000)
            found["licenses"].append({"file": rel, "spdx": spdx_from_text(text), "text": text[:500]})
        elif SCRIPT.match(p.name):
            text = _read(p, 3000)
            found["scripts"].append({"file": rel, "language": p.suffix.lstrip(".").lower(), "head": text})
        elif p.suffix.lower() in (".md", ".txt", ".pdf", ".rst", ".html"):
            found["other_docs"].append({"file": rel, "text": _read(p, 2000) if p.suffix.lower() != ".pdf" else ""})
    return found


def _dois(text: str) -> list[str]:
    return sorted(set(m.rstrip(".,;)") for m in re.findall(r"\b10\.\d{4,9}/[^\s\"'<>]+", text)))


def summary_text(c: dict) -> str:
    parts = []
    for r in c["readmes"]:
        parts.append("README %s:\n%s" % (r["file"], r["text"][:3000]))
    for ci in c["citations"]:
        parts.append("CITATION %s: %s" % (ci["file"], ci.get("cff") or ci.get("text", "")[:1500]))
    for lic in c["licenses"]:
        parts.append("LICENSE %s: spdx=%s" % (lic["file"], lic["spdx"]))
    for s in c["scripts"][:5]:
        parts.append("SCRIPT %s (first lines):\n%s" % (s["file"], s["head"][:800]))
    for o in c["other_docs"][:3]:
        parts.append("DOC %s:\n%s" % (o["file"], o["text"][:800]))
    return "\n\n".join(parts) or "(no companion documents)"
