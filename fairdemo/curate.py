"""Curate one dataset folder into a proposed record.

Every field carries where it came from, so a reviewer can see why it says what it says:

  {"value": ..., "source": "header" | "companion" | "inferred" | "human",
   "evidence": "oisst.nc#title" | "CITATION.cff" | "model reasoning",
   "confidence": 0.0-1.0, "status": "accepted" | "proposed" | "missing"}

Facts from file headers and the researcher's own documents are taken as they are. The model may only
propose descriptive fields (title, description, keywords, domain, technique, variable descriptions); it
never supplies creators, licence, publisher or identifiers. Those come from the data, or from a human.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from . import __version__, companions, extract, llm

DOMAINS = ["climate & earth science", "astronomy & astrophysics", "particle physics", "genomics",
           "bioimaging & microscopy", "materials science", "public sector & social statistics",
           "ecology & biodiversity", "chemistry", "engineering", "other"]

INFERABLE = ("title", "description", "keywords", "domain", "measurementTechnique",
             "temporalCoverage", "spatialCoverage", "instrument")
FACT_ONLY = ("creators", "license", "publisher", "identifier", "datePublished", "version", "funding")
REQUIRED = ("title", "description", "keywords", "creators", "license")

# Header attributes (ACDD/CF for NetCDF, FITS keywords, VCF meta lines) that state a record field outright.
# Attributes that only hint at a field (FITS OBJECT is the target, not a title; CF "source" is the method
# of production) are left to the model, which sees every attribute.
HEADER_MAP = {
    "title": ["title"],
    "description": ["summary", "abstract", "description"],
    "keywords": ["keywords"],
    "license": ["license", "licence"],
    "identifier": ["doi", "DOI"],
    "publisher": ["publisher_name", "publisher_institution", "ORIGIN"],
    "creators": ["creator_name", "creator_institution", "institution"],
    "datePublished": ["date_created", "date_issued", "fileDate"],
    "version": ["product_version", "PROCVER"],
    "instrument": ["instrument", "INSTRUME", "TELESCOP", "platform", "sensor"],
    "citation": ["references", "reference"],
}

# Models are poorly calibrated about their own guesses: never let an inference look as sure as a fact,
# and drop variable descriptions the model itself rates as near-guesswork.
MAX_INFERRED_CONFIDENCE = 0.9
MIN_VARIABLE_CONFIDENCE = 0.4

SYSTEM = """You are a research data curator producing FAIR metadata for a dataset folder.
You are given FACTS: file headers, variable lists and the researchers' own documents.
Rules:
- Use only the FACTS. Never invent people, organisations, licences, DOIs, dates, places or numbers.
- If the FACTS do not support a field, return null for it with confidence 0.
- confidence is your probability (0-1) that a domain expert would accept the value as written.
- evidence names the fact(s) you relied on, e.g. "oisst.nc attr summary" or "variable names sst, ice".
- Write the description for a researcher deciding whether to reuse the data: what was measured or
  simulated, how, where and when, and the files' structure. 2 to 5 sentences, no marketing.
- The title names what the data is (subject, method, place, period), in under 15 words. Never use a
  file name, a run label or an internal experiment name as the title, and never put a person's name in it.
- keywords: 5 to 10 specific, lower-case terms a domain scientist would search for.
Return only a JSON object."""

USER = """FACTS
=====
{facts}

Return JSON with exactly these keys:
{{
 "title": {{"value": str|null, "confidence": num, "evidence": str}},
 "description": {{"value": str|null, "confidence": num, "evidence": str}},
 "keywords": {{"value": [str]|null, "confidence": num, "evidence": str}},
 "domain": {{"value": one of {domains}, "confidence": num, "evidence": str}},
 "measurementTechnique": {{"value": str|null, "confidence": num, "evidence": str}},
 "instrument": {{"value": str|null, "confidence": num, "evidence": str}},
 "temporalCoverage": {{"value": "ISO8601 interval start/end"|null, "confidence": num, "evidence": str}},
 "spatialCoverage": {{"value": str|null, "confidence": num, "evidence": str}},
 "variables": [{{"name": str, "description": str, "unit": str|null, "confidence": num}}]
}}
"variables" only for variables whose meaning is not already stated in the FACTS (at most 30), and only
when the FACTS let you say what the variable measures. Never describe a variable by its shape or type alone;
leave out any variable you cannot interpret.
If the files look unrelated to each other, say so in the description and lower your confidence."""


def field(value, source, evidence, confidence, status=None):
    if status is None:
        status = "accepted" if source in ("header", "companion", "human") else "proposed"
    return {"value": value, "source": source, "evidence": evidence, "confidence": round(float(confidence), 2), "status": status}


def _first_attr(profiles: list[dict], keys: list[str]):
    for p in profiles:
        for k in keys:
            v = p["attributes"].get(k)
            if v and str(v).strip() and str(v).strip().lower() not in ("none", "null", "n/a", "unknown"):
                return str(v).strip(), "%s#%s" % (p["name"], k)
    return None, None


def harvest(profiles: list[dict], comp: dict) -> dict:
    """Fields stated by the data or the researchers. No inference."""
    fields = {}
    for name, keys in HEADER_MAP.items():
        v, ev = _first_attr(profiles, keys)
        if v is None:
            continue
        if name == "keywords":
            v = [k.strip() for k in re.split(r"[,;>]", v) if k.strip()][:20]
        elif name == "creators":
            v = [{"name": v}]
        elif name == "license":
            v = companions.spdx_from_text(v) or v
        fields[name] = field(v, "header", ev, 0.95)

    for c in comp["citations"]:
        cff = c.get("cff") or {}
        for k in ("title", "description", "identifier", "license", "datePublished", "version", "keywords", "creators"):
            if cff.get(k):
                fields[k] = field(cff[k], "companion", c["file"], 0.97)
        if cff.get("references"):
            fields["citation"] = field(cff["references"], "companion", c["file"], 0.95)
        for doi in c.get("doi") or []:
            fields.setdefault("citation", field([doi], "companion", c["file"], 0.9))
    for lic in comp["licenses"]:
        if lic["spdx"] and "license" not in fields:
            fields["license"] = field(lic["spdx"], "companion", lic["file"], 0.95)
    for r in comp["readmes"]:
        if r["license"] and "license" not in fields:
            fields["license"] = field(r["license"], "companion", r["file"] + " (licence statement)", 0.8)
        if r["doi"] and "citation" not in fields:
            fields["citation"] = field(r["doi"], "companion", r["file"], 0.8)

    if comp["scripts"]:
        fields["isBasedOn"] = field([s["file"] for s in comp["scripts"]], "companion", "scripts in folder", 0.9)
    conv = sorted({p["conventions"] for p in profiles if p.get("conventions")})
    if conv:
        fields["conformsTo"] = field(conv, "header", "file conventions", 0.95)
    formats = sorted({p["media_type"] for p in profiles})
    fields["encodingFormat"] = field(formats, "header", "file signatures", 1.0)
    return fields


def infer(profiles: list[dict], comp: dict, harvested: dict, model: str | None = None) -> tuple[dict, list]:
    facts = "\n\n".join(extract.summary_text(p) for p in profiles[:12])
    if len(profiles) > 12:
        facts += "\n\n… and %d more files of formats %s" % (
            len(profiles) - 12, sorted({p["format"] for p in profiles[12:]}))
    facts += "\n\nCOMPANION DOCUMENTS\n" + companions.summary_text(comp)
    known = {k: v["value"] for k, v in harvested.items() if k in FACT_ONLY + ("title", "description")}
    if known:
        facts += "\n\nALREADY KNOWN (stated by the data or researchers)\n" + json.dumps(known, default=str)[:2000]
    out = llm.chat_json(SYSTEM, USER.format(facts=facts[:24000], domains=DOMAINS), model=model)

    fields = {}
    for name in INFERABLE:
        item = out.get(name) or {}
        value = item.get("value") if isinstance(item, dict) else item
        if value in (None, "", []) or name in harvested:
            continue
        conf = item.get("confidence", 0.5) if isinstance(item, dict) else 0.5
        conf = min(float(conf or 0), MAX_INFERRED_CONFIDENCE)
        fields[name] = field(value, "inferred", (item.get("evidence") if isinstance(item, dict) else None) or "model", conf)
    if "domain" in fields and fields["domain"]["value"] not in DOMAINS:
        fields["domain"]["value"] = "other"
    return fields, out.get("variables") or []


def merge_variables(profiles: list[dict], inferred_vars: list) -> list[dict]:
    by_name = {}
    for p in profiles:
        for v in p["variables"]:
            entry = by_name.setdefault(v["name"], {"name": v["name"], "files": []})
            entry["files"].append(p["name"])
            for k in ("units", "standard_name", "long_name", "dtype"):
                if v.get(k) and not entry.get(k):
                    entry[k] = v[k]
    for iv in inferred_vars:
        e = by_name.get(iv.get("name"))
        conf = min(float(iv.get("confidence") or 0), MAX_INFERRED_CONFIDENCE)
        if e and not e.get("long_name") and iv.get("description") and conf >= MIN_VARIABLE_CONFIDENCE:
            e["description"] = field(iv["description"], "inferred", "variable name and context", conf)
            if iv.get("unit") and not e.get("units"):
                e["unit_inferred"] = iv["unit"]
    return list(by_name.values())


OUTPUTS = ("ro-crate-metadata.json", "croissant.json", "ro-crate-preview.html")


def curate(folder: str | Path, use_llm: bool = True, model: str | None = None) -> dict:
    """Curate a folder on disk: profile its data files, then curate_profiles."""
    folder = Path(folder)
    paths = sorted(p for p in folder.rglob("*") if p.is_file() and not p.name.startswith(".")
                   and p.name not in OUTPUTS and "_fair" not in p.parts)
    data_paths = [p for p in paths if extract.is_data_file(p)]
    profiles = [extract.profile(p) for p in data_paths]
    comp = companions.find(folder, {str(p) for p in data_paths})
    return curate_profiles(folder.name, str(folder), profiles, comp, use_llm, model)


def curate_profiles(name: str, location: str, profiles: list[dict], comp: dict,
                    use_llm: bool = True, model: str | None = None) -> dict:
    """Curate from file profiles already taken (on the cluster, one per object as it landed) and the
    folder's companion documents, so the data itself is read only once."""
    fields = harvest(profiles, comp)
    inferred_vars, llm_error = [], None
    if use_llm and profiles:
        try:
            inferred, inferred_vars = infer(profiles, comp, fields, model=model)
            fields.update(inferred)
        except llm.LLMError as e:
            llm_error = str(e)

    for key in REQUIRED:
        if key not in fields:
            fields[key] = field(None, "none", "not stated in data or documents; needs a person", 0, "missing")

    return {
        "folder": location,
        "name": name,
        "generated": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "curator": {"software": "fairdemo %s" % __version__, "model": (model or llm.CHAT_MODEL) if use_llm else None},
        "fields": fields,
        "variables": merge_variables(profiles, inferred_vars),
        "files": [{k: p[k] for k in ("name", "path", "size", "sha256", "format", "media_type")} | {"notes": p["notes"]}
                  for p in profiles],
        "companions": {k: [c["file"] for c in v] for k, v in comp.items()},
        "llm_error": llm_error,
    }


def apply_review(record: dict, decisions: dict) -> dict:
    """decisions: {field: {"action": "accept" | "reject" | "edit", "value": ...}} from the review screen."""
    for name, d in decisions.items():
        f = record["fields"].get(name)
        if d["action"] == "accept" and f:
            f["status"] = "accepted"
        elif d["action"] == "reject" and f:
            f["status"] = "rejected"
        elif d["action"] == "edit":
            record["fields"][name] = field(d["value"], "human", d.get("by", "reviewer"), 1.0, "accepted")
    record["reviewed"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    return record


def accepted(record: dict) -> dict:
    """Field values a publish may use: accepted ones only."""
    return {k: f["value"] for k, f in record["fields"].items() if f["status"] == "accepted" and f["value"] not in (None, "", [])}
