"""Publish a reviewed record as standard, machine-readable metadata that sits next to the data:

  ro-crate-metadata.json   RO-Crate 1.1 (schema.org JSON-LD): the dataset, its files, people, provenance
  croissant.json           MLCommons Croissant 1.0: the same dataset, ready for ML tooling
  ro-crate-preview.html    a human landing page with the schema.org JSON-LD embedded, for search engines
                           and FAIR assessors such as F-UJI

Identifiers are ARKs on the reserved test shoulder ark:/99999/fk4…, which every ARK resolver treats as
test-only. A deployment would mint DOIs through DataCite instead.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import os
import re

from .curate import accepted

TEST_ARK_SHOULDER = "ark:/99999/fk4"
# The repository that publishes the datasets: their publisher when the data names none (as DataCite does).
PUBLISHER = os.environ.get("FAIR_PUBLISHER", "FAIR demo repository on the VAST Data Platform")
OPEN = {"CC0-1.0", "CC-BY-4.0", "CC-BY-3.0", "OGL-UK-3.0", "ODC-By-1.0", "PDDL-1.0", "CC-BY-SA-4.0"}

# Publishing needs these accepted (RO-Crate itself requires a licence); without them a person must act first.
REQUIRED_TO_PUBLISH = ("title", "description", "license", "creators")


class NotReady(ValueError):
    """The record lacks accepted values a published crate must have."""

    def __init__(self, missing):
        self.missing = missing
        super().__init__("needs a person before publishing: %s" % ", ".join(missing))

CROISSANT_CONTEXT = {
    "@language": "en", "@vocab": "https://schema.org/", "sc": "https://schema.org/",
    "cr": "http://mlcommons.org/croissant/", "rai": "http://mlcommons.org/croissant/RAI/",
    "dct": "http://purl.org/dc/terms/", "citeAs": "cr:citeAs", "column": "cr:column",
    "conformsTo": "dct:conformsTo", "data": {"@id": "cr:data", "@type": "@json"},
    "dataType": {"@id": "cr:dataType", "@type": "@vocab"}, "examples": {"@id": "cr:examples", "@type": "@json"},
    "extract": "cr:extract", "field": "cr:field", "fileObject": "cr:fileObject", "fileProperty": "cr:fileProperty",
    "fileSet": "cr:fileSet", "format": "cr:format", "includes": "cr:includes", "isLiveDataset": "cr:isLiveDataset",
    "jsonPath": "cr:jsonPath", "key": "cr:key", "md5": "cr:md5", "parentField": "cr:parentField", "path": "cr:path",
    "recordSet": "cr:recordSet", "references": "cr:references", "regex": "cr:regex", "repeated": "cr:repeated",
    "replace": "cr:replace", "samplingRate": "cr:samplingRate", "separator": "cr:separator", "source": "cr:source",
    "subField": "cr:subField", "transform": "cr:transform", "equivalentProperty": "cr:equivalentProperty",
}

CROISSANT_TYPES = {"integer": "sc:Integer", "number": "sc:Float", "date": "sc:Date", "string": "sc:Text"}


def check_publishable(record: dict) -> None:
    a = accepted(record)
    missing = [k for k in REQUIRED_TO_PUBLISH if not a.get(k)]
    if missing:
        raise NotReady(missing)


def mint_pid(record: dict) -> str:
    """Stable for a folder and its contents; two folders holding the same bytes still get their own."""
    basis = (record.get("key_prefix") or record["folder"]) + "".join(sorted(f["sha256"] for f in record["files"]))
    digest = hashlib.sha256(basis.encode()).hexdigest()
    return TEST_ARK_SHOULDER + digest[:10]


def _license_url(spdx: str | None) -> str | None:
    if not spdx:
        return None
    if spdx == "OGL-UK-3.0":
        return "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
    return "https://spdx.org/licenses/%s" % spdx if " " not in spdx else None


def _people(creators) -> list[dict]:
    out = []
    for i, c in enumerate(creators or []):
        c = c if isinstance(c, dict) else {"name": str(c)}
        ent = {"@id": c.get("orcid") or "#creator-%d" % i, "@type": "Person" if c.get("orcid") or " " in c["name"] and "," not in c["name"] else "Organization", "name": c["name"]}
        if c.get("affiliation"):
            ent["affiliation"] = c["affiliation"]
        if c.get("email"):
            ent["email"] = c["email"]
        out.append(ent)
    return out


def _variables(record: dict) -> list[dict]:
    out = []
    for v in record["variables"][:300]:
        desc = v.get("long_name") or ((v.get("description") or {}).get("value")
                                      if (v.get("description") or {}).get("status") == "accepted" else None)
        pv = {"@type": "PropertyValue", "name": v["name"]}
        if desc:
            pv["description"] = desc
        if v.get("units") or v.get("unit_inferred"):
            pv["unitText"] = v.get("units") or v.get("unit_inferred")
        if v.get("standard_name"):
            pv["propertyID"] = "http://vocab.nerc.ac.uk/standard_name/%s/" % v["standard_name"]
        out.append(pv)
    return out


def build(record: dict, base_url: str, s3_prefix: str) -> dict:
    """The three documents for a reviewed record. base_url serves the landing pages; s3_prefix the data."""
    check_publishable(record)
    a = accepted(record)
    pid = record.get("pid") or mint_pid(record)
    now = dt.datetime.now(dt.timezone.utc).date().isoformat()
    landing = "%s/%s" % (base_url.rstrip("/"), pid)
    license_url = _license_url(a.get("license"))
    people = _people(a.get("creators"))

    root = {
        "@id": "./", "@type": "Dataset", "identifier": pid, "url": landing,
        "name": a.get("title") or record["name"],
        "datePublished": a.get("datePublished") or now,
        "hasPart": [{"@id": f["name"]} for f in record["files"]],
        "distribution": [{"@id": "#s3"}],
    }
    for key in ("description", "keywords", "version", "measurementTechnique", "temporalCoverage", "spatialCoverage"):
        if a.get(key):
            root[key] = a[key]
    if a.get("domain"):
        root["about"] = a["domain"]
    if a.get("instrument"):
        root["instrument"] = a["instrument"]
    if license_url:
        root["license"] = {"@id": license_url}
    if people:
        root["creator"] = [{"@id": p["@id"]} for p in people]
    root["publisher"] = {"@id": "#publisher"}
    if a.get("license") in OPEN:
        # Stated by the licence itself, not guessed: open data, no registration.
        root["conditionsOfAccess"] = "Open access: no registration or login; reuse under %s" % a["license"]
        root["isAccessibleForFree"] = True
    if a.get("citation"):
        cites = a["citation"] if isinstance(a["citation"], list) else [a["citation"]]
        root["citation"] = [("https://doi.org/" + c) if str(c).startswith("10.") else c for c in cites]
    if a.get("isBasedOn"):
        root["isBasedOn"] = [{"@id": s} for s in a["isBasedOn"]]
    if a.get("conformsTo"):
        root["conformsTo"] = a["conformsTo"]
    # RO-Crate wants a flat graph: each variable is its own entity, referenced from the dataset.
    variables = _variables(record)
    var_entities = [{"@id": "#var-%s" % re.sub(r"[^A-Za-z0-9_.-]", "_", v["name"]), **v} for v in variables]
    if variables:
        root["variableMeasured"] = [{"@id": v["@id"]} for v in var_entities]

    graph = [
        {"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
         "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}, "about": {"@id": "./"}},
        root,
        {"@id": "#s3", "@type": "DataDownload", "contentUrl": s3_prefix.rstrip("/") + "/",
         "encodingFormat": a.get("encodingFormat"), "description": "S3 prefix on the VAST DataStore"},
        *people,
        *var_entities,
    ]
    if license_url:
        graph.append({"@id": license_url, "@type": "CreativeWork", "name": a["license"]})
    graph.append({"@id": "#publisher", "@type": "Organization", "name": str(a.get("publisher") or PUBLISHER)})
    for f in record["files"]:
        graph.append({"@id": f["name"], "@type": "File", "name": f["name"], "contentSize": f["size"],
                      "encodingFormat": f["media_type"], "sha256": f["sha256"],
                      "contentUrl": "%s/files/%s" % (landing, f["name"]),
                      "sameAs": "%s/%s" % (s3_prefix.rstrip("/"), f["name"])})
    snap = record.get("snapshot")
    if snap:
        # The published bytes, preserved: a VAST snapshot of the folder taken at publication.
        root["archivedAt"] = {"@id": "#snapshot"}
        graph.append({"@id": "#snapshot", "@type": "CreativeWork", "name": "VAST snapshot %s" % snap["name"],
                      "identifier": snap.get("guid"), "dateCreated": snap.get("created"),
                      "description": "Immutable copy of %s at publication; read-only at %s"
                                     % (snap.get("path"), snap.get("access_path"))})
    for s in a.get("isBasedOn") or []:
        if not any(g["@id"] == s for g in graph):
            graph.append({"@id": s, "@type": ["File", "SoftwareSourceCode"], "name": s})

    # Provenance of the metadata itself: which software and model proposed it, and who reviewed it.
    agent = {"@id": "#curator", "@type": "SoftwareApplication", "name": record["curator"]["software"]}
    if record["curator"].get("model"):
        agent["description"] = "metadata proposals by %s" % record["curator"]["model"]
    graph.append(agent)
    graph.append({"@id": "#curation", "@type": "CreateAction", "name": "FAIR curation",
                  "agent": {"@id": "#curator"}, "result": {"@id": "ro-crate-metadata.json"},
                  "endTime": record.get("reviewed") or record["generated"],
                  "description": "%d fields from file headers and companion documents, %d inferred and reviewed by a person" % (
                      sum(1 for f in record["fields"].values() if f["source"] in ("header", "companion")),
                      sum(1 for f in record["fields"].values() if f["source"] in ("inferred", "human")))})

    crate = {"@context": ["https://w3id.org/ro/crate/1.1/context",
                          {"sha256": "https://w3id.org/ro/terms/workflow-run#sha256"}], "@graph": graph}
    croissant = _croissant(record, a, pid, landing, license_url, people, s3_prefix)
    preview = _preview(root | ({"variableMeasured": variables} if variables else {}), people, record, crate)
    return {"pid": pid, "landing_page": landing, "ro-crate-metadata.json": crate,
            "croissant.json": croissant, "ro-crate-preview.html": preview}


def _croissant(record, a, pid, landing, license_url, people, s3_prefix) -> dict:
    doc = {
        "@context": CROISSANT_CONTEXT, "@type": "sc:Dataset", "conformsTo": "http://mlcommons.org/croissant/1.0",
        "name": a.get("title") or record["name"], "description": a.get("description") or "",
        "url": landing, "identifier": pid, "keywords": a.get("keywords") or [],
        "datePublished": a.get("datePublished") or dt.date.today().isoformat(), "version": a.get("version") or "1.0.0",
        "citeAs": "%s. %s. %s" % ("; ".join(p["name"] for p in people) or "Unknown",
                                   a.get("title") or record["name"], pid),
        "distribution": [], "recordSet": [],
    }
    if license_url:
        doc["license"] = license_url
    if people:
        doc["creator"] = [{"@type": "sc:" + p["@type"], "name": p["name"]} for p in people]
    for f in record["files"]:
        doc["distribution"].append({"@type": "cr:FileObject", "@id": f["name"], "name": f["name"],
                                    "contentUrl": "%s/files/%s" % (landing, f["name"]),
                                    "contentSize": "%d B" % f["size"], "encodingFormat": f["media_type"],
                                    "sha256": f["sha256"]})
    # Tabular files get a RecordSet with one field per column, so ML loaders can read them directly.
    for f in record["files"]:
        if f["media_type"] not in ("text/csv", "application/vnd.apache.parquet"):
            continue
        cols = [v for v in record["variables"] if f["name"] in v.get("files", [])]
        rs_id = f["name"].rsplit(".", 1)[0]
        doc["recordSet"].append({"@type": "cr:RecordSet", "@id": rs_id, "name": rs_id, "field": [
            {"@type": "cr:Field", "@id": "%s/%s" % (rs_id, c["name"]), "name": c["name"],
             "dataType": CROISSANT_TYPES.get(c.get("dtype"), "sc:Text"),
             "source": {"fileObject": {"@id": f["name"]}, "extract": {"column": c["name"]}}} for c in cols]})
    if not doc["recordSet"]:
        del doc["recordSet"]
    return doc


def _preview(root, people, record, crate) -> str:
    e = html.escape
    jsonld = {k: v for k, v in root.items() if k not in ("hasPart", "distribution", "archivedAt", "publisher")}
    jsonld["@context"] = {"@vocab": "https://schema.org/", "prov": "http://www.w3.org/ns/prov#"}
    pub = next((g for g in crate["@graph"] if g["@id"] == "#publisher"), None)
    if pub:
        jsonld["publisher"] = {"@type": "Organization", "name": pub["name"]}
    # Provenance of this record, in PROV-O: which software and model drafted it, who reviewed it, when.
    cur = record.get("curator") or {}
    jsonld["prov:wasGeneratedBy"] = {
        "@type": "prov:Activity", "name": "FAIR curation",
        "prov:endedAtTime": record.get("reviewed") or record.get("generated"),
        "prov:wasAssociatedWith": [{"@type": "prov:SoftwareAgent", "name": cur.get("software")}]
                                  + ([{"@type": "prov:SoftwareAgent", "name": cur["model"]}] if cur.get("model") else [])
                                  + ([{"@type": "prov:Person", "name": record["reviewer"]}] if record.get("reviewer") else [])}
    jsonld["@id"] = root["url"]
    if people:
        jsonld["creator"] = [{"@type": p["@type"], "name": p["name"], **({"sameAs": p["@id"]} if p["@id"].startswith("http") else {})}
                             for p in people]
    jsonld["distribution"] = [{"@type": "DataDownload", "contentUrl": g["contentUrl"], "encodingFormat": g["encodingFormat"]}
                              for g in crate["@graph"] if g.get("@type") == "File"]
    rows = "".join('<tr><td><a href="%s/files/%s">%s</a></td><td>%s</td><td>%s</td><td><code>%s…</code></td></tr>'
                   % (e(root["url"]), e(f["name"]), e(f["name"]), e(f["format"]), f"{f['size']:,}", f["sha256"][:16])
                   for f in record["files"])
    snap = record.get("snapshot")
    preserved = ("<p><b>Preserved:</b> VAST snapshot <code>%s</code>, taken %s at publication. The files above are "
                 "served from the live folder only while they match it.</p>" % (e(snap["name"]), e(str(snap.get("created"))[:19])))\
        if snap else ""
    lic = root.get("license", {}).get("@id", "")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{e(root['name'])}</title>
<link rel="cite-as" href="{e(root['url'])}">
<link rel="describedby" type="application/ld+json" href="{e(root['url'])}/ro-crate-metadata.json">
<script type="application/ld+json">{json.dumps(jsonld, default=str)}</script>
<style>body{{font:16px/1.5 system-ui,sans-serif;max-width:52rem;margin:2rem auto;padding:0 1rem;color:#1d1d1f;background:#fff}}
td,th{{text-align:left;padding:.25rem .75rem .25rem 0}}code{{background:#f2f2f2;padding:0 .25rem}}</style></head>
<body><p><code>{e(root['identifier'])}</code></p><h1>{e(root['name'])}</h1>
<p>{e(root.get('description', ''))}</p>
<p><b>Creators:</b> {e('; '.join(p['name'] for p in people) or 'not stated')}<br>
<b>Licence:</b> {f'<a href="{e(lic)}">{e(lic)}</a>' if lic else 'not stated'}<br>
<b>Keywords:</b> {e(', '.join(root.get('keywords') or []))}</p>
<table><tr><th>File</th><th>Format</th><th>Bytes</th><th>SHA-256</th></tr>{rows}</table>
{preserved}
<p>Machine-readable: <a href="{e(root['url'])}/ro-crate-metadata.json">RO-Crate</a> · <a href="{e(root['url'])}/croissant.json">Croissant</a></p>
</body></html>"""
