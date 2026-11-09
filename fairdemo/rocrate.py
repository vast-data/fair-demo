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

from . import theme
from .curate import accepted

TEST_ARK_SHOULDER = "ark:/99999/fk4"
# The repository that publishes the datasets: their publisher when the data names none (as DataCite does).
PUBLISHER = os.environ.get("FAIR_PUBLISHER", "FAIR demo repository on the VAST Data Platform")
# Where this site keeps files and tables, as its published records say. A site that isn't on VAST (Site B in the
# catalog-of-catalogs demo runs in local mode) names its own.
DATASTORE = os.environ.get("FAIR_DATASTORE", "the VAST DataStore")
TABLE_HOME = os.environ.get("FAIR_TABLE_HOME", "VAST DataBase")
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
         "encodingFormat": a.get("encodingFormat"), "description": "S3 prefix on %s" % DATASTORE},
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
    for t in record.get("tables") or []:
        # The data itself, queryable in place: a VAST DataBase table per tabular part, read over HTTP with
        # column selection and filters pushed down to VAST (and from SQL engines through VAST's connectors).
        tid = "#table-%s" % t["table"]
        root["distribution"].append({"@id": tid})
        graph.append({"@id": tid, "@type": "DataDownload", "name": "Table %s" % t["location"],
                      "contentUrl": "%s/tables/%s?format=csv" % (landing, t["table"]), "encodingFormat": "text/csv",
                      "contentSize": "%d rows" % t["rows"], "isBasedOn": {"@id": t["source"]},
                      "description": "%s as a %s table (%d rows, %d columns). Query it in place: "
                                     "?columns=a,b&where=a>=1&limit=100&format=csv|json|parquet"
                                     % (t["source"], TABLE_HOME, t["rows"], len(t["columns"]))})
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


LICENCE_NAMES = {"CC0-1.0": "CC0 1.0 (public domain)", "CC-BY-4.0": "CC BY 4.0", "CC-BY-3.0": "CC BY 3.0",
                 "OGL-UK-3.0": "Open Government Licence v3.0", "ODC-By-1.0": "ODC Attribution 1.0", "PDDL-1.0": "PDDL 1.0",
                 "CC-BY-SA-4.0": "CC BY-SA 4.0"}

# The landing page's look: the portal's palette, light and dark, with nothing loaded from elsewhere.
PAGE_CSS = """
:root{color-scheme:light} @media (prefers-color-scheme:dark){:root{color-scheme:dark}}
*{box-sizing:border-box}
body{margin:0;font:15px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--ink);background:var(--bg);-webkit-font-smoothing:antialiased}
a{color:var(--accent)} code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.9em}
.top{display:flex;align-items:center;gap:10px;padding:14px max(16px,calc((100vw - 60rem)/2 + 16px));border-bottom:1px solid var(--line);background:var(--panel)}
.top .logo{width:26px;height:26px;border-radius:7px;background:var(--accent);color:var(--accent-ink);display:grid;place-items:center;font-weight:800;font-size:13px;flex:none}
.top b{font-size:14px} .top .where{margin-left:auto;color:var(--muted);font-size:13px}
main{max-width:60rem;margin:0 auto;padding:28px 16px 48px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:28px 28px 24px;margin-bottom:18px}
.pid{display:inline-block;font-size:12.5px;color:var(--muted);background:var(--soft);border-radius:999px;padding:2px 10px}
h1{font-size:clamp(22px,3.4vw,30px);line-height:1.25;letter-spacing:-.02em;margin:12px 0 6px;text-wrap:balance}
.by{color:var(--muted);font-size:14px;margin:0 0 14px}
.desc{margin:0 0 18px;max-width:46rem}
.chips{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 20px;padding:0;list-style:none}
.chips li{font-size:12.5px;border:1px solid var(--line);border-radius:999px;padding:2px 10px;background:var(--panel)}
.acts{display:flex;flex-wrap:wrap;gap:10px 18px;font-size:14px}
.acts a.primary{background:var(--accent);color:var(--accent-ink);text-decoration:none;font-weight:600;border-radius:9px;padding:7px 14px}
.cite{margin-top:20px;padding:12px 14px;border-radius:10px;background:var(--soft);font-size:13.5px;overflow-wrap:anywhere}
.cite b{display:block;font-size:11.5px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:2px}
h2{font-size:17px;letter-spacing:-.01em;margin:0 0 6px} h3{font-size:14px;margin:22px 0 4px;overflow-wrap:anywhere}
.lead{color:var(--muted);font-size:14px;margin:0 0 14px;max-width:46rem}
.scroll{overflow-x:auto;border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;font-size:13.5px}
th{text-align:left;font-size:12px;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);font-weight:600;background:var(--soft);padding:8px 12px}
td{padding:7px 12px;border-top:1px solid var(--line);vertical-align:top} td.n{font-variant-numeric:tabular-nums;white-space:nowrap}
.meta{color:var(--muted);font-size:13px;margin:0 0 8px} .note{color:var(--muted);font-size:13px;margin:12px 0 0}
footer{color:var(--muted);font-size:13px;text-align:center}
@media (max-width:600px){.card{padding:20px 16px} .top .where{display:none}}
"""


def render_preview(record: dict, crate: dict) -> str:
    """The landing page for a published record, from its RO-Crate: so a page published earlier is shown in the
    current design too."""
    g = {x["@id"]: x for x in crate["@graph"]}
    root = dict(g["./"])
    people = [g[c["@id"]] for c in root.get("creator", []) if c.get("@id") in g]
    if root.get("variableMeasured"):
        root["variableMeasured"] = [{k: v for k, v in g[ref["@id"]].items() if k != "@id"}
                                    for ref in root["variableMeasured"] if ref.get("@id") in g]
    return _preview(root, people, record, crate)


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
                              for g in crate["@graph"] if g.get("@type") == "File" or str(g["@id"]).startswith("#table-")]
    url = root["url"]
    publisher = pub["name"] if pub else PUBLISHER
    lic = (root.get("license") or {}).get("@id", "")
    lic_id = next((g.get("name") for g in crate["@graph"] if g["@id"] == lic), None) or lic
    lic_html = f'<a href="{e(lic)}">{e(LICENCE_NAMES.get(lic_id, lic_id))}</a>' if lic else "licence not stated"
    names = [p["name"] for p in people]
    year = str(root.get("datePublished") or "")[:4]
    by = " · ".join(x for x in (e(", ".join(names[:6]) + (f" and {len(names) - 6} more" if len(names) > 6 else "")) if names else "",
                                e(year), lic_html) if x)
    cite = "%s (%s). %s. %s. %s" % (", ".join(names) or publisher, year or "n.d.", root["name"], publisher, url)
    keywords = root.get("keywords") or []
    keywords = keywords.split(",") if isinstance(keywords, str) else keywords
    chips = "".join(f"<li>{e(str(k).strip())}</li>" for k in keywords if str(k).strip())
    rows = "".join('<tr><td><a href="%s/files/%s">%s</a></td><td>%s</td><td class="n">%s</td><td><code title="%s">%s…</code></td></tr>'
                   % (e(url), e(f["name"]), e(f["name"]), e(f["format"]), _size(f["size"]), e(f["sha256"]), f["sha256"][:16])
                   for f in record["files"])
    snap = record.get("snapshot")
    preserved = ('<p class="note"><b>Preserved:</b> VAST snapshot <code>%s</code>, taken %s at publication. The files are '
                 "served from the live folder only while they match it.</p>" % (e(snap["name"]), e(str(snap.get("created"))[:19])))\
        if snap else '<p class="note">Each file is served only while it matches the checksum it was published with.</p>'
    tables = "".join(_table_html(url, t) for t in record.get("tables") or [])
    if tables:
        tables = ('<section class="card" id="query"><h2>Query it in place</h2><p class="lead">The data is also in %s, one table '
                  "per part, so it can be read without downloading the files: choose columns, filter rows, and get CSV, JSON "
                  "or Parquet. Units and meanings are the reviewed ones.</p>%s</section>" % (e(TABLE_HOME), tables))
    acts = (f'<a class="primary" href="#files">Download files</a>' + (' <a href="#query">Query it in place</a>' if tables else "")
            + f' <a href="{e(url)}/ro-crate-metadata.json">RO-Crate</a> <a href="{e(url)}/croissant.json">Croissant</a>')
    palette = theme.css(dark_selectors=(":root",), toggle=False)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{e(root['name'])}</title>
<link rel="cite-as" href="{e(url)}">
<link rel="describedby" type="application/ld+json" href="{e(url)}/ro-crate-metadata.json">
<script type="application/ld+json">{json.dumps(jsonld, default=str)}</script>
<style>{palette}{PAGE_CSS}</style></head>
<body><div class="top"><span class="logo" aria-hidden="true">{e(theme.logo())}</span><b>{e(PUBLISHER)}</b><span class="where">Dataset landing page</span></div>
<main>
<section class="card">
<span class="pid">{e(root['identifier'])}</span>
<h1>{e(root['name'])}</h1>
<p class="by">{by}</p>
<p class="desc">{e(root.get('description', ''))}</p>
{f'<ul class="chips" aria-label="Keywords">{chips}</ul>' if chips else ""}
<div class="acts">{acts}</div>
<div class="cite"><b>Cite as</b>{e(cite)}</div>
</section>
<section class="card" id="files"><h2>Files</h2>
<div class="scroll"><table><tr><th>File</th><th>Format</th><th>Size</th><th>SHA-256</th></tr>{rows}</table></div>
{preserved}</section>
{tables}
<footer>Machine-readable: <a href="{e(url)}/ro-crate-metadata.json">RO-Crate</a> · <a href="{e(url)}/croissant.json">Croissant</a> · published by {e(PUBLISHER)}</footer>
</main></body></html>"""


def _size(n: int) -> str:
    for unit in ("bytes", "kB", "MB", "GB", "TB"):
        if n < 1000 or unit == "TB":
            return f"{n:,} bytes" if unit == "bytes" else f"{n:.1f} {unit}"
        n /= 1000


def _table_html(base: str, t: dict) -> str:
    e = html.escape
    url = "%s/tables/%s" % (base, t["table"])
    cols = t["columns"][:40]
    # Units and meaning only when some column has them: an empty column says nothing.
    units = any(c.get("units") for c in cols)
    meaning = any(c.get("description") or c.get("standard_name") for c in cols)
    rng = lambda c: ("%s" % c["min"] if c["min"] == c["max"] else "%s to %s" % (c["min"], c["max"])) \
        if c.get("min") is not None else ("text" if c["type"] == "string" else c["type"])
    head = "<tr><th>Column</th>%s%s<th>Range</th></tr>" % ("<th>Units</th>" if units else "", "<th>Meaning</th>" if meaning else "")
    body = "".join("<tr><td><code>%s</code></td>%s%s<td class=\"n\">%s</td></tr>" % (
        e(c["name"]), "<td>%s</td>" % e(c.get("units") or "") if units else "",
        "<td>%s</td>" % e(c.get("description") or c.get("standard_name") or "") if meaning else "", e(rng(c))) for c in cols)
    more = '<p class="note">… and %d more columns.</p>' % (len(t["columns"]) - 40) if len(t["columns"]) > 40 else ""
    return (f"<h3><code>{e(t['location'])}</code></h3><p class=\"meta\">{t['rows']:,} rows from {e(t['source'])} · "
            f"<a href=\"{e(url)}?limit=100&format=csv\">first 100 rows (CSV)</a> · "
            f"<a href=\"{e(url)}?limit=100\">JSON</a></p>"
            f"<div class=\"scroll\"><table>{head}{body}</table></div>{more}")
