# FAIR curation on the VAST Data Platform

Drop a folder of research data onto VAST. Minutes later it has a machine-readable record (RO-Crate and
Croissant, next to the data), a measured FAIR score, an identifier and a landing page, and it can be found
by meaning. A person approves what the model proposed, and never has to type what the data already says.

**FAIR** data is **F**indable, **A**ccessible, **I**nteroperable and **R**eusable: described in a standard,
machine-readable way, with a lasting identifier, so that people and software, including AI and ML tools, can
find it, understand it and use it again. It's the international standard for research data, and what funders
such as NIH, Horizon Europe and UKRI ask for.

This is a working demonstration, not a VAST product. It shows how VAST DataEngine, VAST DB (with vector
search), VAST Catalog and snapshots fit together to curate data where it already lives.

```
researcher / HPC job ──S3 or NFSv4──▶ s3://fair-raw/<domain>/<dataset>/
   │ element trigger → Kafka topic on the VAST Event Broker → DataEngine function fair-curate
   ├─ each data file     profile: format, header, variables, coverage, sha256      → fair-db/fair/files
   ├─ <dataset>/_READY    curate: header facts + researcher's docs + model proposals → fair-db/fair/records
   │                      (every field carries source, evidence, confidence)         + _fair/record.json
   └─ _fair/review.json   publish: VAST snapshot, ro-crate-metadata.json, croissant.json, landing page,
                          identifier, FAIR score; indexed in fair-db/fair/search (vectors) and fair/variables
portal ── reads _fair/record.json, writes _fair/review.json, searches by meaning and by variable,
          lists the backlog from VAST Catalog, resolves /id/<ark>
```

## What makes it credible

- **Facts before inference.** Headers (ACDD/CF, FITS, VCF, OME, ROOT) and the researcher's own README /
  CITATION.cff / LICENSE are taken as they are. The model only proposes descriptive fields (title,
  description, keywords, domain, technique, coverage, variable meanings), capped at 0.9 confidence.
  **It never supplies creators, licence, publisher or identifiers.**
- **Publishing is gated.** Title, description, licence and creators must be accepted; RO-Crate itself
  requires a licence. Dark data with no licence stops at `needs-review`, with the reason shown.
- **The score is deterministic.** `fairdemo/fairscore.py` has 18 checks keyed to the RDA FAIR Data
  Maturity Model indicators. There is no model in the loop, and every point traces to one check.
- **Standard outputs, validated.** `rocrate-validator` (profile ro-crate-1.1) and `mlcroissant validate`
  pass for every published dataset.
- **An audit trail.** `fair.records` is append-only: each curation and review is a version, with who
  did it.
- **Duplicates are caught.** VAST DB finds the same bytes (sha256) in other folders before a second
  copy is published.

## Open standards, no lock-in

The metadata belongs to the data, not to the platform:

- Records are plain files in open standards, written **next to the data**: RO-Crate 1.1, Croissant 1.0,
  schema.org JSON-LD and PROV-O. They stay readable, and valid, if the data ever moves off VAST.
- FAIR is measured two ways: the built-in RDA-based checks, and **F-UJI**, an independent, open-source
  assessor.
- The curation logic (`fairdemo/`) is plain Python with open-source dependencies. It runs anywhere,
  including on a laptop with no cluster ([below](#run-it-without-a-cluster)).
- What VAST adds is the platform around it: event-driven processing where the data lands (DataEngine),
  search by meaning and by variable (VAST DB with its vector index), finding the data nobody has described
  (VAST Catalog), and preserving what was published (snapshots). The "How it works" page in the portal's
  Help shows which part does what.

## Demo data (all licences allow reuse, including commercially)

Six domains, from `data/SOURCES.md` (provenance, sha256 and licence evidence for each file). The data
isn't in this repository: `scripts/fetch_data.py` downloads each file from its original public source and
checks it against `data/sources.json`. In `data/staged/`, each domain then has a *documented* folder with
a README and CITATION.cff, and four domains also have a *dark* copy: the same bytes renamed, with no docs.

| Domain | Data | Format | Licence |
|---|---|---|---|
| climate | NOAA OISST v2.1, 2024-09-01 | NetCDF | CC0 / US Gov |
| astronomy | TESS sector 1 light curve, TIC 261136679 | FITS | public domain |
| particle physics | CMS H→ZZ→4ℓ reduced NanoAOD (CERN Open Data 12361) | ROOT | CC0 |
| genomics | gnomAD v2.1.1 exomes, chr21 slice | VCF (bgzip) | CC0 |
| bioimaging | BBBC039 U2OS nuclei, 3 fields | TIFF | CC0 |
| public sector | DfT annual average daily flow, Wales | CSV | OGL v3 |

Before → after publishing: documented folders score 24–52 → 72–92; a dark file scores 8 → 76 once a
person adds the licence and creator. `data/SOURCES.md` gives the attribution each source asks for.

## Run it without a cluster

The same function code runs in-process, over folders on disk (`data/buckets/`). Everything except
VAST-specific features (Catalog, snapshots, the vector index) works this way.

```bash
uv venv --python 3.12 && uv pip install -e '.[dev,portal,function]'
.venv/bin/python -m pytest -q        # offline, no cluster and no model needed
python3 scripts/fetch_data.py        # the six datasets, checked against data/sources.json, then staged
cp .env.example .env                 # set LITELLM_BASE_URL, LITELLM_API_KEY and the two model names
FAIR_STORE=local .venv/bin/uvicorn portal.app:app --port 8080
python3 scripts/upload.py            # uploads every folder in data/staged through the portal
```

Without a model gateway, `fairdemo curate FOLDER --no-llm` still produces a facts-only record.

## Run it on a VAST cluster

Follow **[docs/SETUP.md](docs/SETUP.md)**. In short: install DataEngine's services on a Kubernetes cluster
you run (`vastde install`), fill in `.env` from `.env.example`, then run `uv run deploy/de.py up`. It
creates everything in its own tenant, links your cluster as DataEngine compute, and deploys the function,
pipeline and portal. Every step can be re-run, and `uv run deploy/de.py down` removes it all again.

Data arrives over S3 (uploads, or `scripts/upload.py` with chunked, retried parts) or **NFSv4** from HPC
jobs: the landing and estate views serve S3 and NFS, and NFS4 element triggers start curation on files
written over NFS. Known issues are in [docs/known-issues.md](docs/known-issues.md). What a production
service would need beyond the demo is in [docs/PRODUCTION.md](docs/PRODUCTION.md).

## Search

Enter in the portal's search box ranks published datasets by meaning: embeddings of title, description,
keywords and variables, searched with VAST DB's vector index, with a small boost for query words found in
the title, keywords, domain or description. *Find a variable* matches names, CF standard names, units and
descriptions in `fair.variables` across every published dataset. For example, `sea_surface_temperature`
finds OISST's `sst` and `anom`; `Muon_pt` finds the CMS ntuple.

## Backlog: dark data already on the system

Upload is only half the story; most data is already sitting on storage. **Backlog** in the portal queries
**VAST Catalog** (a periodic snapshot of every file in the namespace) through VMS as the tenant's admin, so
the answer is scoped to the tenant. It groups files into folders and lists those that hold data but have no
FAIR record, with size, formats, any README/CITATION/LICENSE, and age:

- `dark`: nobody has described it. **Curate** drops `_READY` in the folder.
- `queued` → `review` → `curated`: the same DataEngine function curates it **in place** (dataset id
  `<bucket>/<prefix>`), a person reviews it, and the crate is written next to the data.

`s3://fair-estate` is the demo estate, a view shared over **S3 and NFS**. Its triggers fire only for
`_READY` and `review.json`, so data written there is left alone until someone asks.
`scripts/seed_estate.py` fills it with the demo data under project, scratch and instrument paths.

## Preservation and download

Publishing takes a **VAST snapshot of the dataset folder** (through VMS as the tenant admin) before
writing the crate, so the crate names the snapshot that holds the published bytes (`archivedAt`). Every
file in the crate and in Croissant has an **HTTP `contentUrl`** (`/id/<ark>/files/<name>`), with the S3
object as `sameAs`. The portal streams the file with a `Digest: sha-256=…` header carrying the published
checksum. If the live object has changed since publication, it refuses with **409** and returns the
published checksum and the snapshot instead, so one identifier never serves two versions. The demo doesn't
lock its snapshots (`indestructible` can't be undone); a production service should.

## Measured, not claimed

**Accuracy against ground truth** (`scripts/scorecard.py`; `data/ground_truth.json` comes from the
providers' own pages, listed in `data/SOURCES.md`). The pipeline's own drafts, before any human edits, for
the 10 `fair-raw` folders, with a general-purpose chat model on an on-site gateway, on two test clusters:

- **62 of 64 graded fields correct on both.** On one, 0 were wrong and 2 were left for a person (no
  technique stated for gnomAD or the dark ROOT file). On the other, 1 was wrong: the dark ROOT file can't
  show it's a simulation, and the model guessed "collision-based event reconstruction", at 0.7 confidence,
  below its confident fields. That's the case review is for.
- **0 invented facts in 7 checks** where nothing in the folder states the licence or creators.
- By source: file headers 5/5, researchers' docs 12/12, model proposals 38/38.
- Caveats: the rules check substance (terms present), not wording. Titles and descriptions aren't graded,
  because that's the reviewer's job. With one wrong model value, this sample can't show whether low
  confidence predicts errors.

**Independent FAIR assessment** with F-UJI 3.5.0 (`scripts/fuji.py`):

| Dataset | Our score | F-UJI | F | A | I | R |
|---|---|---|---|---|---|---|
| bbbc039-u2os-nuclei-sample | 88 | 87.5 | 71 | 100 | 100 | 90 |
| oisst-v2.1-2024-09-01 | 92 | 87.5 | 71 | 100 | 100 | 90 |
| gnomad-v2.1.1-exomes-chr21-slice | 76 | 87.5 | 71 | 100 | 100 | 90 |
| cms-higgs-zz-4l-8tev | 80 | 83.3 | 71 | 100 | 75 | 90 |
| dft-aadf-wales | 84 | 87.5 | 71 | 100 | 75 | 100 |
| export-2025-q3 (dark, completed by a person) | 76 | 87.5 | 71 | 100 | 75 | 100 |
| estate copies (TESS, OISST ×2) | 84–92 | 83.3–87.5 | 71 | 100 | 75–100 | 90 |

The first F-UJI run gave OISST 64.6. Its findings led to real metadata fixes: a publisher (the repository,
when the data names none), open-access conditions taken from the licence, HTTP-only download links in the
schema.org metadata, and PROV-O for the curation activity. Findable stops at 71 because of the demo
environment: test ARKs aren't resolvable persistent identifiers, and a demo portal isn't listed in DataCite
or Google Dataset Search. A DataCite DOI on a public host would lift it.

## Demo script (≈10 min)

1. **The problem.** Show `data/staged/public-sector/export-2025-q3/`: one CSV called `export_4.csv`, no
   README. Its FAIR score is 8.
2. **Drop it in.** Upload a folder in the portal (or run `scripts/upload.py`) and watch *Pipeline
   activity*: profile → curate, on DataEngine.
3. **Review a documented dataset** (OISST). Point out where each field came from: *from the file*
   (ACDD title, keywords, instrument), *from the researcher's docs* (8 authors, CC0, DOI), *proposed by
   the model* (domain, coverage). Publish: 52 → 92.
4. **Review a dark dataset** (`user-skims-07`). The model describes the physics correctly from branch
   names, but publishing is blocked: no licence or creators. *These are never guessed.* Note the
   duplicate warning: the same bytes are already published as the CMS outreach dataset.
5. **Open the landing page** (`/id/ark:/99999/fk4…`): human HTML with schema.org JSON-LD; RO-Crate for
   machines (`Accept: application/ld+json`); Croissant for ML loaders.
6. **Show the catalog** in VAST DB (`fair-db/fair/records`): every version, who changed it, scores.
7. **Backlog.** The dark folders VAST Catalog found in the estate. Curate one in place; it flags that the
   same bytes are already published from `fair-raw`.

## Works with the research assistant

[vast-research-assistant-agent](https://github.com/vast-data/vast-research-assistant-agent) is the
complement. This demo describes the data: it extracts metadata from data files where they sit, a curator
approves it, and it publishes FAIR records. InsightEngine, which the research assistant uses, indexes the
content: it chunks and embeds documents so an agent can answer questions about what they say. Both run on
DataEngine triggers, VAST DB vectors and Kubernetes.

- **Approved records feed the assistant.** Each publish writes the dataset's landing text to
  `s3://fair-docs`; an InsightEngine pipeline on that bucket makes curated, citable descriptions
  answerable in chat.
- **The catalogue as an agent tool** (not built): alongside its Auto-Ontology database tool, the agent
  could search this catalogue ("datasets with sea-surface temperature in kelvin since 2020") and return
  approved records with links.

## Not built yet

- **Ask questions of the data**: needs InsightEngine deployed over `s3://fair-docs` (see above).
- **Identifiers** are test ARKs (`ark:/99999/fk4…`); a deployment would mint DataCite DOIs.
- **Downloads from the snapshot**: a changed file is refused (409) with the snapshot named, but not yet
  served from it; the snapshot is readable over NFS at its access path.
- **A catalogue of catalogues**: a DCAT-AP feed per site, harvested centrally so one search spans every
  site's published datasets (see [docs/PRODUCTION.md](docs/PRODUCTION.md)).
- Dataset folders are flat (`<domain>/<dataset>/<files>`).

## Licence, citation and contributing

- **Code:** Apache License 2.0 ([LICENSE](LICENSE)).
- **Data:** not included; each source keeps its own licence, listed with its attribution in
  [data/SOURCES.md](data/SOURCES.md).
- **Citing:** [CITATION.cff](CITATION.cff) (GitHub shows a "Cite this repository" button).
- **Contributing:** see [CONTRIBUTING.md](CONTRIBUTING.md).
