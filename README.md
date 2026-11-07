# FAIR curation on the VAST Data Platform

Drop a folder of research data onto VAST. Minutes later it has a machine-readable record (RO-Crate and
Croissant, next to the data), a measured FAIR score, an identifier and a landing page; it can be found
by meaning, and the data itself can be queried in place as VAST DB tables. A person approves what the model
proposed, and never has to type what the data already says.

**FAIR** data is **F**indable, **A**ccessible, **I**nteroperable and **R**eusable: described in a standard,
machine-readable way, with a lasting identifier, so that people and software, including AI and ML tools, can
find it, understand it and use it again. It's the international standard for research data, and what funders
such as NIH, Horizon Europe and UKRI ask for.

This is a working demonstration, not a VAST product. It shows how VAST DataEngine, VAST DB (with vector
search), VAST Catalog and snapshots fit together to curate data where it already lives.

```
researcher / HPC job ──S3 or NFSv4──▶ s3://fair-raw/<domain>/<dataset>/
   │ DataEngine element trigger → Kafka topic on the VAST Event Broker → DataEngine function fair-curate
   ├─ each data file     profile: format, header, variables, coverage, sha256      → fair-db/fair/files
   ├─ <dataset>/_READY    curate: header facts + researcher's docs + model proposals → fair-db/fair/records
   │                      (every field carries source, evidence, confidence)         + _fair/record.json
   └─ _fair/review.json   publish: VAST snapshot, the data as tables in fair-db/data (one per part),
                          ro-crate-metadata.json, croissant.json, landing page, identifier, FAIR score;
                          indexed in fair-db/fair/search (vectors) and fair/variables
portal ── reads _fair/record.json, writes _fair/review.json, searches by meaning and by variable,
          queries the tables, lists the backlog from VAST Catalog, resolves /id/<ark>
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
  search by meaning and by variable (VAST Vector Search in VAST DataBase), finding the data nobody has described
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
VAST-specific features (Catalog, snapshots, VAST Vector Search) works this way.

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
jobs: the landing and estate views serve S3 and NFS, and NFS4 DataEngine element triggers start curation on files
written over NFS. Known issues are in [docs/known-issues.md](docs/known-issues.md). What a production
service would need beyond the demo is in [docs/PRODUCTION.md](docs/PRODUCTION.md).

## Search

Enter in the portal's search box ranks published datasets by meaning: embeddings of title, description,
keywords and variables, searched with VAST Vector Search in VAST DataBase, with a small boost for query words found in
the title, keywords, domain or description. *Find a variable* matches names, CF standard names, units and
descriptions in `fair.variables` across every published dataset. For example, `sea_surface_temperature`
finds OISST's `sst` and `anom`; `Muon_pt` finds the CMS ntuple.

## Query the data in place

Publishing also loads the data itself into VAST DB, one table per part, so a published dataset can be used
without downloading its files:

| Data | Table | Rows |
|---|---|---|
| OISST (NetCDF) | one row per ocean grid cell: time, lat, lon, sst, anom, err, ice | 691,150 |
| TESS light curve (FITS) | one row per cadence: time, fluxes, quality, centroids | 20,076 |
| gnomAD chr21 (VCF) | one row per variant: chrom, pos, ref, alt, filter, the first 193 INFO fields | 3,323 |
| CMS H→ZZ→4ℓ (ROOT) | events (flat branches), and one row per muon and per electron, keyed by event | 299,973 · 441,394 · 423,453 |
| DfT traffic counts (CSV) | one row per count point and year | 37,338 |

Every column keeps what curation established: units, CF standard name and the description a person
accepted. The record names each table's source file and SHA-256, so every row traces back to the published
bytes and the snapshot that preserves them. Images (TIFF) stay as files.

- **The curator chooses at publish.** *Also load as tables in VAST DataBase* is on by default for tabular
  files; a curator can publish as files only. The files stay where they are either way (`tables` in the
  review, `tables_wanted` in the record).
- **Six example questions per dataset** are made by the pipeline when the tables are loaded, each with its
  query already checked against the tables, and kept in the record (`questions`), so the portal shows them
  at once and clicking one runs without a second model call.
- **In the portal**, a published dataset has *Ask this dataset*: a question in plain language, the suggested
  questions, or *build the query by hand*: pick columns, add filters, run. Only
  the chosen columns of matching rows come back (projection and predicates are pushed down to VAST DB), with
  statistics over every match and the same question as SQL for Trino
  (`SELECT … FROM vast."fair-db/data".<table> WHERE …`). VAST runs Trino as a managed application on the
  cluster's own CNodes (5.4 and later; DataEngine → Applications in VMS), so a research centre gets SQL
  over its published data without standing up a query cluster of its own, and VAST identity policies
  decide which rows and columns each user sees. The demo shows the SQL but doesn't deploy Trino: the portal and
  the HTTP API read the tables through the VAST DB Python SDK (`vastdb`), as a notebook or job can.
- **Or ask in your own words** ("average traffic on the M4 by year", "warmest 5 places in the Gulf of
  Mexico"). The model sees only the tables' curated metadata (each column's units, reviewed meaning, range
  and commonest values) and proposes columns, filters, an aggregate and a sort as JSON: never code or SQL.
  The proposal is checked like any other query, sent back once with the reason if it doesn't fit (an
  unknown column, an empty range), and shown as ordinary controls with the model's one-line reading, so a
  person sees and can change how the question was read. Arithmetic the model is unreliable at, such as
  western longitudes in a 0–360° table, is done by the portal. On the demo data, 16 test questions across
  the five domains were all read sensibly with `octo/chat-medium`, and an unrelated one ("who funded
  this?") was declined.
- **Over HTTP**, for any tool: `/id/<ark>/tables/<table>?columns=lat,lon,sst&where=lat>=50&where=lat<=60&limit=1000&format=csv`
  (or `json`, `parquet`), with `agg=avg:sst&by=year` and `order=-sst` for aggregates and sorting. Responses carry `Link: rel="cite-as"` and the source file's checksum. Each table
  is a `DataDownload` in the RO-Crate and listed on the landing page, so FAIR assessors and harvesters see it.
- **Tables never hold up publishing.** They load after the dataset is published, so a file that is too
  large (over 512 MB, 30M cells or 5M rows by default: `FAIR_TABLE_MAX_MB`, `FAIR_TABLE_MAX_CELLS`), unreadable,
  or refused by the store stays a file, and the portal says why. Budgets are checked from file metadata
  before reading; a dataset's loading stops starting new tables after 300 s (`FAIR_TABLE_SECONDS`). One bad
  column, branch or file is skipped, not the dataset. Columns are stored as portable types (unsigned
  integers as int64, decimals as float64, times as text; binary and nested columns left in the file).
  Messy CSVs still load: mixed-type columns fall back to text, ragged rows are skipped, Latin-1 is read.
  If the function stops part-way, redelivering the review (or *Try loading again* in the portal) loads
  only the tables.
- **Filters are parsed, never executed.** `column op value`, with the value cast to the column's type;
  unknown columns and values that don't fit are refused.

Examples from the demo data: the mean sea-surface temperature in a North Atlantic box (lat 50–60, lon
320–350: 4,798 cells); muons with pt ≥ 20 GeV (214,041 of 441,394); gnomAD variants with AF ≥ 0.05; M4
traffic counts in 2024. In local mode (Parquet) each takes under 20 ms.

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

## Walkthrough

The portal has two views. **Curate** is where records are drafted, reviewed and published: one list on the
left, one line per dataset saying what state it is in; on the right the dataset, its FAIR score before and after,
and the facts, each with a coloured dot and one line saying where it came from. Facts are quiet; anything that
waits on a person is highlighted with Accept and Reject beside it. **Find data** is for the people who then look
for and use the data: a search box as the home page, results ranked by meaning, and on each dataset an *Ask this
dataset* card with suggested questions, then tabs for the facts, variables, files, code and similar datasets.
**Help → Walkthrough** goes through both with the demo datasets (about 15 minutes):

1. **The list** (Curate): one line per dataset; the footer counts folders with data but no record, and shows the
   pipeline's last event.
2. **Add a folder** (optional): *+ Add*, name it, drop files; a draft appears in the list within a minute.
3. **A documented dataset** (`climate/oisst-v2.1-2024-09-01`): where each fact came from: the NetCDF header,
   the folder's CITATION.cff, or the model (with its evidence and how sure it was).
4. **A dark dataset** (`particle-physics/user-skims-07`): described by the model from branch names, but not
   publishable until a person supplies licence and creators; noted as a duplicate of the CMS dataset.
5. **Publish**: accept or edit the highlighted rows, choose whether to also load the data as tables in VAST
   DataBase (on by default; the files stay where they are either way), publish. A snapshot, RO-Crate and
   Croissant next to the data, then the tables and six example questions for them.
6. **Find it** (Find data): the home page's numbers, then `ocean temperature from satellites`, or a variable such
   as `sea_surface_temperature`.
7. **Ask it**: on the DfT dataset, click a suggested question, or ask `average traffic on the M4 by year`; check
   how it was read; adjust it.
8. **The rest of the page**: facts and citation, variables, files, the same query from Python, HTTP or SQL, similar
   datasets; the landing page (HTML with schema.org for people, RO-Crate for machine clients).
9. **The backlog** (Curate): the footer's *Review them*; curate a folder in place.
10. **Under the hood**: Help → How it works (the architecture, with the Event Broker option for a catalog of
    catalogs), Data model (the VAST DB tables) and Production (what the demo leaves out).

## Works with the research assistant

[vast-research-assistant-agent](https://github.com/vast-data/vast-research-assistant-agent) is the
complement. This demo describes the data: it extracts metadata from data files where they sit, a curator
approves it, and it publishes FAIR records. InsightEngine, which the research assistant uses, indexes the
content: it chunks and embeds documents so an agent can answer questions about what they say. Both run on
DataEngine triggers, VAST DB vectors and Kubernetes.

- **Approved records feed the assistant.** Each publish writes the dataset's landing text to
  `s3://fair-docs`; an InsightEngine pipeline on that bucket makes curated, citable descriptions
  answerable in chat.
- **The catalog as an agent tool** (not built): alongside its Auto-Ontology database tool, the agent
  could search this catalog ("datasets with sea-surface temperature in kelvin since 2020") and return
  approved records with links.

## Not built yet

- **Ask questions of the data**: needs InsightEngine deployed over `s3://fair-docs` (see above).
- **Identifiers** are test ARKs (`ark:/99999/fk4…`); a deployment would mint DataCite DOIs.
- **Downloads from the snapshot**: a changed file is refused (409) with the snapshot named, but not yet
  served from it; the snapshot is readable over NFS at its access path.
- **A catalog of catalogs** (the requirement is still being defined): each site could publish "dataset
  published, updated or withdrawn" events, with the record, to a topic on its own VAST Event Broker (the Kafka
  protocol, which the demo already uses for its triggers). A central catalog, or partner sites, subscribe and
  replay the history to catch up, so one search spans every site; a DCAT-AP feed can be generated from the same
  events for harvesters (see [docs/PRODUCTION.md](docs/PRODUCTION.md)).
- Dataset folders are flat (`<domain>/<dataset>/<files>`).

## Licence, citation and contributing

- **Code:** Apache License 2.0 ([LICENSE](LICENSE)).
- **Data:** not included; each source keeps its own licence, listed with its attribution in
  [data/SOURCES.md](data/SOURCES.md).
- **Citing:** [CITATION.cff](CITATION.cff) (GitHub shows a "Cite this repository" button).
- **Contributing:** see [CONTRIBUTING.md](CONTRIBUTING.md).
