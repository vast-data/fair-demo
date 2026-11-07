# Before production

The demo's guardrails are real, but they aren't everything a production service needs. This lists what the
demo does today and what to add, roughly in priority order. The portal's Help, under Production, shows the same table.

| Area | In the demo | Needed for production |
|---|---|---|
| Model scope | Proposes descriptive fields only; never licence, creators, identifiers or dates. Confidence capped at 0.9. | Keep the boundary, and add the items below; the boundary limits which fields the model touches, not whether its values are right. |
| Untrusted input | File headers and README/CITATION text go into the prompt as facts. | Delimit them as data, cap their length, validate output against a schema, and flag proposals that only restate a document's claims (prompt injection). |
| Where data goes | Names, header values and README text are sent to the model gateway. | Open data: fine. Embargoed, personal or export-controlled data: an on-site model, classification before any prompt, and redaction. |
| Confidence | The model's own estimate; one real error (simulation read as collision data) was at 0.7. | Calibrate on a larger labelled set, per field and domain, before any automatic acceptance. |
| Vocabularies | Keywords and domain are free text; CF standard names come from the files. | Map keywords and domains to GCMD, EDAM, PhySH (or the community's own) with identifiers, validated rather than generated. |
| Provenance | The model name and the curation software are recorded. | Record the model version, prompt version and settings per proposal, so any record can be explained and re-created. |
| Quality over time | An accuracy scorecard against ground truth, and F-UJI, run by hand. | Re-run both on every model or prompt change and on a schedule; alert on drops. |
| Reviewers | A typed name. | Sign-in (SSO or ORCID), roles for curators and data owners, and every decision tied to a person. |
| Facts mapping | Header-to-field rules written for six formats. | Review by a domain expert for each format and community convention in use. |
| Identifiers | Test ARKs (ark:/99999/fk4…) resolved by the portal. | DataCite DOIs (or the institution's PID service), with landing pages on a stable public host. |
| Access | Open datasets; files served from S3 through the portal. | Restricted data: access conditions in the record, authorisation at download, and metadata-only publication where needed. |
| Preservation | A VAST snapshot per publication, not locked. | Locked (indestructible) snapshots or replication, with a retention policy. |
| Scale | The function downloads whole files; search uses VAST Vector Search in VAST DataBase. | Header-only or ranged reads for large files. |
| Operations | One portal replica; a personal model key. | A service-account key, more than one replica, monitoring of the pipeline, dead-letter topic and Catalog runs, and backups of fair-db. |
| Catalog of catalogs | One site's catalog, searchable in its own portal. | One option, while the requirement is still being defined: each site publishes its catalog changes (a dataset published, updated or withdrawn, with its record) as events on a topic on its own VAST Event Broker, which speaks the Kafka protocol. A central catalog, or any partner site, subscribes and keeps the same tables with a site column, re-embedding with one model so search ranks across sites, and links back to each site for access. The broker keeps an ordered, replayable history, so a new subscriber catches up from the start; a DCAT-AP feed can be generated from the same events for harvesters that pull. |

## Notes

- **The model boundary** (descriptive fields only) is enforced in `fairdemo/curate.py` (`INFERABLE`,
  `FACT_ONLY`, `MAX_INFERRED_CONFIDENCE`) and in publishing (`fairdemo/rocrate.py`, `REQUIRED_TO_PUBLISH`).
  Keep it: it is why the scorecard finds no invented licences or creators.
- **Untrusted input** is the most important addition. Every README, CITATION file and header attribute is
  someone else's text sent to a model; treat it the way a web application treats form input.
- **Calibration** needs labelled examples from the communities the service will serve. Start with
  `data/ground_truth.json` and `scripts/scorecard.py`, and grow the set per domain.
- **Known issues** are in `docs/known-issues.md`. One matters for anyone scripting VMS: identity policies
  are attached per tenant (`users/<id>/tenant_data/`), or the Query Engine refuses every query.
- **A catalog of catalogs** needs no change to curation: the events would come from the publish step
  (`fair.records` already holds every version, with who made it), and the central catalog runs the same
  search code over the rows it receives. The demo already runs the Event Broker, for its triggers; it does
  not publish catalog events yet.
