# Known issues and setup traps

## Every Query Engine query is refused: identity policy attached in the wrong tenant

**Symptom.** Ordinary VAST DB reads and writes work, but every query through the VAST Query Engine, even
`SELECT 1`, and every `vector_search()`, fails:

```
Query Status Keepalive Failure: Get query status request failed. (Request GetQueryStatusRequest(...) failed
Server responded with error: ServerErrorResponse { error_code: Other("AccessDenied"),
message: "[req=0x…]: request was denied according to policies res=2", ... })
```

**Cause.** VMS keeps a user's identity policies *per tenant*, in `users/<id>/tenant_data/?tenant_id=<tenant>`.
For a user of the cluster's default local provider, `PATCH /api/users/<id>/` with `s3_policies_ids` (and
`vcli user add|modify --identity-policies-ids` without `--tenant-id`) attaches the policy in the **default**
tenant, where it doesn't apply to work in your own tenant. A user that owns its buckets still gets Tabular
access through ownership, which hides the problem; the Query Engine needs the policy, so it refuses
everything. `res=2` means no policy statement allowed the request. A user that doesn't own the bucket shows
the same mistake as a 403 on `HEAD <bucket>`, although its policy allows `s3:ListBucket`.

**Fix** (what `deploy/de.py`, step `s3_policy`, does):

```
PATCH /api/users/<id>/tenant_data/   {"tenant_id": <tenant id>, "s3_policies_ids": [<policy id>]}
```

or `vcli user modify --id <id> --tenant-id <tenant id> --identity-policies-ids <ids>`. Check with
`GET /api/users/<id>/tenant_data/?tenant_id=<tenant id>`; the user's own record (`GET /api/users/<id>/`)
shows the default tenant's attachments only.

`scripts/qe_replicate.py` sets up the same thing in the default tenant and in a tenant of its own, and runs a
Tabular select, Query Engine `SELECT 1` and a vector search as each tenant's user. `QE_ATTACH=user`
reproduces the mistake.

## NFS listing doesn't show folders created over S3

Over an NFSv3 mount of `fair-raw`, the bucket owner's uid saw files written over NFS but not the folders
created by S3 uploads, while S3 listed both. Curation is unaffected, since triggers and the pipeline work
from S3 keys. The likely cause is the S3-native view flavor's permissions on S3-created directories. A
view policy that must serve both protocols to users may want the Mixed Last Wins flavor instead.

## NFSv3 writes don't fire element triggers

By design, element triggers see NFSv4 writes only (VAST KB: *Provisioning Source Views for Element Type
Triggers*). Mount with `vers=4.1`. The view needs S3 and NFS4 with delegations off
(`allowed_delegations: NONE`), and the trigger needs source type `NFS4`. `de.py` sets all of that up.

## Search falls back to word matching when the embedding model is unavailable

Search by meaning embeds the query with `FAIR_EMBED_MODEL` through the model gateway. If the gateway refuses
(for example LiteLLM's `403 team_model_access_denied`, which it returns both when a key has no access to a
model and when the name doesn't exist), the portal matches the query's words against published records
instead, and says so under the search box. `GET /api/models` lists the models the deployment's key may use.
Publishing is unaffected; datasets published while the model is unavailable aren't indexed for search until
`POST /api/reindex`.

To fix: set `FAIR_EMBED_MODEL` in `.env` to a model the key can use, redeploy (`uv run deploy/de.py up portal
secret`), then reindex. The search table expects 1024-dimension vectors (`fairdemo/catalog.py`, `DIM`); a
model with another size needs the table recreated.
