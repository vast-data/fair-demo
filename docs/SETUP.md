# Setting up the FAIR curation demo on a VAST cluster

This guide builds the whole demo on an existing VAST cluster, with DataEngine running on a Kubernetes cluster
you control. Every step is a command in this repository, and every step can be re-run: each one checks what
exists and creates only what's missing. Allow about two hours the first time, mostly waiting for image
builds and uploads.

It was built and tested on VAST 5.5.0 SP6 with DataEngine 5.5.0-sp2 on RKE2 v1.34, and the curation logic also
runs with no cluster at all (see the README, *Run it without a cluster*).

## What you'll have at the end

```
researcher / HPC job ──S3 or NFSv4──▶ s3://fair-raw/<domain>/<dataset>/        (landing view: S3 + NFS)
existing data                        s3://fair-estate/...                     (estate view: S3 + NFS)
   │ element triggers (S3 and NFS4 sources) → VAST Event Broker (Kafka view fair-kafka)
   ▼
DataEngine function fair-curate, on your Kubernetes cluster (linked to the tenant as external compute)
   ├─ each data file   → profile (format, header, variables, coverage, sha256) → VAST DB fair-db/fair/files
   ├─ <dataset>/_READY → curate: facts + researchers' docs + model proposals     → fair-db/fair/records
   └─ review.json      → VAST snapshot, ro-crate-metadata.json, croissant.json, landing page;
                         index for search (fair/search vectors, fair/variables); landing text → s3://fair-docs
portal (a Deployment on the same Kubernetes cluster, NodePort 30080): review, publish, search, backlog
   (VAST Catalog), identifier resolution, downloads
```

Everything on the VAST cluster lives in one tenant (`fair` by default), so nothing else on the cluster is touched.

## 1. Requirements

| What | Details |
|---|---|
| VAST cluster | 5.5 or later; cluster admin login for VMS, or a developer tenant (section 4, *In a developer tenant*). A Query Engine VIP pool (role `QUERY_ENGINE_CNODE_GROUP`, with a domain name, which needs the cluster DNS service) for native vector search. VAST Catalog enabled for the backlog view. |
| Kubernetes cluster | One you administer, for DataEngine and the portal. Single-node RKE2 v1.34 is enough (tested: 64 cores, 47 GB). Its nodes must reach the VAST cluster's **data network** (the VIPs). VMS must reach its API server on 6443. |
| Container registry | HTTPS with a **valid certificate**, reachable from the Kubernetes nodes and from your workstation for pushes. |
| Model gateway | OpenAI-compatible, with a chat model that can return JSON and a 1024-dimension embedding model (tested: LiteLLM serving Qwen3-Embedding-0.6B). Reachable from the Kubernetes pods. On-site models keep the researchers' text on your network. |
| Workstation | macOS or Linux with `uv`, `docker buildx` (builds `linux/amd64`), `skopeo`, `kubectl`, `python3`. Network access to VMS, the Kubernetes API, the registry and the portal. |
| Addresses | Free VIPs on the data network: **2 or more** for S3 and VAST DB, and, for a dedicated Event Broker pool, **one per CNode or more** (optional: without it the Event Broker shares the protocols pool). In a developer tenant, the pool you were given. |

Check the network from a throwaway pod before going further. Every line should print an HTTP status:

```bash
kubectl run netcheck --rm -i --restart=Never --image=curlimages/curl:8.10.1 --command -- sh -c '
for u in https://<VMS>/ http://<an existing S3 VIP>/ https://<registry>/v2/ <model gateway URL>; do
  curl -sk -m 8 -o /dev/null -w "$u %{http_code}\n" $u || echo "$u unreachable"; done'
```

## 2. Get the code and the data

```bash
git clone https://github.com/vast-data/fair-demo.git && cd fair-demo
uv venv --python 3.12 && uv pip install -e '.[dev,portal,function]'
.venv/bin/python -m pytest -q                 # all offline, no cluster needed
python3 scripts/fetch_data.py                 # ~150 MB from NOAA, MAST, CERN, gnomAD, BBBC, DfT; stages data/staged/
```

`data/SOURCES.md` lists each file's provenance, checksum, licence evidence and the attribution it requires.

## 3. Prepare the Kubernetes cluster

If you don't have one, install single-node RKE2 on a Linux host that sits on both your lab network and
the VAST data network:

```bash
sudo mkdir -p /etc/rancher/rke2
printf 'write-kubeconfig-mode: "0640"\ntls-san:\n  - <host IP>\n  - <host name>\nnode-ip: <host IP>\ndisable:\n  - rke2-ingress-nginx\n' \
  | sudo tee /etc/rancher/rke2/config.yaml
curl -sfL https://get.rke2.io | sudo INSTALL_RKE2_CHANNEL=v1.34 sh -
sudo systemctl enable --now rke2-server.service
```

Copy `/etc/rancher/rke2/rke2.yaml` to your workstation (for example `~/.kube/fair-k8s.yaml`) and replace
`127.0.0.1` with the host's address.

Install DataEngine's platform services (Knative, the DataEngine controller and collector) with the
[vastde CLI](https://github.com/vast-data/dataengine-cli/releases). Choose the release closest to your
cluster's version. Note that `--kubeconfig` comes **after** `install`:

```bash
vastde install --kubeconfig ~/.kube/fair-k8s.yaml --list-versions
vastde install --kubeconfig ~/.kube/fair-k8s.yaml --dry-run       # review the namespaces and images it will create
vastde install --kubeconfig ~/.kube/fair-k8s.yaml --wait --timeout 10m
```

Reference: KB, *Enabling DataEngine on a VAST Cluster Tenant*, section *Preparing External Compute Clusters*.

## 4. Configure `.env`

```bash
cp .env.example .env && chmod 600 .env
```

Fill in the model gateway, its key and the two model names, `VMS_ADMIN_PASSWORD`, and the `CLUSTER_*` values.
The tenant admin's password is generated for you unless you set `FAIR_TENANT_ADMIN_PASSWORD`. Choose VIPs carefully: list every pool's ranges, and ping each candidate. An address outside every
pool can still be in use (the compute cluster's kube VIP is one example).

```bash
curl -sk -u admin:"$VMS_ADMIN_PASSWORD" https://<VMS>/api/vippools/ | python3 -c \
  'import json,sys;[print(p["name"], p["ip_ranges"]) for p in json.load(sys.stdin)]'
```

`.env` is git-ignored. Never paste its values into a chat or a ticket.

### In a developer tenant

You don't need a cluster admin login. If your platform team gives you a
[developer tenant](https://docs.vastdata.com/develop/docs/dev-start-developer-sandbox), leave
`VMS_ADMIN_PASSWORD` empty and `de.py` does everything as that tenant's admin. It checks that the tenant, its
pool and its admin exist, creates the rest inside the tenant, and `down` leaves those three for the platform
team. Ask them for what that page lists, plus the demo's extras:

| Ask for | Why the demo needs it |
|---|---|
| A tenant with its own local provider | The bucket owner is a user in it; a tenant admin can't use the cluster's default provider. |
| A tenant admin login | Every step runs as it, DataEngine's included. |
| A Protocols VIP pool for the tenant | S3, VAST DB and the Event Broker all use it, so no separate broker pool is needed. |
| A Query Engine pool, and VAST Catalog on | Vector search, and the backlog view. |

Your own Kubernetes cluster, with DataEngine's services installed (section 3), and a container registry are
linked by the tenant admin, as in the main path. Then set, instead of the cluster admin and the VIP ranges:

```bash
FAIR_TENANT=<your tenant>
FAIR_TENANT_ADMIN_USER=<the tenant admin's login>
FAIR_TENANT_ADMIN_PASSWORD=<its password>
CLUSTER_PROTOCOLS_POOL=<the pool's name>
CLUSTER_PROTOCOLS_VIPS=<the pool's addresses>
FAIR_BUCKET_PREFIX=<your initials>-      # bucket names are unique across the cluster, not per tenant
```

and leave `CLUSTER_BROKER_VIPS` unset. Tested in a developer tenant on VAST 5.6, from the tenant through
the triggers: a file written over S3 or NFSv4 reached the Event Broker's topic through the tenant's
one Protocols pool.

**On a shared cluster**, give each deployment its own `FAIR_BUCKET_PREFIX` whichever way you deploy: two
deployments with the same bucket names collide even in different tenants. The prefix applies to every
bucket, the Kafka view's too, and the function, portal and upload scripts follow it.

## 5. Deploy

```bash
uv run deploy/de.py up        # all steps, in order; prints what it found or created
uv run deploy/de.py status    # what exists
```

| Step | What it creates (all in the tenant) |
|---|---|
| tenant, vippools, admin | the tenant, its VIP pools (a separate Event Broker pool only with `CLUSTER_BROKER_VIPS`), a role and a tenant admin; in a developer tenant, a check that the platform team's exist |
| s3_identity, s3_policy | an S3_NATIVE view policy, the bucket-owner user and key (kept in `deploy/.state.json`, git-ignored, mode 600), an identity policy (`s3:*` on the tenant's buckets, `s3:QueryEngine*` and `s3:Tabular*` for the Query Engine) |
| views | `fair-kafka` (Event Broker), `fair-raw` and `fair-estate` (S3 + NFSv3 + NFSv4, delegations off), `fair-db` (VAST DB), `fair-docs` |
| topics, dataengine | Kafka topics, then DataEngine enabled on the tenant with the Internal broker |
| k8s_link | a namespace on your cluster, its mTLS credentials and the cluster itself, linked as external compute |
| wheels, image | the DataEngine SDK and runtime wheels, then the function image, built for linux/amd64, smoke-tested and pushed |
| container_registry, secret | the registry linked to DataEngine; Kubernetes secrets for S3, the model key and VMS |
| trigger | five element triggers: S3 and NFS4 on `fair-raw`; `_READY` (S3 and NFS4) and `review.json` on `fair-estate` |
| function, pipeline | the function and the pipeline, deployed and waited on until Ready |
| portal_image, portal | the portal image, and the portal on your cluster at `<node>:30080` |

Run a single step with `uv run deploy/de.py up <step> <step>`, or resume from one with
`uv run deploy/de.py up <last> <first>`. After a code change, `up portal image` rebuilds and redeploys
the function, pipeline and portal.

## 6. Use it

Open `http://<node>:30080`. The **Help** menu explains the architecture, the review screen, the FAIR score
and the demo data.

```bash
P=http://<node>:30080
python3 scripts/upload.py --portal $P            # the 10 staged folders, chunked uploads, then _READY each
python3 scripts/seed_estate.py --portal $P       # 7 "existing" folders into fair-estate, no _READY
```

**HPC jobs over NFS.** Mount the landing view over **NFSv4** (element triggers see NFSv4 writes, not NFSv3).
Write as the bucket owner's uid (8300 by default), since the view uses the S3-native flavor:

```bash
sudo mount -t nfs -o vers=4.1 <S3 VIP>:/fair-raw /mnt/fair/raw
mkdir -p /mnt/fair/raw/<domain>/<run> && cp results/* /mnt/fair/raw/<domain>/<run>/ && touch /mnt/fair/raw/<domain>/<run>/_READY
```

Then, in the portal:

1. **Review** each dataset: accept, edit or reject what the model proposed. Licence and creators must come
   from the data, the researchers' documents or you.
2. **Publish**: a VAST snapshot, RO-Crate, Croissant, landing page and test identifier, and the dataset
   becomes searchable.
3. **Search** by meaning (Enter in the search box), or **Find a variable** (CF names, units).
4. **Backlog**: VAST Catalog's view of folders with data but no record. *Curate* runs the same pipeline in
   place.

## 7. Measure it

```bash
.venv/bin/python scripts/scorecard.py --portal $P     # model drafts against data/ground_truth.json
docker run -d --name fair-fuji --add-host <portal host>:host-gateway -p 1071:1071 ghcr.io/pangaea-data-publisher/fuji:3.5.0
.venv/bin/python scripts/fuji.py --portal $P          # independent F-UJI score for every published dataset
```

## 8. Remove it

```bash
uv run deploy/de.py down                     # deletes everything up created, in reverse order
vastde uninstall --kubeconfig ~/.kube/fair-k8s.yaml    # if you installed DataEngine's services only for this
```

Snapshots taken at publication are kept. Delete them in VMS if you no longer need them.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| New function code never runs | A pipeline update only takes effect when deployed. `de.py` deploys after every change; check the pod's image with `kubectl -n <ns> get pods -o jsonpath='{..image}'`. |
| Trigger create: "internal error" | One suffix per `name_filters`. `de.py` uses a separate trigger for each. |
| NFS4 trigger: `nfs_triggers_delegations_conflict` | Seen on 5.6 straight after the views are made; it was accepted when re-run. `de.py` carries on without it and says so: re-run `up trigger trigger`. |
| NFS writes don't start curation | Use NFSv4 (`vers=4.1`). The view needs S3 and NFS4 with delegations off, and a trigger with source `NFS4`. |
| `Permission denied` on the NFS mount | Root is squashed; write as the bucket owner's uid (e.g. `setpriv --reuid=8300 --regid=8300 --clear-groups`). |
| VIP pool: "domain name … DNS is not configured" | A Query Engine pool needs a domain name, which needs the cluster DNS service (`/api/dns/`) first. |
| `vastde … unknown flag: --kubeconfig` | The flag belongs to `install`: `vastde install --kubeconfig <file>`. |
| Catalog query: "path - field required" | Pass `path=/` to `/api/bigcatalogconfig/query_data/`. |
| Backlog doesn't show new folders | VAST Catalog is a periodic snapshot; wait for its next run (`GET /api/bigcatalogconfig/` shows the schedule). |
| Large uploads fail | Use the portal's chunked upload (`scripts/upload.py`, or the drop box), which sends 5 MB parts, each retried. |
| Compute cluster DEGRADED | On VAST-managed compute, check the CNodes' Kubernetes memory alarms; over-commit by other workloads shows up this way. That's one reason this guide uses your own Kubernetes. |

## Known issues

- **Identity policies are attached per tenant.** If you script VMS yourself, attach a tenant's policy with
  `PATCH /api/users/<id>/tenant_data/` and `{"tenant_id": …, "s3_policies_ids": […]}`. `PATCH /api/users/<id>`
  attaches it in the default tenant, where it doesn't apply: Tabular still works for a bucket owner, but every
  Query Engine query (vector search) is refused with `denied according to policies res=2`. `de.py` does this
  for you. Details in `docs/known-issues.md`.
- **Version matching.** The public vastde releases (5.5.0-sp2 when this was written) can trail a cluster's
  service pack. It works against 5.5.0 SP6; VAST support can advise on a matching release.
- **Identifiers** are test ARKs (`ark:/99999/fk4…`), not resolvable outside the portal. Production would
  mint DataCite DOIs.
