#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "boto3", "pyyaml"]
# ///
"""Build (and tear down) the FAIR curation pipeline on a VAST cluster, with DataEngine's compute on a
Kubernetes cluster you run, linked to the tenant as external compute (see docs/SETUP.md).

    ./deploy/de.py up [LAST [FIRST]]   create everything, in order, checking each object
    ./deploy/de.py status              show what exists
    ./deploy/de.py down                delete everything up created

  object PUT into s3://fair-raw/<domain>/<dataset>/...  -> Element trigger (ObjectCreated)
    -> Kafka topic on the VAST Event Broker (view fair-kafka) -> DataEngine function fair-curate
    -> fair-db/fair/{files,records,search,variables}, RO-Crate + Croissant back into the dataset folder,
       landing text into s3://fair-docs

Every site-specific value comes from .env (see .env.example). Every step is idempotent. Keys and passwords
created on the way are kept in the state file (git-ignored, mode 600).
"""

import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings()

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def _dotenv():
    out = {}
    for line in (ROOT / ".env").read_text().splitlines() if (ROOT / ".env").exists() else []:
        k, sep, v = line.partition("=")
        if sep and not k.startswith("#"):
            out[k.strip()] = v.split("#", 1)[0].strip() if " #" in v else v.strip()
    return out


ENV = _dotenv()


def need(key):
    """A required setting: the environment, then .env."""
    v = os.environ.get(key) or ENV.get(key)
    if not v:
        raise SystemExit(f"set {key} in .env (see .env.example)")
    return v


def C(key, default=None):
    """A CLUSTER_* setting: the environment, then .env, then the default; required if there is none."""
    v = os.environ.get("CLUSTER_" + key) or ENV.get("CLUSTER_" + key) or default
    if v is None:
        raise SystemExit(f"set CLUSTER_{key} in .env (see .env.example)")
    return v


def _range(v):
    first, _, last = v.partition("-")
    return first.strip(), (last or first).strip()


VMS_HOST = C("VMS").removeprefix("https://").rstrip("/")
VMS = f"https://{VMS_HOST}"
STATE = HERE / C("STATE_FILE", ".state.json")
ADMIN = (ENV.get("VMS_ADMIN_USER", "admin"), need("VMS_ADMIN_PASSWORD"))

TENANT = ENV.get("FAIR_TENANT", "fair")
TENANT_ADMIN = TENANT + "-admin"
ROLE = "fair-role"
S3_POLICY = "fair-s3"
S3_IDENTITY_POLICY = "fair-owner-s3"
BUCKET_OWNER, BUCKET_OWNER_UID = "fair-owner", 8300
PROTOCOLS_POOL, BROKER_POOL = "fair-protocols", "fair-broker"   # S3 + DB; Kafka
KAFKA_VIEW = "fair-kafka"
RAW_BUCKET, DB_BUCKET, DOCS_BUCKET = "fair-raw", "fair-db", "fair-docs"
ESTATE_BUCKET = "fair-estate"   # data already on the system: curated in place by the backlog sweep
DB_SCHEMA = "fair"
DEFAULT_TOPIC, DLT_TOPIC, EVENTS_TOPIC = "default", "default-dlt", "fair-events"
# The DataEngine SDK and runtime wheels come from VAST's published builder image, matching your DataEngine stack.
VAST_BUILDER = C("VAST_BUILDER", "vastdataorg/vast-builder:v5.5.0-sp2")
# VIPs on the cluster's data network that no pool uses (check every pool and ping them first). The Kafka
# pool is dedicated to the Event Broker and needs at least one IP per CNode.
POOL_RANGES = {PROTOCOLS_POOL: _range(C("PROTOCOLS_VIPS")), BROKER_POOL: _range(C("BROKER_VIPS"))}
PROTOCOLS_VIP = POOL_RANGES[PROTOCOLS_POOL][0]
POOL_CIDR = int(C("VIP_CIDR", "24"))
S3_ENDPOINT = C("S3_ENDPOINT", f"http://{PROTOCOLS_VIP}")
REGISTRY_PUSH = C("REGISTRY")                      # host:port, HTTPS with a valid certificate
REGISTRY_URL = f"https://{REGISTRY_PUSH}"
IMAGE_REPO = C("REGISTRY_PREFIX", TENANT) + "/fair-curate"
CR_NAME = "fair-registry"
KUBECONFIG = os.path.expanduser(C("KUBECONFIG"))
EXTERNAL_K8S = {"name": C("K8S_NAME", TENANT + "-k8s"), "api": C("K8S_API"),
                "namespace": C("K8S_NAMESPACE", TENANT + "-dataengine")}
S3_SECRET, LLM_SECRET, VMS_SECRET = "vast-s3-creds", "fair-llm", "fair-vms"
TRIGGER, FUNCTION, PIPELINE = "fair-raw-trigger", "fair-curate", "fair-curate-pipeline"
ESTATE_TRIGGER, ESTATE_REVIEW_TRIGGER = "fair-estate-trigger", "fair-estate-review-trigger"
RAW_NFS_TRIGGER, ESTATE_NFS_TRIGGER = "fair-raw-nfs-trigger", "fair-estate-nfs-trigger"   # NFSv4 writes

LITELLM_BASE_URL = need("LITELLM_BASE_URL")     # any OpenAI-compatible gateway
CHAT_MODEL = need("FAIR_CHAT_MODEL")
EMBED_MODEL = need("FAIR_EMBED_MODEL")          # 1024 dimensions: fairdemo/catalog.py DIM
HTTPS_PROXY = C("HTTPS_PROXY", "")              # empty: the cluster reaches the model gateway directly
LANDING_BASE_URL = C("LANDING_BASE_URL")
PORTAL_REPO, PORTAL_NODE_PORT = C("REGISTRY_PREFIX", TENANT) + "/fair-portal", int(ENV.get("FAIR_PORTAL_NODE_PORT", "30080"))
# "Site B (demo)", the second site of the catalog-of-catalogs demo: its own namespace and NodePort, landing pages
# on the same host as this portal's unless CLUSTER_SITE_B_LANDING_BASE_URL says otherwise.
SITE_B_REPO, SITE_B_NODE_PORT = C("REGISTRY_PREFIX", TENANT) + "/fair-portal-site-b", int(ENV.get("FAIR_SITE_B_NODE_PORT", "30081"))
_landing = __import__("urllib.parse").parse.urlsplit(LANDING_BASE_URL)
SITE_B_LANDING_BASE_URL = C("SITE_B_LANDING_BASE_URL", f"{_landing.scheme}://{_landing.hostname}:{SITE_B_NODE_PORT}{_landing.path}")
SITE_B_OAI = "http://portal-b.fair-portal-b.svc.cluster.local:8080/oai"


# ---------------------------------------------------------------- helpers

def log(msg):
    print(f"[fair] {msg}", flush=True)


def load_state():
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def save_state(**kv):
    st = load_state() | kv
    STATE.write_text(json.dumps(st, indent=2))
    STATE.chmod(0o600)


def tenant_admin_pw():
    """FAIR_TENANT_ADMIN_PASSWORD if set; otherwise one generated on first use and kept in the state file."""
    pw = ENV.get("FAIR_TENANT_ADMIN_PASSWORD") or load_state().get("tenant_admin_password")
    if not pw:
        pw = secrets.token_urlsafe(18) + "-Aa1"   # VMS asks for mixed case, a digit and a symbol
        save_state(tenant_admin_password=pw)
    return pw


def vms(method, path, **kw):
    r = requests.request(method, f"{VMS}/api/{path.lstrip('/')}", auth=ADMIN, verify=False, timeout=60, **kw)
    if not r.ok:
        raise SystemExit(f"VMS {method} {path} -> HTTP {r.status_code}: {r.text[:800]}")
    return r.json() if r.text else None


def vms_list(endpoint, **params):
    res = vms("GET", endpoint, params=params)
    return res["results"] if isinstance(res, dict) and "results" in res else res


def vms_one(endpoint, **params):
    rows = [r for r in vms_list(endpoint, **params) if all(str(r.get(k)) == str(v) for k, v in params.items())]
    return rows[0] if rows else None


class DE:
    """The DataEngine (provisioning) API, as the tenant admin."""

    def __init__(self):
        r = requests.post(f"{VMS}/api/token/{TENANT}", data={"username": TENANT_ADMIN, "password": tenant_admin_pw()},
                          verify=False, timeout=30)
        if not r.ok:
            raise SystemExit(f"tenant token -> HTTP {r.status_code}: {r.text[:300]}")
        self.h = {"Authorization": f"Bearer {r.json()['access']}"}

    def req(self, method, path, ok404=False, **kw):
        r = requests.request(method, f"{VMS}/api/latest/dataengine{path}", headers=self.h, verify=False,
                             timeout=120, **kw)
        if ok404 and r.status_code == 404:
            return None
        if not r.ok:
            raise SystemExit(f"DE {method} {path} -> HTTP {r.status_code}: {r.text[:1500]}")
        return r.json() if r.text else None

    def list(self, path):
        res = self.req("GET", path)
        return res.get("data", res) if isinstance(res, dict) else res

    def find(self, path, name):
        return next((o for o in self.list(path) if o.get("name") == name), None)


def wait(what, fn, timeout=600, every=5):
    t0 = time.time()
    while True:
        v = fn()
        if v:
            return v
        if time.time() - t0 > timeout:
            raise SystemExit(f"timed out waiting for {what}")
        time.sleep(every)


def run(*cmd, **kw):
    log("$ " + " ".join(cmd))
    return subprocess.run(cmd, check=True, **kw)


def tenant_id():
    t = vms_one("tenants/", name=TENANT)
    if not t:
        raise SystemExit(f"tenant {TENANT} missing: run ./deploy/de.py up")
    return t["id"]


# ---------------------------------------------------------------- steps (VMS)

def step_tenant():
    t = vms_one("tenants/", name=TENANT)
    if not t:
        t = vms("POST", "tenants/", json={"name": TENANT})
    log(f"tenant {TENANT}: id {t['id']}")


def step_vippools():
    tid = tenant_id()
    for name, (first, last) in POOL_RANGES.items():
        p = vms_one("vippools/", name=name)
        if not p:
            p = vms("POST", "vippools/", json={"name": name, "role": "PROTOCOLS", "subnet_cidr": POOL_CIDR,
                                               "ip_ranges": [[first, last]], "port_membership": "ALL", "tenant_id": tid})
        log(f"vip pool {name}: id {p['id']} {p.get('ip_ranges')}")


def step_admin():
    tid = tenant_id()
    role = vms_one("roles/", name=ROLE)
    if not role:
        perms = [f"{op}_{realm}" for op in ("create", "view", "edit", "delete")
                 for realm in ("applications", "database", "events", "hardware", "logical", "monitoring",
                               "security", "settings", "support")]
        role = vms("POST", "roles/", json={"name": ROLE, "permissions_list": perms, "tenant_id": tid})
    if not vms_one("managers/", username=TENANT_ADMIN):
        vms("POST", "managers/", json={"username": TENANT_ADMIN, "password": tenant_admin_pw(),
                                       "password_expiration_disabled": True, "user_type": "TENANT_ADMIN",
                                       "tenant_id": tid, "is_temporary_password": False, "roles": [role["id"]]})
    DE()
    log(f"role {ROLE} and tenant admin {TENANT_ADMIN}: ok (tenant token issued)")


def step_s3_identity():
    tid = tenant_id()
    t = vms_one("tenants/", name=TENANT)
    if not vms_one("viewpolicies/", name=S3_POLICY):
        vms("POST", "viewpolicies/", json={"name": S3_POLICY, "flavor": "S3_NATIVE", "tenant_id": tid,
                                           "auth_source": "PROVIDERS"})
    u = vms_one("users/", name=BUCKET_OWNER)
    if not u:
        u = vms("POST", "users/", json={"name": BUCKET_OWNER, "uid": BUCKET_OWNER_UID,
                                        "local_provider_id": t["local_provider_id"]})
    st = load_state()
    have = {k["access_key"] for k in (vms("GET", f"users/{u['id']}/").get("access_keys") or [])}
    # VMS doesn't list the keys of a user in a non-default tenant, so an empty list doesn't mean the key
    # in the state file is gone: keep it. Make one only when there is none, or VMS lists others but not it.
    if not st.get("access_key") or (have and st["access_key"] not in have):
        r = requests.post(f"{VMS}/api/users/{u['id']}/access_keys/", auth=ADMIN, verify=False, timeout=60,
                          json={"tenant_id": tid})
        if r.status_code == 400 and "key_limit_reached" in r.text:
            raise SystemExit(f"{BUCKET_OWNER} already has the maximum number of S3 keys and none is in "
                             f"{STATE.name}: remove an unused key in VMS (Users > {BUCKET_OWNER}), then re-run")
        r.raise_for_status()
        k = r.json()
        save_state(access_key=k["access_key"], secret_key=k["secret_key"])
    log(f"view policy {S3_POLICY}, user {BUCKET_OWNER} (id {u['id']}) with S3 key {load_state()['access_key']}")


def step_s3_policy():
    """An identity policy for the bucket owner. Owning the buckets covers S3 and VAST DB reads and writes,
    but the Query Engine (vector search) also needs s3:QueryEngine* and s3:Tabular* granted explicitly,
    on ARN resources (KB: "VAST Query Engine", "Identity and Bucket Policy Reference")."""
    tid = tenant_id()
    u = vms_one("users/", name=BUCKET_OWNER)
    arns = [r for b in (RAW_BUCKET, DB_BUCKET, DOCS_BUCKET, KAFKA_VIEW, ESTATE_BUCKET)
            for r in (f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/*")]
    doc = {"Version": "2012-10-17", "Statement": [
        {"Sid": "Buckets", "Effect": "Allow", "Action": "s3:*", "Resource": arns},
        # Query status and other engine calls name no bucket, so this statement can't be bucket-scoped.
        {"Sid": "QueryEngine", "Effect": "Allow", "Action": ["s3:QueryEngine*", "s3:Tabular*"],
         "Resource": ["arn:aws:s3:::*"]}]}
    pol = vms_one("s3policies/", name=S3_IDENTITY_POLICY)
    if not pol:
        pol = vms("POST", "s3policies/", json={"name": S3_IDENTITY_POLICY, "policy": json.dumps(doc),
                                               "tenant_id": tid, "enabled": True})
    elif json.loads(pol["policy"]) != doc:
        pol = vms("PATCH", f"s3policies/{pol['id']}/", json={"policy": json.dumps(doc)})
    # A user's identity policies are kept per tenant (users/<id>/tenant_data/?tenant_id=...). PATCH users/<id>
    # writes the default tenant's, where a policy of this tenant never applies: ownership still covers Tabular,
    # but the Query Engine, which is policy-gated, refuses everything ("denied according to policies res=2").
    have = vms("GET", f"users/{u['id']}/tenant_data/", params={"tenant_id": tid}).get("s3_policies_ids") or []
    if pol["id"] not in have:
        vms("PATCH", f"users/{u['id']}/tenant_data/", json={"tenant_id": tid, "s3_policies_ids": have + [pol["id"]]})
    log(f"s3 identity policy {S3_IDENTITY_POLICY} (id {pol['id']}) on {BUCKET_OWNER}: s3:* and Query Engine on {len(arns)} ARNs")


def step_views():
    tid = tenant_id()
    policy = vms_one("viewpolicies/", name=S3_POLICY)
    broker_pool = vms_one("vippools/", name=BROKER_POOL)
    specs = [
        (f"/{KAFKA_VIEW}", KAFKA_VIEW, ["KAFKA", "S3", "DATABASE"], {"kafka_vip_pools": [broker_pool["id"]]}),
        # Landing bucket: S3 for uploads, NFS for HPC jobs writing results straight in. Element triggers see
        # NFSv4 writes (KB: Provisioning Source Views for Element Type Triggers) when the view has S3 and NFS4
        # and NFSv4 delegations are off.
        (f"/{RAW_BUCKET}", RAW_BUCKET, ["S3", "NFS", "NFS4"], {"allowed_delegations": "NONE"}),
        (f"/{DB_BUCKET}", DB_BUCKET, ["DATABASE", "S3"], {}),
        (f"/{DOCS_BUCKET}", DOCS_BUCKET, ["S3"], {}),
        # The estate, as HPC jobs see it: S3 and NFS on the same files.
        (f"/{ESTATE_BUCKET}", ESTATE_BUCKET, ["S3", "NFS", "NFS4"], {"allowed_delegations": "NONE"}),
    ]
    for path, bucket, protocols, extra in specs:
        v = vms_one("views/", path=path, tenant_id=tid)
        if not v:
            body = {"path": path, "bucket": bucket, "bucket_owner": BUCKET_OWNER, "policy_id": policy["id"],
                    "tenant_id": tid, "protocols": protocols, "create_dir": True, **extra}
            r = requests.post(f"{VMS}/api/views/", auth=ADMIN, verify=False, timeout=60, json=body)
            if not r.ok and "NFS" in protocols:
                log(f"view {path} with NFS refused ({r.text[:200]}); creating it S3-only")
                r = requests.post(f"{VMS}/api/views/", auth=ADMIN, verify=False, timeout=60,
                                  json=body | {"protocols": [x for x in protocols if x != "NFS"]})
            if not r.ok:
                raise SystemExit(f"VMS POST views/ {path} -> HTTP {r.status_code}: {r.text[:800]}")
            v = r.json()
        else:
            # An existing view gains the protocols and settings it lacks (e.g. NFS added later); never removes any.
            change = {k: val for k, val in extra.items() if k != "kafka_vip_pools" and v.get(k) != val}
            if set(protocols) - set(v["protocols"]):
                change["protocols"] = sorted(set(v["protocols"]) | set(protocols))
            if change:
                r = requests.patch(f"{VMS}/api/views/{v['id']}/", auth=ADMIN, verify=False, timeout=60, json=change)
                if r.ok:
                    v = r.json()
                else:
                    log(f"view {path}: could not apply {change}: {r.text[:200]}")
        log(f"view {path}: id {v['id']} bucket {v['bucket']} {v['protocols']}")


def step_topics():
    tid = tenant_id()
    for name in (DEFAULT_TOPIC, DLT_TOPIC, EVENTS_TOPIC):
        r = requests.post(f"{VMS}/api/topics/", auth=ADMIN, verify=False, timeout=60,
                          json={"database_name": KAFKA_VIEW, "schema_name": "kafka_topics", "name": name,
                                "topic_partitions": 8, "retention_ms": 604800000, "tenant_id": tid})
        if not r.ok and "ALREADY_EXISTS" not in r.text.upper():
            raise SystemExit(f"VMS POST topics/ {name} -> HTTP {r.status_code}: {r.text[:500]}")
    got = vms("GET", "topics/", params={"database_name": KAFKA_VIEW, "tenant_id": tid})
    log(f"kafka topics in {KAFKA_VIEW}: {json.dumps(got)[:300]}")


# ---------------------------------------------------------------- steps (DataEngine)

def step_dataengine():
    de = DE()
    st = (de.req("GET", "/data-engine", ok404=True) or {}).get("setup", {})
    if st.get("status") in (None, "not_started", "error"):
        de.req("POST", "/data-engine", json={"kafka_broker": {"type": "Internal", "name": KAFKA_VIEW},
                                             "default_topic_name": DEFAULT_TOPIC,
                                             "dead_letter_topic_name": DLT_TOPIC})

    def done():
        s = de.req("GET", "/data-engine")["setup"]
        if s.get("status") == "error":
            raise SystemExit(f"data engine setup failed: {s}")
        return s if s.get("status") == "completed" else None
    s = wait("data engine setup", done, timeout=900)
    log(f"data engine on tenant {TENANT}: {s.get('status')} {s.get('message', '')}")


def step_k8s_link():
    """Link our own Kubernetes cluster to the tenant's DataEngine as external compute: a namespace for the
    tenant, the cluster's mTLS credentials, and the cluster itself (KB: Enabling DataEngine on a Tenant,
    Preparing External Compute Clusters; the DataEngine stack is already installed with vastde)."""
    import base64
    import yaml
    ns = EXTERNAL_K8S["namespace"]
    if subprocess.run(["kubectl", "--kubeconfig", KUBECONFIG, "get", "ns", ns], capture_output=True).returncode:
        _kubectl("create", "namespace", ns)
    kc = yaml.safe_load(Path(KUBECONFIG).read_text())
    cluster, user = kc["clusters"][0]["cluster"], kc["users"][0]["user"]
    de = DE()
    name = EXTERNAL_K8S["name"]
    cred = de.find("/mtls-authentication-credentials", name + "-mtls")
    if not cred:
        cred = de.req("POST", "/mtls-authentication-credentials", json={
            "name": name + "-mtls", "certificate_authority_b64": cluster["certificate-authority-data"],
            "client_certificate_b64": user["client-certificate-data"], "client_key_b64": user["client-key-data"]})
    k = de.find("/kubernetes-clusters", name)
    if not k:
        k = de.req("POST", "/kubernetes-clusters", json={"name": name, "kube_api_url": EXTERNAL_K8S["api"],
                                                          "mtls_credentials_guid": cred["guid"], "namespaces": [ns]})
    log(f"external kubernetes cluster {name} ({EXTERNAL_K8S['api']}) linked: {k.get('vrn')} ns {k.get('namespaces')} "
        f"status {k.get('status')}")


def kube_cluster(de):
    ks = de.list("/kubernetes-clusters")
    if not ks:
        raise SystemExit("no DataEngine kubernetes cluster for the tenant (link the compute cluster first)")
    return ks[0]


def _kubectl(*args, **kw):
    return run("kubectl", "--kubeconfig", KUBECONFIG, *args, **kw)


def step_wheels():
    """The DataEngine SDK and runtime wheels, from VAST's published builder image (VAST_BUILDER), whose
    version should match the DataEngine stack on your compute cluster."""
    build = HERE / "build"
    wheels, img = build / "wheels", build / "vast-builder"
    subprocess.run(["rm", "-rf", str(wheels), str(img)], check=True)
    wheels.mkdir(parents=True)
    run("skopeo", "copy", "-q", "--override-os", "linux", "--override-arch", "amd64",
        f"docker://docker.io/{VAST_BUILDER}", f"dir:{img}")
    assets = "/cnb/buildpacks/vast-buildpack/1.0.0/assets/"
    for layer in img.iterdir():
        names = subprocess.run(["tar", "-tzf", str(layer)], capture_output=True, text=True).stdout.split()
        whls = [n for n in names if n.startswith(assets) and n.endswith(".whl")]
        if whls:
            subprocess.run(["tar", "-xzf", str(layer), "-C", str(wheels), "--strip-components", "6", *whls],
                           check=True, capture_output=True)
            break
    got = sorted(w.name for w in wheels.glob("*.whl"))
    if len(got) != 2:
        raise SystemExit(f"expected the sdk and runtime wheels in {VAST_BUILDER}, found {got}")
    log(f"wheels from {VAST_BUILDER}: {got}")


def image_version():
    """A tag that changes whenever the function or the fairdemo package changes."""
    import hashlib
    h = hashlib.sha256()
    for f in sorted([*(ROOT / "fairdemo").glob("*.py"), ROOT / "function/main.py", ROOT / "function/Dockerfile",
                     ROOT / "pyproject.toml"]):
        h.update(f.read_bytes())
    return "v-" + h.hexdigest()[:10]


def step_image():
    build = HERE / "build"
    ctxdir = build / "image-context"
    shutil.rmtree(ctxdir, ignore_errors=True)
    (ctxdir / "wheels").mkdir(parents=True)
    for f in ("main.py", "Dockerfile"):
        shutil.copy(ROOT / "function" / f, ctxdir / f)
    shutil.copytree(ROOT / "fairdemo", ctxdir / "fairdemo-src/fairdemo", ignore=shutil.ignore_patterns("__pycache__"))
    for f in ("pyproject.toml", "README.md"):   # pyproject.toml names README.md as the package readme
        shutil.copy(ROOT / f, ctxdir / "fairdemo-src" / f)
    for w in (build / "wheels").glob("*.whl"):
        shutil.copy(w, ctxdir / "wheels" / w.name)
    tag = image_version()
    oci = build / "fair-curate.oci.tar"
    run("docker", "buildx", "build", "--platform", "linux/amd64", "--provenance=false",
        "--output", f"type=oci,dest={oci}", str(ctxdir))
    run("docker", "buildx", "build", "--quiet", "--platform", "linux/amd64", "--load", "-t", "fair-smoke",
        str(ctxdir), stdout=subprocess.DEVNULL)
    run("docker", "run", "--rm", "--platform", "linux/amd64", "--entrypoint", "python", "fair-smoke", "-c",
        "import sys; sys.path.insert(0, '/workspace'); import vast.dataengine.runtime.main, "
        "serverless_common_models.generated, main, netCDF4, h5py, astropy, uproot, tifffile; print('smoke ok')")
    subprocess.run(["docker", "rmi", "-f", "fair-smoke"], capture_output=True)
    run("skopeo", "copy", "--dest-tls-verify=true", f"oci-archive:{oci}",
        f"docker://{REGISTRY_PUSH}/{IMAGE_REPO}:{tag}")
    r = requests.head(f"https://{REGISTRY_PUSH}/v2/{IMAGE_REPO}/manifests/{tag}", timeout=10, headers={
        "Accept": "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"})
    r.raise_for_status()
    save_state(image_tag=tag, image_digest=r.headers["Docker-Content-Digest"])
    log(f"image pushed: {REGISTRY_PUSH}/{IMAGE_REPO}:{tag}@{load_state()['image_digest']}")


def image_tag():
    st = load_state()
    return f"{st['image_tag']}@{st['image_digest']}"


def step_container_registry():
    de = DE()
    k = kube_cluster(de)
    cr = de.find("/container-registries", CR_NAME)
    if not cr:
        cr = de.req("POST", "/container-registries", json={
            "name": CR_NAME, "url": REGISTRY_URL, "auth_type": "none", "is_accessible_to_data_engine": True,
            "primary_kubernetes_cluster": {"kubernetes_cluster_vrn": k["vrn"], "namespace": k["namespaces"][0]}})
    log(f"container registry {CR_NAME}: {cr['vrn']} -> {cr['url']}")


def step_secret():
    de = DE()
    k = kube_cluster(de)
    st = load_state()
    key = ENV.get("FAIR_LLM_KEY") or ENV.get("LITELLM_API_KEY")
    if not key:
        raise SystemExit("no model key: set FAIR_LLM_KEY in .env (ideally a service account's key)")
    for name, entries in ((S3_SECRET, [{"key": "vast-s3-access-key", "value": st["access_key"]},
                                       {"key": "vast-s3-secret-key", "value": st["secret_key"]}]),
                          (LLM_SECRET, [{"key": "api-key", "value": key}]),
                          # the tenant admin, for the snapshot taken at publication
                          (VMS_SECRET, [{"key": "vms-user", "value": TENANT_ADMIN},
                                        {"key": "vms-password", "value": tenant_admin_pw()}])):
        de.req("POST", "/kubernetes-secrets", json={"kubernetes_cluster_vrn": k["vrn"], "namespace": k["namespaces"][0],
                                                    "secret_name": name, "entries": entries})
        log(f"kubernetes secret {name} in {k['namespaces'][0]}: ok")


def step_trigger():
    """fair-raw: every object (each file is profiled as it lands). fair-estate: only _READY and review
    files, so data already on the system is left alone until the backlog sweep (or a person) asks.
    (One suffix per trigger: VMS rejects a trigger with two, "internal error".)"""
    de = DE()
    for name, bucket, filters, source in ((TRIGGER, RAW_BUCKET, {}, "S3"),
                                          (ESTATE_TRIGGER, ESTATE_BUCKET, {"name_filters": {"suffixes": ["_READY"]}}, "S3"),
                                          (ESTATE_REVIEW_TRIGGER, ESTATE_BUCKET, {"name_filters": {"suffixes": ["review.json"]}}, "S3"),
                                          # The same, for files HPC jobs write over NFSv4
                                          (RAW_NFS_TRIGGER, RAW_BUCKET, {}, "NFS4"),
                                          (ESTATE_NFS_TRIGGER, ESTATE_BUCKET, {"name_filters": {"suffixes": ["_READY"]}}, "NFS4")):
        tr = next((t for t in de.list("/triggers/all") if t.get("name") == name), None)
        if not tr:
            tr = de.req("POST", "/triggers/element", json={
                "name": name, "broker": {"type": "Internal", "name": KAFKA_VIEW}, "topic_name": EVENTS_TOPIC,
                "source_types": [source],
                "config": {"events": ["ObjectCreated:*"], "source_bucket_name": bucket, **filters}})
        log(f"trigger {name}: {tr['vrn']} topic {tr.get('topic') or tr.get('topic_name')} ({tr.get('status')})")


def step_function():
    de = DE()
    cr = de.find("/container-registries", CR_NAME)
    fn = de.find("/functions", FUNCTION)
    body = {"name": FUNCTION, "description": "FAIR curation: profile files, curate folders, publish RO-Crate",
            "architecture": "x86", "runtime": "python-3.11", "artifact_type": "image",
            "artifact_source": IMAGE_REPO, "image_tag": image_tag(), "container_registry_vrn": cr["vrn"],
            "skip_image_validation": True}
    if not fn:
        fn = de.req("POST", "/functions", json=body)
    elif latest_revision(de, fn)["image_tag"] != image_tag():
        de.req("PUT", f"/functions/{fn['guid']}", json=body)
        fn = de.find("/functions", FUNCTION)
    rev = latest_revision(de, fn)
    log(f"function {FUNCTION}: {fn['vrn']} revision {rev['revision_number']} image {rev['image_tag']}")


def latest_revision(de, fn):
    revs = de.list(f"/functions/{fn['guid']}/revisions")
    return max(revs, key=lambda r: r["revision_number"])


def step_pipeline():
    de = DE()
    k = kube_cluster(de)
    triggers = {t["name"]: t for t in de.list("/triggers/all")
                if t.get("name") in (TRIGGER, ESTATE_TRIGGER, ESTATE_REVIEW_TRIGGER, RAW_NFS_TRIGGER, ESTATE_NFS_TRIGGER)}
    tr = triggers[TRIGGER]
    fn = de.find("/functions", FUNCTION)
    rev = latest_revision(de, fn)["revision_number"]
    p = de.find("/pipelines", PIPELINE)
    env = [("vast-s3-endpoint-url", S3_ENDPOINT), ("DB_BUCKET", DB_BUCKET), ("DB_SCHEMA", DB_SCHEMA),
           ("DOCS_BUCKET", DOCS_BUCKET), ("LANDING_BASE_URL", LANDING_BASE_URL),
           ("LITELLM_BASE_URL", LITELLM_BASE_URL), ("FAIR_CHAT_MODEL", CHAT_MODEL), ("FAIR_EMBED_MODEL", EMBED_MODEL),
           ("VMS_URL", VMS), ("VMS_TENANT", TENANT), ("NO_PROXY", f"{VMS_HOST},{PROTOCOLS_VIP},localhost")]
    if HTTPS_PROXY:
        env += [("HTTPS_PROXY", HTTPS_PROXY), ("https_proxy", HTTPS_PROXY)]
    manifest = {
        "triggers": [{"name": "fair-raw", "vrn": tr["vrn"]}, {"name": "fair-estate", "vrn": triggers[ESTATE_TRIGGER]["vrn"]},
                     {"name": "fair-estate-review", "vrn": triggers[ESTATE_REVIEW_TRIGGER]["vrn"]}]
                    + [{"name": n, "vrn": triggers[t]["vrn"]} for n, t in (("fair-raw-nfs", RAW_NFS_TRIGGER),
                                                                           ("fair-estate-nfs", ESTATE_NFS_TRIGGER))
                       if t in triggers],
        "function_deployments": [{
            "name": "fair-curate", "function_vrn": fn["vrn"], "revision": rev,
            "config": {"environment_variables": [{"name": n, "value": v} for n, v in env],
                       "secrets": [S3_SECRET, LLM_SECRET, VMS_SECRET]},
            # DataEngine's defaults (256Mi, 500m) are too small to load a published dataset as tables:
            # converting the CMS ROOT file alone peaks above 256 MB. Each file converts in a child process
            # (fairdemo/tables.py), so running short of memory loses that file's table, not the function.
            "resources": {"min_cpu": "250m", "max_cpu": C("FUNCTION_MAX_CPU", "2"),
                          "min_memory": "512Mi", "max_memory": C("FUNCTION_MAX_MEMORY", "2Gi"),
                          "timeout": int(C("FUNCTION_TIMEOUT", "600"))}}],   # Knative allows at most 600
        "links": [{"source": ["fair-raw", "fair-estate", "fair-estate-review"]
                   + [n for n, t in (("fair-raw-nfs", RAW_NFS_TRIGGER), ("fair-estate-nfs", ESTATE_NFS_TRIGGER)) if t in triggers],
                   "destination": ["fair-curate"], "topic": tr["topic"]}],
    }
    body = {"name": PIPELINE, "description": "s3://fair-raw -> profile, curate, publish (RO-Crate, Croissant)",
            "kubernetes_cluster_vrn": k["vrn"], "namespace": k["namespaces"][0], "manifest": manifest}
    # GET doesn't return the manifest, so what is deployed is tracked in .state.json: a PUT makes a new
    # pipeline revision but doesn't roll it out, so every change is followed by an explicit deploy.
    want = json.dumps(manifest, sort_keys=True)
    changed = load_state().get("pipeline_manifest") != want
    if not p:
        p = de.req("POST", "/pipelines", json=body)
    elif changed:
        de.req("PUT", f"/pipelines/{p['guid']}", json=body)
    p = de.req("GET", f"/pipelines/{p['guid']}")
    if changed or p.get("status") != "Ready":
        de.req("POST", f"/pipeline/{p['guid']}/deploy")
        time.sleep(10)
    t0 = time.time()

    def ready():
        s = de.req("GET", f"/pipelines/{p['guid']}")
        if s.get("status") in ("Failed", "Error") and time.time() - t0 > 120:
            raise SystemExit(f"pipeline deploy failed: {json.dumps(s)[:1500]}")
        return s if s.get("status") == "Ready" else None
    s = wait("pipeline Ready", ready, timeout=900, every=10)
    save_state(pipeline_manifest=want)
    log(f"pipeline {PIPELINE}: {s['status']} ({p['guid']}, revision {s.get('last_revision_number')}, function revision {rev})")


def _push_image(dockerfile: Path, repo: str, tag: str, oci_name: str) -> str:
    oci = HERE / "build" / oci_name
    oci.parent.mkdir(parents=True, exist_ok=True)
    run("docker", "buildx", "build", "--platform", "linux/amd64", "--provenance=false", "-f", str(dockerfile),
        "--output", f"type=oci,dest={oci}", str(ROOT))
    run("skopeo", "copy", "--dest-tls-verify=true", f"oci-archive:{oci}", f"docker://{REGISTRY_PUSH}/{repo}:{tag}")
    r = requests.head(f"https://{REGISTRY_PUSH}/v2/{repo}/manifests/{tag}", timeout=10, headers={
        "Accept": "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"})
    r.raise_for_status()
    return f"{REGISTRY_PUSH}/{repo}:{tag}@{r.headers['Docker-Content-Digest']}"


def step_site_b():
    """"Site B (demo)": a second, self-contained portal (local mode, its own namespace) whose datasets this
    portal harvests over OAI-PMH, for the catalog-of-catalogs demo. Its datasets come from
    scripts/site_b_data.py, derived from data/raw. Run step_portal afterwards so this portal lists it as a peer."""
    # In the project's environment (numpy, netCDF4, astropy), not this script's own minimal one.
    run("uv", "run", "--project", str(ROOT), "python", str(ROOT / "scripts/site_b_data.py"), str(HERE / "build/site-b"))
    files = sorted([*(ROOT / "fairdemo").glob("*.py"), *(ROOT / "portal").rglob("*.py"), ROOT / "function/main.py",
                    ROOT / "portal/static/index.html", ROOT / "portal/Dockerfile.site", ROOT / "pyproject.toml",
                    *(p for p in (HERE / "build/site-b").rglob("*") if p.is_file())])
    tag = "v-" + __import__("hashlib").sha256(b"".join(f.read_bytes() for f in files)).hexdigest()[:10]
    image = _push_image(ROOT / "portal/Dockerfile.site", SITE_B_REPO, tag, "fair-portal-site-b.oci.tar")
    manifest = (HERE / "site-b.yaml").read_text()
    for k, v in {"IMAGE": image, "LANDING_BASE_URL": SITE_B_LANDING_BASE_URL, "NODE_PORT": str(SITE_B_NODE_PORT)}.items():
        manifest = manifest.replace("${%s}" % k, v)
    _kubectl("apply", "-f", "-", input=manifest.encode())
    _kubectl("-n", "fair-portal-b", "rollout", "status", "deploy/portal-b", "--timeout=300s")
    save_state(site_b={"image": image, "oai": SITE_B_OAI, "landing": SITE_B_LANDING_BASE_URL})
    log(f"Site B (demo) on {EXTERNAL_K8S['api'].split('//')[1].split(':')[0]}:{SITE_B_NODE_PORT}; landing pages under {SITE_B_LANDING_BASE_URL}")


def step_portal_image():
    tag = "v-" + __import__("hashlib").sha256(b"".join(
        f.read_bytes() for f in sorted([*(ROOT / "fairdemo").glob("*.py"), *(ROOT / "portal").rglob("*.py"),
                                        ROOT / "portal/static/index.html", ROOT / "portal/Dockerfile",
                                        ROOT / "pyproject.toml"]))).hexdigest()[:10]
    save_state(portal_image=_push_image(ROOT / "portal/Dockerfile", PORTAL_REPO, tag, "fair-portal.oci.tar"))
    log(f"portal image pushed: {load_state()['portal_image']}")


def step_portal():
    """The portal as a Deployment on your Kubernetes cluster (outside DataEngine's namespaces), at
    <node>:FAIR_PORTAL_NODE_PORT (default 30080)."""
    st = load_state()
    key = ENV.get("FAIR_LLM_KEY") or ENV.get("LITELLM_API_KEY")
    secret = subprocess.run(["kubectl", "--kubeconfig", KUBECONFIG, "create", "secret",
                             "generic", "fair-portal-secrets", "-n", "fair-portal", "--dry-run=client", "-o", "yaml",
                             f"--from-literal=s3-access-key={st['access_key']}", f"--from-literal=s3-secret-key={st['secret_key']}",
                             f"--from-literal=llm-api-key={key}", f"--from-literal=vms-password={tenant_admin_pw()}"],
                            capture_output=True, text=True, check=True).stdout
    manifest = (HERE / "portal.yaml").read_text()
    for k, v in {"IMAGE": st["portal_image"], "S3_ENDPOINT": S3_ENDPOINT, "PROTOCOLS_VIP": PROTOCOLS_VIP,
                 "VIP_NAME": "ipv4-" + PROTOCOLS_VIP.replace(".", "-"), "LITELLM_BASE_URL": LITELLM_BASE_URL,
                 "CHAT_MODEL": CHAT_MODEL, "EMBED_MODEL": EMBED_MODEL, "HTTPS_PROXY": HTTPS_PROXY,
                 "VMS_URL": VMS, "VMS_HOST": VMS_HOST, "VMS_TENANT": TENANT, "VMS_USER": TENANT_ADMIN,
                 # partner repositories to harvest: Site B (demo), once step_site_b has deployed it
                 "PEERS": json.dumps([{"name": "Site B (demo)", "oai": st["site_b"]["oai"]}]) if st.get("site_b") else ""}.items():
        manifest = manifest.replace("${%s}" % k, v)
    ns, rest = manifest.split("\n---\n", 1)
    for doc in (ns, secret, rest):  # the namespace first, then the secret it holds, then the workload
        _kubectl("apply", "-f", "-", input=doc.encode())
    _kubectl("-n", "fair-portal", "rollout", "status", "deploy/portal", "--timeout=300s")
    log(f"portal on {EXTERNAL_K8S['api'].split('//')[1].split(':')[0]}:{PORTAL_NODE_PORT}; "
        f"landing pages under {LANDING_BASE_URL}")


STEPS = [step_tenant, step_vippools, step_admin, step_s3_identity, step_s3_policy, step_views, step_topics,
         step_dataengine, step_k8s_link, step_wheels, step_image, step_container_registry,
         step_secret, step_trigger, step_function, step_pipeline, step_site_b, step_portal_image, step_portal]


# ---------------------------------------------------------------- status / down

def cmd_status():
    log(f"VMS {VMS_HOST}")
    tid = (vms_one("tenants/", name=TENANT) or {}).get("id")
    print("tenant", TENANT, tid)
    if not tid:
        return
    print("vip pools", [(p["name"], p["ip_ranges"]) for p in vms_list("vippools/") if p.get("tenant_id") == tid])
    print("views", [(v["path"], v["protocols"]) for v in vms_list("views/", tenant_id=tid)])
    try:
        de = DE()
    except SystemExit as e:
        print("DE:", e)
        return
    print("data engine", de.req("GET", "/data-engine", ok404=True))
    for path in ("/kubernetes-clusters", "/container-registries", "/triggers/all", "/functions", "/pipelines"):
        print(path, [(o.get("name"), o.get("status") or o.get("vrn")) for o in de.list(path)])


def cmd_down():
    """Delete everything up created, in reverse order. Each delete is best-effort, so a half-built demo
    also tears down. Snapshots taken at publication are kept."""
    def tryit(what, fn):
        try:
            fn()
            log(f"deleted {what}")
        except (SystemExit, Exception) as e:  # noqa: BLE001
            log(f"skip {what}: {e}")
    tid = (vms_one("tenants/", name=TENANT) or {}).get("id")
    if tid:
        try:
            de = DE()
        except SystemExit:
            de = None
        if de:
            for path, name in (("/pipelines", PIPELINE), ("/functions", FUNCTION)):
                o = de.find(path, name)
                if o:
                    tryit(f"{path} {name}", lambda o=o, path=path: de.req("DELETE", f"{path}/{o['guid']}"))
            for t in de.list("/triggers/all"):
                if t.get("name") in (TRIGGER, ESTATE_TRIGGER, ESTATE_REVIEW_TRIGGER, RAW_NFS_TRIGGER, ESTATE_NFS_TRIGGER):
                    tryit(f"trigger {t['name']}", lambda t=t: de.req("DELETE", f"/triggers/{t['guid']}"))
            cr = de.find("/container-registries", CR_NAME)
            if cr:
                tryit(f"registry {CR_NAME}", lambda: de.req("DELETE", f"/container-registries/{cr['guid']}"))
            k = de.find("/kubernetes-clusters", EXTERNAL_K8S["name"])
            if k:
                tryit(f"kubernetes cluster {k['name']}", lambda: de.req("DELETE", f"/kubernetes-clusters/{k['guid']}"))
            c = de.find("/mtls-authentication-credentials", EXTERNAL_K8S["name"] + "-mtls")
            if c:
                tryit(f"mtls credentials {c['name']}",
                      lambda: de.req("DELETE", f"/mtls-authentication-credentials/{c['guid']}"))
        if de:
            tryit("data engine", lambda: de.req("DELETE", "/remove-data-engine", json={"force": True}))
        for path in (f"/{RAW_BUCKET}", f"/{ESTATE_BUCKET}", f"/{DB_BUCKET}", f"/{DOCS_BUCKET}", f"/{KAFKA_VIEW}"):
            v = vms_one("views/", path=path, tenant_id=tid)
            if v:
                tryit(f"view {path}", lambda v=v: vms("DELETE", f"views/{v['id']}/"))
    for path, key, name in (("s3policies/", "name", S3_IDENTITY_POLICY), ("users/", "name", BUCKET_OWNER),
                            ("managers/", "username", TENANT_ADMIN), ("roles/", "name", ROLE),
                            ("viewpolicies/", "name", S3_POLICY), ("vippools/", "name", PROTOCOLS_POOL),
                            ("vippools/", "name", BROKER_POOL)):
        o = vms_one(path, **{key: name})
        if o:
            tryit(f"{path}{name}", lambda o=o, path=path: vms("DELETE", f"{path}{o['id']}/"))
    if tid:
        tryit(f"tenant {TENANT}", lambda: vms("DELETE", f"tenants/{tid}/"))
    for ns in ("fair-portal", "fair-portal-b", EXTERNAL_K8S["namespace"]):
        tryit(f"namespace {ns}", lambda ns=ns: _kubectl("delete", "namespace", ns, "--ignore-not-found"))
    STATE.unlink(missing_ok=True)


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "up":
        names = [s.__name__.removeprefix("step_") for s in STEPS]
        only = sys.argv[2] if len(sys.argv) > 2 else None
        start = sys.argv[3] if len(sys.argv) > 3 else None
        if only and only not in names:
            raise SystemExit(f"unknown step {only}; steps: {' '.join(names)}")
        go = start is None
        for s, n in zip(STEPS, names):
            go = go or n == start
            if go:
                log(f"--- {n}")
                s()
            if n == only:
                break
    elif cmd in ("status", "down"):
        globals()[f"cmd_{cmd}"]()
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
