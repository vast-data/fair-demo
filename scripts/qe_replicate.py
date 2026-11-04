#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11,<3.13"   # wheels for every dependency (3.13+ builds zstandard from source)
# dependencies = ["requests", "vastdb[adbc]==2.1.0", "pyarrow"]
# ///
"""Reproduce the Query Engine refusal (docs/known-issues.md) on a fresh VAST cluster, in two tenants.

Sets up the same thing twice, once in the default tenant and once in a tenant of its own (as `fair` is on
a demo tenant): a protocols VIP pool, an S3_NATIVE view policy, a user with an S3 key and an identity policy
allowing s3:ListBucket, s3:Tabular* and s3:QueryEngine* (plain and ARN resources, and "*"), and a
Database-enabled view owned by that user. Both share one all-tenant Query Engine VIP pool, with the cluster
DNS service its domain name needs. Then, in each tenant, as that user:

  1. a Tabular select (no Query Engine)
  2. Query Engine `SELECT 1` (no table)
  3. a vector search on a vector-indexed table

Runs on Linux (the ADBC driver has no macOS build), on a host that reaches VMS and the data VIPs, e.g. the
cluster's own Linux host:

    VMS=https://<vms ip> SUBNET=15.0.0 uv run scripts/qe_replicate.py           # set up and test
    VMS=https://<vms ip> SUBNET=15.0.0 uv run scripts/qe_replicate.py --test    # test only
    VMS=https://<vms ip> SUBNET=15.0.0 uv run scripts/qe_replicate.py --remove  # delete what it created

On a shared cluster it reuses what is there: the DNS service, any Query Engine pool, and any pool that already
holds VIP_A or VIP_B. It creates only the tenant qe-b, pools for VIPs no pool holds, and its own qe-* users,
policies and views, and --remove deletes exactly those.

Settings (environment): VMS, VMS_ADMIN (user:password, required), SUBNET (first three octets of
the data network), CIDR (24), DB_PORT (the data VIPs' port for S3/DB, default 80), VIA (one address
every tenant's user connects through, instead of its tenant's protocols VIP), VIP_A and VIP_B (the default
tenant's and the own tenant's protocols VIPs, default SUBNET.20 and SUBNET.21).
"""
import json
import os
import sys
import time
import traceback
from pathlib import Path

import pyarrow as pa
import requests
import urllib3

urllib3.disable_warnings()

VMS = os.environ["VMS"].rstrip("/")
ADMIN = tuple(os.environ["VMS_ADMIN"].split(":", 1))
NET = os.environ.get("SUBNET", "15.0.0")
CIDR = int(os.environ.get("CIDR", "24"))
DB_PORT = os.environ.get("DB_PORT", "80")
STATE = Path(os.environ.get("QE_STATE", "~/.qe_replicate.json")).expanduser()

DNS = {"name": "qe-dns", "vip": f"{NET}.53", "domain_suffix": "qe.vast.test"}
QE_POOL = {"name": "qe-all-tenants", "first": f"{NET}.40", "last": f"{NET}.41"}
CASES = [  # tenant (None: default), its protocols VIP, user, bucket
    {"case": "default-tenant", "tenant": None, "vip": os.environ.get("VIP_A", f"{NET}.20"), "user": "qe-a-owner", "uid": 8401, "bucket": "qe-a-db"},
    {"case": "own-tenant", "tenant": "qe-b", "vip": os.environ.get("VIP_B", f"{NET}.21"), "user": "qe-b-owner", "uid": 8402, "bucket": "qe-b-db"},
]


def vms(method, path, **kw):
    r = requests.request(method, f"{VMS}/api/{path.lstrip('/')}", auth=ADMIN, verify=False, timeout=60, **kw)
    if not r.ok:
        raise SystemExit(f"VMS {method} {path} -> HTTP {r.status_code}: {r.text[:600]}")
    return r.json() if r.text else None


def vms_list(path, **params):
    res = vms("GET", path, params=params)
    return res["results"] if isinstance(res, dict) and "results" in res else res


def one(endpoint, **match):
    return next((r for r in vms_list(endpoint) if all(str(r.get(k)) == str(v) for k, v in match.items())), None)


def log(msg):
    print(f"[qe] {msg}", flush=True)


def _ip(a):
    return tuple(int(x) for x in a.split("."))


def pool_holding(vip):
    return next((p for p in vms_list("vippools/") for lo, hi in p.get("ip_ranges") or [] if _ip(lo) <= _ip(vip) <= _ip(hi)), None)


def setup():
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    if not vms_list("dns/"):
        vms("POST", "dns/", json={**DNS, "vip_subnet_cidr": CIDR, "enabled": True})
    for _ in range(40):
        if (vms_list("dns/") or [{}])[0].get("sync") == "SYNCED":
            break
        time.sleep(3)
    if not any(p["role"] == "QUERY_ENGINE_CNODE_GROUP" for p in vms_list("vippools/")):
        vms("POST", "vippools/", json={"name": QE_POOL["name"], "role": "QUERY_ENGINE_CNODE_GROUP", "subnet_cidr": CIDR,
                                       "ip_ranges": [[QE_POOL["first"], QE_POOL["last"]]], "port_membership": "ALL",
                                       "domain_name": "qe"})
    qe = [(p["name"], p.get("tenant_name") or "all tenants") for p in vms_list("vippools/") if p["role"] == "QUERY_ENGINE_CNODE_GROUP"]
    log(f"cluster DNS {vms_list('dns/')[0].get('domain_suffix')}; Query Engine pools {qe}")
    for c in CASES:
        t = one("tenants/", name=c["tenant"] or "default")
        if not t:
            t = vms("POST", "tenants/", json={"name": c["tenant"]})
        tid = t["id"]
        pool = f"{c['case']}-protocols"
        if not pool_holding(c["vip"]):
            vms("POST", "vippools/", json={"name": pool, "role": "PROTOCOLS", "subnet_cidr": CIDR, "ip_ranges": [[c["vip"], c["vip"]]],
                                           "port_membership": "ALL", "tenant_id": tid})
        vp = one("viewpolicies/", name=f"{c['case']}-s3")
        if not vp:
            vp = vms("POST", "viewpolicies/", json={"name": f"{c['case']}-s3", "flavor": "S3_NATIVE", "tenant_id": tid,
                                                    "auth_source": "PROVIDERS"})
        u = one("users/", name=c["user"])
        if not u:
            u = vms("POST", "users/", json={"name": c["user"], "uid": c["uid"], "local_provider_id": t["local_provider_id"]})
        if c["case"] not in state:
            k = vms("POST", f"users/{u['id']}/access_keys/", json={"tenant_id": tid})
            state[c["case"]] = {"access": k["access_key"], "secret": k["secret_key"]}
            STATE.write_text(json.dumps(state))
            STATE.chmod(0o600)
        b = c["bucket"]
        doc = {"Version": "2012-10-17", "Statement": [{
            "Sid": "DatabaseAndQueryEngine", "Effect": "Allow", "Action": ["s3:ListBucket", "s3:Tabular*", "s3:QueryEngine*"],
            "Resource": [b, f"{b}/*", f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/*", "*", "arn:aws:s3:::*"]}]}
        pol = one("s3policies/", name=f"{c['case']}-identity")
        if not pol:
            pol = vms("POST", "s3policies/", json={"name": f"{c['case']}-identity", "policy": json.dumps(doc), "tenant_id": tid, "enabled": True})
        # Per tenant: PATCH users/<id> would attach it in the default tenant, where it never applies to this one
        # (the cause of the refusal this script was written to reproduce). QE_ATTACH=user reproduces that mistake.
        if os.environ.get("QE_ATTACH") == "user":
            vms("PATCH", f"users/{u['id']}/", json={"s3_policies_ids": [pol["id"]]})
        else:
            vms("PATCH", f"users/{u['id']}/tenant_data/", json={"tenant_id": tid, "s3_policies_ids": [pol["id"]]})
        if not one("views/", path=f"/{b}", tenant_id=tid):
            vms("POST", "views/", json={"path": f"/{b}", "bucket": b, "bucket_owner": c["user"], "policy_id": vp["id"],
                                         "tenant_id": tid, "protocols": ["S3", "DATABASE"], "create_dir": True})
        log(f"{c['case']}: tenant {t['name']} (id {tid}), VIP {c['vip']}, user {c['user']} with identity policy, view /{b}")
    log("waiting 30 s for VIPs and policies to settle")
    time.sleep(30)


def test():
    import vastdb
    from vastdb._internal import VectorIndexSpec
    state = json.loads(STATE.read_text())
    vec_t = pa.list_(pa.field("", pa.float32(), False), 8)
    results = {}
    for c in CASES:
        keys, out = state[c["case"]], {}
        # VIA: connect every tenant's user through one address instead (e.g. a protocols pool for all tenants).
        endpoint = f"http://{os.environ.get('VIA') or c['vip']}" + ("" if DB_PORT == "80" else f":{DB_PORT}")

        def step(name, fn):
            try:
                out[name] = f"ok: {fn()}"
            except Exception as e:  # noqa: BLE001
                msg = str(e).replace("\n", " ")
                i = msg.find("Server responded")
                out[name] = "FAIL: " + (msg[i:i + 260] if i >= 0 else msg[:260])
                if os.environ.get("QE_TRACE"):
                    traceback.print_exc()
        s = vastdb.connect(endpoint=endpoint, access=keys["access"], secret=keys["secret"])
        with s.transaction() as tx:
            bucket = tx.bucket(c["bucket"])
            sch = bucket.schema("qe", fail_if_missing=False) or bucket.create_schema("qe")
            t = sch.table("probe", fail_if_missing=False)
            if t is None:
                cols = pa.schema([("id", pa.int64()), ("embedding", vec_t)])
                try:
                    t = sch.create_table("probe", cols, vector_index=VectorIndexSpec("embedding", "l2sq"))
                except Exception as e:  # noqa: BLE001  (releases before vector indexes)
                    log(f"{c['case']}: no vector index ({str(e)[:100]}); plain table")
                    t = sch.create_table("probe", cols)
                t.insert(pa.table({"id": [1, 2], "embedding": pa.array([[0.1] * 8, [0.9] * 8], vec_t)}))
        with s.transaction() as tx:
            t = tx.bucket(c["bucket"]).schema("qe").table("probe")
            step("1 tabular select", lambda: t.select(columns=["id"]).read_all().num_rows)
            step("2 QE SELECT 1", lambda: (tx._adbc_conn.cursor.execute("SELECT 1 AS one"),
                                         tx._adbc_conn.cursor.fetch_arrow_table().to_pylist())[1])
        with s.transaction() as tx:
            t = tx.bucket(c["bucket"]).schema("qe").table("probe")
            step("3 vector search", lambda: t.vector_search(vec=[0.1] * 8, columns=["id"], limit=1).read_all().to_pylist())
        results[c["case"]] = out
    cluster = vms_list("clusters/")[0]
    print(f"\nVAST {cluster.get('sw_version')} ({cluster.get('name')})")
    for case, out in results.items():
        print(f"\n{case}:")
        for name, res in out.items():
            print(f"  {name:<18} {res}")


def remove():
    """Delete what setup() created: tables, views, identity and view policies, users, its pools, tenant qe-b."""
    import vastdb
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    for c in CASES:
        keys = state.get(c["case"])
        if keys:
            try:
                s = vastdb.connect(endpoint=f"http://{c['vip']}" + ("" if DB_PORT == "80" else f":{DB_PORT}"),
                                   access=keys["access"], secret=keys["secret"])
                with s.transaction() as tx:
                    sch = tx.bucket(c["bucket"]).schema("qe", fail_if_missing=False)
                    if sch:
                        for t in sch.tables():
                            t.drop()
                        sch.drop()
            except Exception as e:  # noqa: BLE001
                log(f"{c['case']}: could not drop the probe schema: {str(e)[:160]}")
        t = one("tenants/", name=c["tenant"] or "default")
        for v in vms_list("views/"):
            if v["path"] == f"/{c['bucket']}" and t and v.get("tenant_id") == t["id"]:
                vms("DELETE", f"views/{v['id']}/")
        for name, path in ((f"{c['case']}-identity", "s3policies/"), (f"{c['case']}-s3", "viewpolicies/"), (f"{c['case']}-protocols", "vippools/")):
            o = one(path, name=name)
            if o:
                vms("DELETE", f"{path}{o['id']}/")
        u = one("users/", name=c["user"])
        if u:
            vms("DELETE", f"users/{u['id']}/")
        if c["tenant"] and t:   # force: VMS won't otherwise remove the tenant's empty folders
            vms("DELETE", f"tenants/{t['id']}/", params={"force": "true"})
        log(f"{c['case']}: removed")
    STATE.unlink(missing_ok=True)


if __name__ == "__main__":
    if "--remove" in sys.argv:
        remove()
        sys.exit(0)
    if "--test" not in sys.argv:
        setup()
    test()
