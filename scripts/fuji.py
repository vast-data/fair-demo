#!/usr/bin/env python3
"""Independent FAIR assessment: run F-UJI (https://github.com/pangaea-data-publisher/fuji) against every
published landing page and compare it with our own score.

    docker run -d --name fair-fuji --add-host fair.localhost:host-gateway -p 1071:1071 \\
        ghcr.io/pangaea-data-publisher/fuji:3.5.0          # 4.0.1's image lacks its headless browser
    .venv/bin/python scripts/fuji.py --portal http://<portal>   # writes out/fuji.json and out/FUJI.md
"""
import base64
import json
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FUJI = "http://localhost:1071/fuji/api/v1/evaluate"
PORTAL = "http://localhost:8080"
AUTH = "Basic " + base64.b64encode(b"marvel:wonderwoman").decode()  # F-UJI's documented default


def get(url):
    return json.load(urllib.request.urlopen(url, timeout=60))


def evaluate(landing):
    body = json.dumps({"object_identifier": landing, "test_debug": True, "use_datacite": False,
                       "use_github": False, "metadata_service_type": "oai_pmh"}).encode()
    # F-UJI answers 400 to a request without an Accept header (urllib sends none)
    req = urllib.request.Request(FUJI, body, {"Content-Type": "application/json", "Accept": "*/*", "Authorization": AUTH})
    return json.load(urllib.request.urlopen(req, timeout=900))


def main():
    global PORTAL
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", default=PORTAL)
    PORTAL = ap.parse_args().portal.rstrip("/")
    rows = []
    for d in get(PORTAL + "/api/datasets")["datasets"]:
        if d["status"] != "published":
            continue
        rec = get(f"{PORTAL}/api/datasets/{d['folder']}/record")["record"]
        try:
            res = evaluate(rec["landing_page"])
        except urllib.error.HTTPError as e:
            print(f"{d['folder']:56} F-UJI refused {rec['landing_page']}: {e.code} {e.read()[:200]!r}")
            continue
        s = res["summary"]["score_percent"]
        failed = [r["metric_identifier"] for r in res["results"] if r["score"]["earned"] < r["score"]["total"]]
        rows.append({"dataset": d["folder"], "ours": rec.get("score_after"), "fuji": s["FAIR"],
                     "F": s["F"], "A": s["A"], "I": s["I"], "R": s["R"], "not_full": failed})
        print(f"{d['folder']:56} ours {rec.get('score_after')}  F-UJI {s['FAIR']:.1f}  (F {s['F']} A {s['A']} I {s['I']} R {s['R']})")
    (ROOT / "out").mkdir(exist_ok=True)
    (ROOT / "out/fuji.json").write_text(json.dumps(rows, indent=2) + "\n")
    md = ["# F-UJI assessment (F-UJI 3.5.0, run locally)", "", "| Dataset | Our score | F-UJI | F | A | I | R |",
          "|---|---|---|---|---|---|---|"]
    md += [f"| {r['dataset']} | {r['ours']} | {r['fuji']:.1f} | {r['F']} | {r['A']} | {r['I']} | {r['R']} |" for r in rows]
    (ROOT / "out/FUJI.md").write_text("\n".join(md) + "\n")


if __name__ == "__main__":
    main()
