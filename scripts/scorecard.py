#!/usr/bin/env python3
"""Accuracy scorecard: the pipeline's drafts against ground truth (data/ground_truth.json).

The drafts are read from VAST DB (fair.records): for each dataset, the latest version written by the
fair-curate function itself, i.e. before any person's review. --local grades out/*.json instead.

    .venv/bin/python scripts/scorecard.py --portal http://<portal>   # any target: drafts through the portal
    .venv/bin/python scripts/scorecard.py                            # direct: FAIR_S3_ENDPOINT and the state file's key
    .venv/bin/python scripts/scorecard.py --local                    # out/*.json from fairdemo curate
Writes out/scorecard.json and out/SCORECARD.md.
"""
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from fairdemo import scorecard  # noqa: E402


def drafts_from_cluster() -> dict:
    import vastdb
    from vastdb.config import QueryConfig
    st = json.loads((ROOT / "deploy/.state.json").read_text())
    endpoint = os.environ.get("FAIR_S3_ENDPOINT")
    if not endpoint:
        raise SystemExit("set FAIR_S3_ENDPOINT, or use --portal or --local")
    s = vastdb.connect(endpoint=endpoint, access=st["access_key"], secret=st["secret_key"])
    with s.transaction() as tx:
        t = tx.bucket("fair-db").schema("fair").table("records")
        rows = t.select(columns=["folder", "version", "actor", "record"],
                        config=QueryConfig(data_endpoints=[endpoint])).read_all().to_pylist()
    latest = {}
    for r in rows:
        if r["actor"] == "fair-curate" and (r["folder"] not in latest or r["version"] > latest[r["folder"]]["version"]):
            latest[r["folder"]] = r
    return {k: json.loads(v["record"]) for k, v in latest.items()}


def drafts_from_portal(portal) -> dict:
    import urllib.request
    with urllib.request.urlopen(portal.rstrip("/") + "/api/drafts", timeout=120) as r:
        return json.load(r)["drafts"]


def drafts_from_local() -> dict:
    out = {}
    for f in (ROOT / "out").glob("*.json"):
        if f.name == "scorecard.json":
            continue
        r = json.loads(f.read_text())
        out[f.stem.replace("_", "/", 1)] = r
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    ap.add_argument("--portal")
    args = ap.parse_args()
    truth = {k: v for k, v in json.loads((ROOT / "data/ground_truth.json").read_text()).items() if not k.startswith("_")}
    drafts = drafts_from_local() if args.local else drafts_from_portal(args.portal) if args.portal \
        else drafts_from_cluster()
    graded = {d: scorecard.grade(drafts[d], truth[d]) for d in truth if d in drafts}
    summary = scorecard.summarise(graded)
    summary["datasets"] = len(graded)
    summary["model"] = next(iter(drafts.values()))["curator"].get("model") if drafts else None
    (ROOT / "out").mkdir(exist_ok=True)
    (ROOT / "out/scorecard.json").write_text(json.dumps({"summary": summary, "graded": graded}, indent=2) + "\n")

    o, inv = summary["overall"], summary["invented_facts"]
    lines = [f"# Accuracy scorecard ({summary['datasets']} datasets, model {summary['model']})", "",
             f"**{o['correct']}/{o['fields']} fields correct** ({o['accuracy']:.0%}); {o['wrong']} wrong, "
             f"{o['missing']} left for a person.", "",
             f"**Invented facts: {inv['invented']} of {inv['checks']}** checks where nothing in the folder states "
             "the licence or creators.", "", "| Source | Fields | Correct | Wrong | Missing | Accuracy |",
             "|---|---|---|---|---|---|"]
    for name, t in list(summary["by_source"].items()) + [("model, confidence ≥ 0.85", summary["model_calibration"]["confidence >= 0.85"]),
                                                          ("model, confidence < 0.85", summary["model_calibration"]["confidence < 0.85"])]:
        if t["fields"]:
            lines.append(f"| {name} | {t['fields']} | {t['correct']} | {t['wrong']} | {t['missing']} | "
                         f"{t['accuracy']:.0%} |")
    lines += ["", "## Wrong values", "", "| Dataset | Field | Value | Source | Confidence |", "|---|---|---|---|---|"]
    for w in summary["wrong"]:
        lines.append(f"| {w['dataset']} | {w['field']} | {w['value'][:80]} | {w['source']} | {w['confidence']} |")
    (ROOT / "out/SCORECARD.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
