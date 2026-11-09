"""Seed a local-mode portal with published datasets, through its own pipeline: upload, curate, review, publish.

Used by "Site B (demo)", the second site of the catalog-of-catalogs demo (scripts/site_b_data.py makes its
datasets). Set FAIR_SITE_SEED to a directory of <domain>/<dataset>/ folders; each holds data files, an optional
README.md, and seed-review.json with the decisions the site's curator makes on review:

    {"reviewer": "...", "edits": {"title": ..., "description": ..., "creators": [...], "license": ..., ...}}

Every field the pipeline proposed is accepted and every edit is applied as the person's own value, exactly as
the review screen would. A dataset already published is left alone, so a restart doesn't publish it twice.
"""
from __future__ import annotations

import json
import time
from pathlib import Path


def _wait(fn, timeout=180, every=0.5):
    t0 = time.time()
    while time.time() - t0 < timeout:
        got = fn()
        if got:
            return got
        time.sleep(every)
    return None


def seed(store, root: Path, log=print) -> list[str]:
    from portal.store import RAW
    published = []
    for review_file in sorted(Path(root).glob("*/*/seed-review.json")):
        src = review_file.parent
        folder = "%s/%s" % (src.parent.name, src.name)
        record = lambda: store.get_bytes(f"{folder}/_fair/record.json")
        current = json.loads(record()) if record() else None
        if current and current.get("status") == "published":
            continue
        if not current:
            for f in sorted(p for p in src.iterdir() if p.is_file() and p.name != "seed-review.json"):
                store.s3.put_object(Bucket=RAW, Key=f"{folder}/{f.name}", Body=f.read_bytes())
            store.s3.put_object(Bucket=RAW, Key=f"{folder}/_READY", Body=b"")
            if not _wait(record):
                log(f"seed: {folder} was never curated")
                continue
            current = json.loads(record())
        spec = json.loads(review_file.read_text())
        by = spec.get("reviewer", "curator")
        decisions = {k: {"action": "accept"} for k, f in current["fields"].items() if f["status"] == "proposed"}
        decisions |= {k: {"action": "edit", "value": v, "by": by} for k, v in spec.get("edits", {}).items()}
        store.put(f"{folder}/_fair/review.json", json.dumps(
            {"reviewer": by, "decisions": decisions, "variables": {}, "tables": True}, indent=2).encode())
        if _wait(lambda: (json.loads(record()) or {}).get("status") == "published"):
            published.append(folder)
            log(f"seed: published {folder}")
        else:
            log(f"seed: {folder} was reviewed but not published")
    return published
