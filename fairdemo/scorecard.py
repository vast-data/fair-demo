"""Grade the pipeline's own drafts (before any person edits them) against what is actually true.

Ground truth (data/ground_truth.json) comes from the data providers' pages. Each field has one plain rule:

  exact   the value equals this
  any     the value contains at least one of these terms (case-insensitive)
  min2    the value contains at least two of these terms (keywords)
  absent  nothing in the folder states this, so the value must be empty; anything else was invented

A field is "correct", "wrong" (a value that fails the rule, including an invented one) or "missing" (empty
where a value was expected: the pipeline said "a person must supply this", which is honest, not wrong).
"""
from __future__ import annotations

import json


def _text(v) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        return " | ".join(x.get("name", json.dumps(x)) if isinstance(x, dict) else str(x) for x in v)
    if isinstance(v, dict):
        return json.dumps(v)
    return str(v)


def grade_field(rule: dict, value) -> str:
    empty = value in (None, "", [])
    if rule.get("absent"):
        return "correct" if empty else "wrong"
    if empty:
        return "missing"
    t = _text(value).lower()
    if "exact" in rule:
        return "correct" if t == rule["exact"].lower() else "wrong"
    terms = [x.lower() for x in rule.get("any") or rule.get("min2") or []]
    hits = sum(1 for x in terms if x in t)
    need = 2 if "min2" in rule else 1
    return "correct" if hits >= need else "wrong"


def grade(record: dict, truth: dict) -> list[dict]:
    out = []
    for name, rule in truth.items():
        f = record["fields"].get(name) or {}
        value = f.get("value") if f.get("status") != "missing" else None
        out.append({"field": name, "rule": rule, "value": _text(value)[:160], "source": f.get("source") or "none",
                    "confidence": f.get("confidence"), "result": grade_field(rule, value)})
    return out


def summarise(graded: dict[str, list[dict]]) -> dict:
    rows = [r for rs in graded.values() for r in rs]

    def tally(rs):
        n = len(rs)
        c = sum(r["result"] == "correct" for r in rs)
        return {"fields": n, "correct": c, "wrong": sum(r["result"] == "wrong" for r in rs),
                "missing": sum(r["result"] == "missing" for r in rs), "accuracy": round(c / n, 3) if n else None}

    by_source = {s: tally([r for r in rows if r["source"] == s]) for s in ("header", "companion", "inferred", "none")}
    inferred = [r for r in rows if r["source"] == "inferred"]
    calibration = {
        "confidence >= 0.85": tally([r for r in inferred if (r["confidence"] or 0) >= 0.85]),
        "confidence < 0.85": tally([r for r in inferred if (r["confidence"] or 0) < 0.85]),
    }
    absent = [r for r in rows if r["rule"].get("absent")]
    return {"overall": tally(rows), "by_source": by_source, "model_calibration": calibration,
            "invented_facts": {"checks": len(absent), "invented": sum(r["result"] == "wrong" for r in absent)},
            "wrong": [{"dataset": d, **{k: r[k] for k in ("field", "value", "source", "confidence")}}
                      for d, rs in graded.items() for r in rs if r["result"] == "wrong"]}
