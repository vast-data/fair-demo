"""Questions in plain language, turned into the same structured query the portal's controls build.

The model never writes code or SQL. It reads the dataset's tables as curation left them (each column's
units, reviewed meaning, range and commonest values) and proposes columns, filters and an optional
aggregate as JSON. That proposal is checked exactly like a person's query (tables.parse_where,
check_aggregate): anything that doesn't fit the table is sent back once with the reason, and refused if
it still doesn't. The portal shows how the question was read, as filters a person can see and change,
before trusting the answer.
"""
from __future__ import annotations

import json
import re

from . import llm, tables

SYSTEM = """You turn a question about a published research dataset into a query over its tables.
Reply with one JSON object and nothing else:
{"table": "<a listed table>", "columns": ["<column>", ...], "where": ["<column><op><value>", ...],
 "aggregate": null or {"fn": "avg|min|max|sum|count", "column": "<column, or null with count>", "by": "<column or null>"},
 "order": null or {"column": "<column, or the aggregate's result name fn_column (fn alone for count)>", "desc": true|false},
 "limit": <integer 1-1000>, "explanation": "<one plain sentence: how you read the question>"}
If the tables can't answer it, reply {"unanswerable": "<why, in one sentence>"}.
Rules:
- Use only the listed tables and columns, spelled exactly.
- op is one of = != < <= > >=. Several filters are ANDed; there is no OR. A range is two filters.
- Values are in the column's own units as listed. If the question uses other units, convert, and say so.
- Text values: use one of the listed example values when one matches; comparisons are exact.
- Times as ISO 8601 (2024-09-01T00:00:00).
- "columns": what answers the question, plus what it filters on. With an aggregate, columns can be [].
- Questions about an average, total, number or extreme over the data use "aggregate"; "per X" or "by X" sets "by".
- "count" counts matching rows, not distinct values.
- Superlatives (warmest, largest, top, busiest, lowest) need "order" (desc for highest) and a "limit".
- "limit": how many rows to show (20 if the question doesn't say).
- Before replying, check that every number in "where" is the one your explanation gives."""


def describe_tables(record: dict) -> str:
    out = []
    for t in record.get("tables") or []:
        out.append("Table %s (%s rows, from %s):" % (t["table"], f"{t['rows']:,}", t["source"]))
        for c in t["columns"]:
            bits = [c["type"]]
            if c.get("units"):
                bits.append("units " + c["units"])
            if c.get("standard_name"):
                bits.append("CF " + c["standard_name"])
            if c.get("min") is not None:
                bits.append("range %s to %s" % (c["min"], c["max"]))
                if _is_longitude(c) and isinstance(c["max"], (int, float)) and c["min"] >= 0 and c["max"] > 180:
                    bits.append("degrees east, 0 to 360: a longitude of x degrees west is 360 - x")
            if c.get("examples"):
                bits.append("values such as " + ", ".join(repr(x) for x in c["examples"][:8]))
            desc = (" : " + c["description"][:160]) if c.get("description") else ""
            out.append("  - %s (%s)%s" % (c["name"], "; ".join(bits), desc))
    return "\n".join(out)


def _is_longitude(c: dict) -> bool:
    return c["name"] in ("lon", "longitude", "long") or (c.get("standard_name") or "") == "longitude" \
        or (c.get("units") or "").lower() in ("degrees_east", "degree_east")


def _empty_range(filters: list[tuple], columns: list[dict]) -> str | None:
    """Filters on one column that can't both hold (lon >= 330 and lon <= 320; or lon >= 330 and lon <= 330
    on a float): almost always a slip in reading the question, not what was asked."""
    floats = {c["name"] for c in columns if c["type"].startswith(("float", "double"))}
    for col in {f[0] for f in filters}:
        lo = max((v for c, o, v in filters if c == col and o in (">", ">=") and not isinstance(v, str)), default=None)
        hi = min((v for c, o, v in filters if c == col and o in ("<", "<=") and not isinstance(v, str)), default=None)
        if lo is not None and hi is not None and (lo > hi or (lo == hi and col in floats)):
            return "the filters on %s select nothing (%s to %s)" % (col, lo, hi)
    return None


def _normalise(q: dict) -> dict:
    """The model's common variations on the format: op/function for fn, group_by for by, direction for desc."""
    q = dict(q)
    a = q.get("aggregate")
    if isinstance(a, dict):
        a = dict(a)
        a["fn"] = a.get("fn") or a.get("op") or a.get("function") or a.get("type")
        a["fn"] = {"average": "avg", "mean": "avg", "total": "sum"}.get(str(a["fn"]).lower(), a["fn"])
        a["by"] = a.get("by") or a.get("group_by") or a.get("groupby")
        q["aggregate"] = a
    elif isinstance(a, str) and a:
        q["aggregate"] = {"fn": a, "column": None, "by": None}
    o = q.get("order")
    if isinstance(o, dict) and "desc" not in o:
        o = dict(o)
        o["desc"] = str(o.get("direction") or o.get("dir") or "").lower().startswith("desc")
        q["order"] = o
    return q


GROUPED = re.compile(r"\b(per|by|for each|each)\s+([a-z_ ]+)", re.I)


def _check(q: dict, record: dict) -> tuple[dict, dict]:
    q = _normalise(q)
    entry = next((t for t in record.get("tables") or [] if t["table"] == q.get("table")), None)
    if entry is None:
        raise ValueError("table %r is not one of the listed tables" % q.get("table"))
    names = {c["name"] for c in entry["columns"]}
    columns = [c for c in q.get("columns") or [] if c]
    bad = [c for c in columns if c not in names]
    if bad:
        raise ValueError("no column %s in %s" % (", ".join(bad), entry["table"]))
    where = [w for w in q.get("where") or [] if str(w).strip()]
    why = _empty_range(tables.parse_where(where, entry["columns"]), entry["columns"])
    if why:
        raise ValueError(why)
    agg = tables.check_aggregate(q.get("aggregate"), entry["columns"])
    order = tables.check_order(q.get("order"), entry, agg)
    if agg and not agg["by"] and order:
        raise ValueError("an aggregate over every row is one value: to rank, group it with \"by\" (e.g. per road)")
    try:
        limit = max(1, min(int(q.get("limit") or 20), 1000))
    except (TypeError, ValueError):
        limit = 20
    if not columns and not agg:
        columns = [c["name"] for c in entry["columns"][:6]]
    return entry, {"table": entry["table"], "columns": columns, "where": where, "aggregate": agg, "order": order, "limit": limit,
                   "explanation": str(q.get("explanation") or "")[:300]}


WEST = re.compile(r"(\d+(?:\.\d+)?)(?:\s*(?:°|deg(?:rees?)?)?\s*(?:to|-|–|and)\s*(\d+(?:\.\d+)?))?"
                  r"\s*(?:°|deg(?:rees?)?)?\s*(?:W\b|west)", re.I)


def arithmetic_notes(question: str, record: dict) -> str:
    """Unit conversions done here rather than left to the model: western longitudes, for a table that
    counts longitude 0 to 360 degrees east (as OISST does)."""
    east360 = any(_is_longitude(c) and isinstance(c.get("max"), (int, float)) and c.get("min", -1) >= 0 and c["max"] > 180
                  for t in record.get("tables") or [] for c in t["columns"])
    if not east360:
        return ""
    vals = []
    for m in WEST.finditer(question):
        vals += [float(x) for x in m.groups() if x is not None]
    vals = [v for v in dict.fromkeys(vals) if 0 <= v <= 180]
    if not vals:
        return ""
    return "\nIn this table's longitude (0 to 360 degrees east): " + ", ".join(
        "%g degrees west = %g" % (v, 360 - v) for v in vals) + "."


def interpret(question: str, record: dict, title: str) -> dict:
    """The question as a checked query: {table, columns, where, aggregate, limit, explanation}, or
    {"unanswerable": why}. Raises llm.LLMError if the model can't be reached, ValueError if its answer
    still doesn't fit the tables after one correction."""
    user = "Dataset: %s\n\n%s\n\nQuestion: %s%s" % (title, describe_tables(record), question.strip()[:500],
                                                     arithmetic_notes(question, record))
    q = llm.chat_json(SYSTEM, user, max_tokens=600)
    if q.get("unanswerable"):
        return {"unanswerable": str(q["unanswerable"])[:300]}
    try:
        return _check(q, record)[1]
    except ValueError as e:
        q = llm.chat_json(SYSTEM, user + "\n\nYour previous answer was %s\nIt was refused: %s. Correct it."
                          % (json.dumps(q)[:1500], e), max_tokens=600)
        if q.get("unanswerable"):
            return {"unanswerable": str(q["unanswerable"])[:300]}
        return _check(q, record)[1]


SUGGEST = """You suggest questions a researcher might ask of a published dataset's tables, each with its query.
Reply with one JSON object and nothing else: {"questions": [ <item>, ... ]}, where each item is
{"question": "<a short, natural question, under 80 characters>", "table": "<a listed table>", "columns": ["<column>", ...],
 "where": ["<column><op><value>", ...],
 "aggregate": null or {"fn": "avg|min|max|sum|count", "column": "<column, or null with count>", "by": "<column or null>"},
 "order": null or {"column": "<column, or the aggregate's result name fn_column (fn alone for count)>", "desc": true|false},
 "limit": <1-1000>, "explanation": "<one sentence>"}
using exactly these keys ("fn", "by", "desc"; no others) and the rules below. A question that says "per X",
"by X" or "for each X" must set "by" to that column. Make them varied: some with filters, some averages or counts,
at least one per-group summary ("by year", "per filter"), at least one top-N with an order, and, when there are
several tables, questions about more than one table. Use real column names and values within the listed ranges
and example values, so every question has an answer.
Query format and rules, as for any question:
""" + SYSTEM.split("Rules:", 1)[1]


def suggest(record: dict, title: str, n: int = 6) -> list[dict]:
    """Up to n example questions for a dataset's tables, each with a query that has passed the same checks
    as any other (so a suggestion never fails when it is run). One model call; invalid items are dropped."""
    user = "Dataset: %s\n\n%s\n\nSuggest %d questions." % (title, describe_tables(record), n + 2)
    got = llm.chat_json(SUGGEST, user, max_tokens=2500)
    out, seen = [], set()
    for item in got.get("questions") or []:
        q = str(item.get("question") or "").strip()
        if not q or q.lower() in seen:
            continue
        try:
            checked = _check(item, record)[1]
        except (ValueError, TypeError, KeyError, AttributeError):
            continue
        if GROUPED.search(q) and not (checked["aggregate"] and checked["aggregate"]["by"]) and not checked["order"]:
            continue   # "average X per year" without the grouping would answer a different question
        seen.add(q.lower())
        out.append({"question": q[:120], "query": checked})
        if len(out) == n:
            break
    return out
