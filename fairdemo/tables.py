"""Published data as tables in VAST DataBase, so it can be queried where it lives, not only found.

At publication, every file the pipeline can read as records becomes a table in fair-db/data:

  CSV, Parquet   one row per row
  FITS           one table per binary table HDU (a TESS light curve: one row per cadence)
  NetCDF         one row per grid cell, with its coordinates (OISST: lat, lon, time and four fields)
  VCF            one row per variant: CHROM..FILTER and the INFO fields the header declares
  ROOT           one row per event for the flat branches, and a child table per collection of jagged
                 branches (Muon_pt, Muon_eta, ...), one row per object with the event's entry number

Each column keeps what curation established about it: units, CF standard name and the description a
person accepted. The table's entry in the record names the source file and its SHA-256, so every row
traces back to the published bytes (and the snapshot that preserves them).

The same code reads them back: VastTables through the vastdb SDK (predicates and column selection are
pushed down to VAST), LocalTables from Parquet files when there is no cluster. Filters are parsed into
predicates, never into SQL text, so a query can't do anything but read the table it names. The SQL shown
alongside is what Trino would run for the same question: the Trino that VAST runs as a managed application
on the cluster's own CNodes (5.4 and later), so SQL needs no separate query cluster.
"""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import math
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

# Budgets: anything larger is published as files only, with a note saying why. They are checked from
# file metadata before reading, so a huge upload never has to fit in the function's memory.
MAX_FILE_BYTES = int(os.environ.get("FAIR_TABLE_MAX_MB", "512")) * 10 ** 6   # per source file
MAX_CELLS = int(os.environ.get("FAIR_TABLE_MAX_CELLS", "30000000"))           # rows x columns, per table
MAX_ROWS = 5_000_000                                                         # per table
MAX_COLUMNS = 200          # as many as the profile lists variables (extract.MAX_VARIABLES)
LOAD_SECONDS = int(os.environ.get("FAIR_TABLE_SECONDS", "300"))              # per dataset, all its tables
TABULAR = {"CSV", "Parquet", "FITS", "NetCDF", "VCF", "ROOT"}
OPS = ("<=", ">=", "!=", "=", "<", ">")


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_") or "x"


def table_name(dataset: str, part: str | None) -> str:
    base = slug(dataset)
    if len(base) > 80:   # long estate paths: keep the end, which names the dataset, and stay unique
        base = base[-70:].lstrip("_") + "_" + hashlib.sha256(dataset.encode()).hexdigest()[:8]
    return base if not part else "%s__%s" % (base, slug(part))


# ---------------------------------------------------------------- converters: file -> [(part, table, sources)]
# `sources` maps each column to the variable name the profile gave it, which is how its metadata is found.
# Converters never fail a dataset over one column: what can't be read is dropped and noted. They check
# their size budget from metadata before reading, so a huge upload is noted and left as a file rather
# than read into memory.

def _too_big(cells: int, what: str):
    if cells > MAX_CELLS:
        raise TooBig("%s: %s cells, over the %s-cell budget for a table" % (what, f"{cells:,}", f"{MAX_CELLS:,}"))


class TooBig(ValueError):
    """Larger than a demo table should be: left as a file."""


def _safe(table: pa.Table, notes: list, where: str) -> pa.Table:
    """Only types every VAST DB client and SQL engine handles: signed integers, floats, strings, booleans,
    dates and microsecond timestamps. NaN in a float column becomes null. Unsigned integers become int64 (text if they don't fit), decimals
    float64, times text; binary and nested columns are dropped (and noted)."""
    cols, fields = [], []
    for f, col in zip(table.schema, table.columns):
        t = f.type
        try:
            if pa.types.is_dictionary(t):
                col, t = col.cast(t.value_type), t.value_type
            if pa.types.is_null(t) or pa.types.is_large_string(t):
                col, t = col.cast(pa.string()), pa.string()
            elif pa.types.is_unsigned_integer(t):
                mx = pc.max(col).as_py()
                col, t = (col.cast(pa.int64()), pa.int64()) if mx is None or mx < 2 ** 63 else (col.cast(pa.string()), pa.string())
            elif pa.types.is_float16(t):
                col, t = col.cast(pa.float32()), pa.float32()
            elif pa.types.is_decimal(t):
                col, t = col.cast(pa.float64()), pa.float64()
            elif pa.types.is_date64(t):
                col, t = col.cast(pa.date32()), pa.date32()
            elif pa.types.is_timestamp(t) and (t.unit != "us" or t.tz):
                # microseconds, as UTC wall time (nanoseconds are truncated)
                col = pc.cast(col, pa.timestamp("us", tz=t.tz), safe=False)
                col, t = (col.cast(pa.timestamp("us")) if t.tz else col), pa.timestamp("us")
            elif pa.types.is_time(t) or pa.types.is_duration(t):
                col, t = col.cast(pa.string()), pa.string()
            elif not (pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_string(t)
                      or pa.types.is_boolean(t) or pa.types.is_date32(t) or pa.types.is_timestamp(t)):
                notes.append("%s: column %s (%s) left out of the table" % (where, f.name, t))
                continue
            if pa.types.is_floating(t) and pc.any(pc.is_nan(col)).as_py():
                # NaN marks a missing value (FITS light curves, NetCDF fill values): store it as null, so
                # averages, minimums and filters skip it, and answers stay valid JSON.
                col = pc.if_else(pc.is_nan(col), pa.scalar(None, t), col)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, OverflowError) as e:
            notes.append("%s: column %s left out (%s)" % (where, f.name, str(e)[:80]))
            continue
        cols.append(col)
        fields.append(pa.field(f.name, t))
    if not cols:
        raise ValueError("no column could be stored")
    return pa.Table.from_arrays(cols, schema=pa.schema(fields))


def _unique(names: list[str]) -> list[str]:
    seen, out = {}, []
    for n in names:
        n = slug(n)
        seen[n] = seen.get(n, 0) + 1
        out.append(n if seen[n] == 1 else "%s_%d" % (n, seen[n]))
    return out


def _csv(path: Path, profile: dict, notes: list):
    """Arrow's reader first; if a column changes type part-way down (a code column with letters in row
    40,000), read every column as text and give each the narrowest type all its values fit. Ragged rows
    are skipped and counted; a file that isn't UTF-8 is read as Latin-1, which accepts any bytes."""
    import pyarrow.csv as pcsv
    delim = (profile.get("attributes") or {}).get("delimiter") or ","
    skipped = []

    def ragged(row):
        skipped.append(row.number)
        return "skip"
    parse = pcsv.ParseOptions(delimiter=delim, invalid_row_handler=ragged, newlines_in_values=True)

    def read(enc, as_text=False):
        skipped.clear()
        ro = pcsv.ReadOptions(encoding=enc)
        conv = None
        if as_text:
            names = pcsv.open_csv(path, parse_options=parse, read_options=ro).schema.names
            conv = pcsv.ConvertOptions(column_types={n: pa.string() for n in names})
        return pcsv.read_csv(path, parse_options=parse, read_options=ro, convert_options=conv)

    with open(path, "rb") as fh:
        sample = fh.read(1 << 20)
    if b"\x00" in sample:
        raise ValueError("binary data, not delimited text")
    head = sample if len(sample) < (1 << 20) else sample[: sample.rfind(b"\n") + 1]
    try:
        head.decode("utf-8")
        encodings = ("utf8", "latin1")
    except UnicodeDecodeError:
        encodings = ("latin1",)
    t = None
    for enc in encodings:
        try:
            try:
                t = read(enc)
            except pa.ArrowInvalid as e:
                if "conversion error" not in str(e).lower() and "In CSV column" not in str(e):
                    raise
                t = read(enc, as_text=True)
                t = pa.table({n: _narrowest(t.column(n)) for n in t.column_names})
                notes.append("%s: mixed values in some columns; each column typed by what all its values fit" % path.name)
        except pa.ArrowInvalid:
            if enc == "utf8":
                continue
            raise
        if enc == "utf8" and any(pa.types.is_binary(f.type) for f in t.schema):
            continue   # bytes that aren't UTF-8 text
        if enc == "latin1":
            notes.append("%s: not UTF-8; read as Latin-1" % path.name)
        break
    if skipped and len(skipped) > t.num_rows:
        raise ValueError("%d of %d rows don't parse: not a delimited text file" % (len(skipped), len(skipped) + t.num_rows))
    if skipped:
        notes.append("%s: %d malformed row(s) skipped" % (path.name, len(skipped)))
    originals = [c.strip() for c in t.column_names]
    names = _unique(originals)
    return [(None, t.rename_columns(names), dict(zip(names, originals)))]


def _narrowest(col):
    col = pc.if_else(pc.equal(pc.utf8_trim_whitespace(col), ""), pa.scalar(None, pa.string()), col)
    for t in (pa.int64(), pa.float64()):
        try:
            return col.cast(t)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            pass
    return col


def _parquet(path: Path, profile: dict, notes: list):
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    _too_big(pf.metadata.num_rows * max(1, len(pf.schema_arrow)), path.name)
    t = pf.read()
    names = _unique(t.column_names)
    return [(None, t.rename_columns(names), dict(zip(names, t.column_names)))]


def _native(a: np.ndarray) -> np.ndarray:
    return a.astype(a.dtype.newbyteorder("=")) if a.dtype.byteorder not in ("=", "|") else a


def _fits(path: Path, profile: dict, notes: list):
    from astropy.io import fits
    out = []
    with fits.open(path, memmap=True) as hdul:
        for i, hdu in enumerate(hdul):
            if not isinstance(hdu, fits.BinTableHDU) or hdu.data is None:
                continue
            prefix = hdu.name or "HDU%d" % i
            _too_big(len(hdu.data) * len(hdu.columns), "%s %s" % (path.name, prefix))
            cols, sources = {}, {}
            for c in hdu.columns[:MAX_COLUMNS]:
                try:
                    a = np.asarray(hdu.data[c.name])
                    if a.ndim != 1:   # vector cells (images per row) stay in the file
                        continue
                    name = _unique(list(cols) + [c.name])[-1]
                    cols[name] = pa.array(_native(a)) if a.dtype.kind not in "SU" else pa.array(a.astype(str))
                    sources[name] = "%s.%s" % (prefix, c.name)
                except Exception as e:  # noqa: BLE001 - variable-length arrays and the like
                    notes.append("%s %s: column %s left out (%s)" % (path.name, prefix, c.name, str(e)[:60]))
            if cols:
                out.append((prefix, pa.table(cols), sources))
    return out


def _netcdf(path: Path, profile: dict, notes: list):
    """The largest group of numeric data variables sharing their dimensions, one row per grid cell.
    Cells where every variable is missing (land, in a sea-surface field) are dropped."""
    import netCDF4
    with netCDF4.Dataset(path) as ds:
        ds.set_auto_maskandscale(True)
        groups = {}
        for name, v in ds.variables.items():
            if name not in v.dimensions and len(v.dimensions) >= 1 and getattr(v.dtype, "kind", "O") in "iufb":
                groups.setdefault(v.dimensions, []).append(name)
        if not groups:
            return []
        size = lambda d: int(np.prod([len(ds.dimensions[x]) for x in d]))  # noqa: E731
        dims = max(groups, key=lambda d: (size(d), len(groups[d])))
        shape = [len(ds.dimensions[d]) for d in dims]
        n = size(dims)
        names = groups[dims][:MAX_COLUMNS - len(dims)]
        _too_big(n * (len(names) + len(dims)), path.name)
        data = {}
        keep = np.zeros(n, dtype=bool)
        for name in names:
            try:
                a = np.ma.asarray(ds.variables[name][:])
                a = np.ma.masked_invalid(a if a.dtype.kind == "f" else a.astype("f8")).reshape(-1)
            except Exception as e:  # noqa: BLE001 - an unreadable variable: leave it in the file
                notes.append("%s: variable %s left out (%s)" % (path.name, name, str(e)[:60]))
                continue
            mask = np.ma.getmaskarray(a)
            keep |= ~mask
            data[name] = (np.ma.getdata(a), mask)
        if not data:
            return []
        cols, sources = {}, {}
        idx = np.unravel_index(np.arange(n)[keep], shape)
        for d, ix in zip(dims, idx):
            v = ds.variables.get(d)
            try:
                vals = np.asarray(v[:]) if v is not None and v.ndim == 1 else np.arange(len(ds.dimensions[d]))
            except Exception:  # noqa: BLE001
                vals = np.arange(len(ds.dimensions[d]))
            col = vals[ix]
            units = getattr(v, "units", "") if v is not None else ""
            if isinstance(units, str) and " since " in units:
                try:
                    t = netCDF4.num2date(vals, units, getattr(v, "calendar", "standard"),
                                         only_use_cftime_datetimes=False, only_use_python_datetimes=True)
                    col = np.array(t, dtype="datetime64[us]")[ix]
                except Exception:  # noqa: BLE001 - a calendar Python can't represent: keep the numbers
                    pass
            name = slug(d)
            cols[name] = pa.array(_native(np.asarray(col)))
            sources[name] = d
        for name, (vals, mask) in data.items():
            n_ = _unique(list(cols) + [name])[-1]
            cols[n_] = pa.array(_native(vals[keep]), mask=mask[keep])
            sources[n_] = name
    return [(None, pa.table(cols), sources)]


VCF_CORE = [("chrom", "CHROM", pa.string()), ("pos", "POS", pa.int64()), ("id", "ID", pa.string()),
            ("ref", "REF", pa.string()), ("alt", "ALT", pa.string()), ("qual", "QUAL", pa.float64()),
            ("filter", "FILTER", pa.string())]
VCF_TYPES = {"Integer": pa.int64(), "Float": pa.float64(), "Flag": pa.bool_(), "String": pa.string()}


def _vcf(path: Path, profile: dict, notes: list):
    with open(path, "rb") as f:
        opener = gzip.open if f.read(2) == b"\x1f\x8b" else open
    info = []   # (id, number, type) in header order
    rows = {k: [] for k, _, _ in VCF_CORE}
    bad = 0
    with opener(path, "rt", errors="replace") as f:
        for line in f:
            if line.startswith("##INFO=<"):
                m = re.search(r"ID=([^,>]+),Number=([^,>]+),Type=(\w+)", line)
                if m and len(info) < MAX_COLUMNS - len(VCF_CORE) and "info:" + m.group(1) not in rows:
                    info.append(m.groups())
                    rows["info:" + m.group(1)] = []
                continue
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 8 or not p[1].isdigit():
                bad += 1
                continue
            for (k, _, t), v in zip(VCF_CORE, p[:7]):
                rows[k].append(None if v == "." else int(v) if k == "pos" else _num(v, float) if k == "qual" else v)
            kv = {}
            for item in (p[7].split(";") if p[7] != "." else []):
                key, eq, val = item.partition("=")
                kv[key] = val if eq else True
            for ident, number, typ in info:
                v = kv.get(ident)
                if typ == "Flag":
                    v = bool(v)
                elif v is not None and number in ("1", "A") and isinstance(v, str):
                    first = v.split(",")[0]
                    v = None if first == "." else _num(first, int) if typ == "Integer" else \
                        _num(first, float) if typ == "Float" else first
                elif v is not None:
                    v = str(v)
                rows["info:" + ident].append(v)
            if len(rows["pos"]) % 10000 == 0:
                _too_big(len(rows["pos"]) * len(rows), path.name)
    if bad:
        notes.append("%s: %d malformed record(s) skipped" % (path.name, bad))
    names = _unique([k for k, _, _ in VCF_CORE] + [i for i, _, _ in info])
    types = [t for _, _, t in VCF_CORE] + [VCF_TYPES.get(t, pa.string()) if n in ("1", "A") or t == "Flag"
                                           else pa.string() for _, n, t in info]
    keys = [k for k, _, _ in VCF_CORE] + ["info:" + i for i, _, _ in info]
    sources = [s for _, s, _ in VCF_CORE] + ["INFO/" + i for i, _, _ in info]
    cols = {}
    for n, k, ty, src in zip(names, keys, types, sources):
        try:
            cols[n] = pa.array(rows[k], type=ty)
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            cols[n] = pa.array([None if v is None else str(v) for v in rows[k]], type=pa.string())
            notes.append("%s: %s doesn't match its declared type; stored as text" % (path.name, src))
    return [(None, pa.table(cols), dict(zip(names, sources)))]


def _num(v, kind):
    try:
        return kind(v)
    except ValueError:
        return None


def _root(path: Path, profile: dict, notes: list):
    """Flat branches: one row per event (entry). Jagged ones become a child table per collection, one row
    per object, keyed by the event's entry and the object's index in it: TTree branches by their prefix
    (Muon_pt, Muon_eta -> muon), RNTuple collections by name (Muon.pt, Muon.eta -> muon). Each branch is
    read on its own, so one that uproot can't read (custom classes, strings) is noted and skipped."""
    import awkward as ak
    import uproot
    out = []
    with uproot.open(path) as f:
        for key, cls in f.classnames().items():
            if "TTree" not in cls and "RNTuple" not in cls:
                continue
            tree, tname = f[key], key.split(";")[0]
            fields_ = tree.keys(recursive=False)[:MAX_COLUMNS]
            _too_big(tree.num_entries * max(1, len(fields_)), "%s %s" % (path.name, tname))
            flat, groups = {}, {}   # groups: name -> [(column, source, jagged array)]
            for field in fields_:
                try:
                    a = tree.arrays([field], library="ak")[field]
                    t = a.type.content
                except Exception as e:  # noqa: BLE001
                    notes.append("%s %s: branch %s left out (%s)" % (path.name, tname, field, str(e)[:60]))
                    continue
                if isinstance(t, ak.types.NumpyType):
                    flat[field] = a
                elif isinstance(t, ak.types.ListType) and isinstance(t.content, ak.types.NumpyType):
                    g, _, sub = field.partition("_")
                    groups.setdefault(g if sub else field, []).append((sub or field, "%s/%s" % (tname, field), a))
                elif isinstance(t, ak.types.ListType) and isinstance(t.content, ak.types.RecordType):
                    for sub in a.fields:
                        if isinstance(a[sub].type.content.content, ak.types.NumpyType):
                            groups.setdefault(field, []).append((sub, "%s/%s.%s" % (tname, field, sub), a[sub]))
                else:
                    notes.append("%s %s: branch %s (%s) left out" % (path.name, tname, field, t))
            if flat:
                cols = {"entry": pa.array(np.arange(tree.num_entries, dtype=np.int64))}
                sources = {"entry": None}
                for b, a in flat.items():
                    n = _unique(list(cols) + [b])[-1]
                    cols[n], sources[n] = pa.array(_native(ak.to_numpy(a))), "%s/%s" % (tname, b)
                out.append((tname, pa.table(cols), sources))
            for group, members in groups.items():
                first = members[0][2]
                counts = ak.to_numpy(ak.num(first, axis=1))
                _too_big(int(counts.sum()) * (len(members) + 2), "%s %s_%s" % (path.name, tname, group))
                cols = {"entry": pa.array(np.repeat(np.arange(len(first), dtype=np.int64), counts)),
                        "idx": pa.array(ak.to_numpy(ak.flatten(ak.local_index(first))).astype(np.int32))}
                sources = {"entry": None, "idx": None}
                for col, src, a in members:
                    if not np.array_equal(ak.to_numpy(ak.num(a, axis=1)), counts):
                        continue   # a different multiplicity: not the same objects
                    n = _unique(list(cols) + [col])[-1]
                    cols[n], sources[n] = pa.array(_native(ak.to_numpy(ak.flatten(a)))), src
                out.append(("%s_%s" % (tname, group), pa.table(cols), sources))
    return out


CONVERTERS = {"CSV": _csv, "Parquet": _parquet, "FITS": _fits, "NetCDF": _netcdf, "VCF": _vcf, "ROOT": _root}


def convert(path: Path, profile: dict, notes: list | None = None) -> list[tuple[str | None, pa.Table, dict]]:
    """A file's tables. Raises (TooBig, or the reader's own error) only when none can be made; anything
    smaller that goes wrong is appended to `notes`."""
    notes = [] if notes is None else notes
    fn = CONVERTERS.get(profile.get("format"))
    if not fn:
        return []
    size = Path(path).stat().st_size
    if size > MAX_FILE_BYTES:
        raise TooBig("%s MB, over the %d MB limit for loading as a table" % (f"{size / 1e6:,.0f}", MAX_FILE_BYTES // 10 ** 6))
    parts = []
    for part, table, sources in fn(Path(path), profile, notes):
        where = Path(path).name + (" " + part if part else "")
        if table.num_rows > MAX_ROWS:
            raise TooBig("%s rows, over the %s-row limit" % (f"{table.num_rows:,}", f"{MAX_ROWS:,}"))
        if table.num_rows == 0:
            notes.append("%s: no rows, so no table" % where)
            continue
        parts.append((part, _safe(table, notes, where), sources))
    return parts


# ---------------------------------------------------------------- column metadata

def _variables(record: dict) -> dict:
    return {v["name"]: v for v in record.get("variables") or []}


def describe(columns: list[dict], record: dict) -> list[dict]:
    """Each column's units, standard name and description, as curation (and review) left them."""
    vs = _variables(record)
    out = []
    for c in columns:
        v = vs.get(c.get("source") or "") or {}
        d = v.get("description") or {}
        long_name = v.get("long_name")
        if long_name and re.fullmatch(r"[\w.]+(\[\w+\])*/[A-Za-z]", long_name):
            long_name = None   # a ROOT leaf list ("Muon_pt[nMuon]/F"): a type, not a meaning
        desc = long_name or (d.get("value") if d.get("status") == "accepted" else None)
        if c["name"] == "entry" and not c.get("source"):
            desc = "Entry (event) number in the source tree"
        elif c["name"] == "idx" and not c.get("source"):
            desc = "Index of the object within its event"
        units = v.get("units") or v.get("unit_inferred")
        if units and " since " in units and str(c.get("type", "")).startswith("timestamp"):
            units = None   # decoded into real times: "days since 1978-01-01" no longer applies
        out.append({**c, "units": units, "standard_name": v.get("standard_name"), "description": desc})
    return out


def _stat(v):
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, float):
        return round(v, 6) if math.isfinite(v) else None
    return v


def column_stats(table: pa.Table, sources: dict) -> list[dict]:
    cols = []
    for f, col in zip(table.schema, table.columns):
        c = {"name": f.name, "type": str(f.type), "source": sources.get(f.name), "nulls": col.null_count}
        t = f.type
        if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_temporal(t):
            mm = pc.min_max(col)
            c["min"], c["max"] = _stat(mm["min"].as_py()), _stat(mm["max"].as_py())
        elif pa.types.is_string(t):
            # The commonest values: what a person (or the model reading a question) needs to filter on
            vc = sorted(pc.value_counts(col).to_pylist(), key=lambda x: -x["counts"])
            c["distinct"] = len(vc)
            c["examples"] = [x["values"] for x in vc if x["values"] is not None and len(x["values"]) <= 40][:8]
        cols.append(c)
    return cols


# ---------------------------------------------------------------- where the tables live

class VastTables:
    """Tables in a VAST DataBase schema, through the vastdb SDK."""
    engine = "VAST DataBase"

    def __init__(self, session, bucket: str, schema: str):
        self.session, self.bucket, self.schema = session, bucket, schema

    def location(self, name: str) -> str:
        return "%s/%s/%s" % (self.bucket, self.schema, name)

    def _schema(self, tx):
        b = tx.bucket(self.bucket)
        return b.schema(self.schema, fail_if_missing=False) or b.create_schema(self.schema)

    def exists(self, name: str) -> bool:
        with self.session.transaction() as tx:
            return bool(self._schema(tx).table(name, fail_if_missing=False))

    def replace(self, name: str, table: pa.Table) -> None:
        with self.session.transaction() as tx:
            t = self._schema(tx).table(name, fail_if_missing=False)
            if t:
                t.drop()
        try:
            with self.session.transaction() as tx:
                t = self._schema(tx).create_table(name, table.schema)
                for batch in table.to_batches(max_chunksize=100_000):
                    t.insert(batch)
        except Exception:
            try:   # never leave half a table behind
                with self.session.transaction() as tx:
                    t = self._schema(tx).table(name, fail_if_missing=False)
                    if t:
                        t.drop()
            except Exception:  # noqa: BLE001
                pass
            raise

    def read(self, name: str, columns: list[str], where: list[tuple], limit: int | None) -> pa.Table:
        with self.session.transaction() as tx:
            t = self._schema(tx).table(name)
            return t.select(columns=columns, predicate=vast_predicate(where), limit_rows=limit).read_all()


def vast_predicate(where: list[tuple]):
    """Parsed filters as one ibis predicate, which the vastdb SDK pushes down to VAST DB."""
    from ibis import _
    pred = None
    for col, op, val in where:
        e = _[col]   # not getattr(_, col): a column named "filter" or "count" would be the ibis method
        e = {"=": e == val, "!=": e != val, "<": e < val, "<=": e <= val, ">": e > val, ">=": e >= val}[op]
        pred = e if pred is None else pred & e
    return pred


class LocalTables:
    """The same tables as Parquet files in a folder (local mode, tests)."""
    engine = "Parquet files (local mode)"

    def __init__(self, root: Path, bucket: str = "fair-db", schema: str = "data"):
        self.root, self.bucket, self.schema = Path(root), bucket, schema

    def location(self, name: str) -> str:
        return "%s/%s/%s" % (self.bucket, self.schema, name)

    def _path(self, name):
        return self.root / ("%s.parquet" % name)

    def exists(self, name: str) -> bool:
        return self._path(name).exists()

    def replace(self, name: str, table: pa.Table) -> None:
        import pyarrow.parquet as pq
        self.root.mkdir(parents=True, exist_ok=True)
        tmp = self._path(name).with_suffix(".tmp")
        pq.write_table(table, tmp)
        tmp.replace(self._path(name))

    def read(self, name: str, columns: list[str], where: list[tuple], limit: int | None) -> pa.Table:
        import pyarrow.dataset as ds
        expr = None
        for col, op, val in where:
            f = pc.field(col)
            e = {"=": f == val, "!=": f != val, "<": f < val, "<=": f <= val, ">": f > val, ">=": f >= val}[op]
            expr = e if expr is None else expr & e
        d = ds.dataset(self._path(name))
        return d.head(limit, columns=columns, filter=expr) if limit else d.to_table(columns=columns, filter=expr)


# ---------------------------------------------------------------- loading at publication

# Convert and write each file in a child process where that is safe (Linux, as on DataEngine): if the
# converter runs out of memory or a C library crashes on a corrupt file, only the child dies and the
# function goes on. Off by default elsewhere (macOS local mode runs threads; tests run in-process).
ISOLATE = os.environ.get("FAIR_TABLE_ISOLATE", "1" if sys.platform.startswith("linux") else "0") == "1"


def _load_file(sink, record: dict, fname: str, path: Path, single: bool) -> tuple[list[dict], list[str]]:
    """One file's tables: convert, write each, describe. `single`: the dataset has one tabular file."""
    f = next(x for x in record["files"] if x["name"] == fname)
    notes, out = [], []
    parts = convert(path, f, notes)
    for part, table, sources in parts:
        label = "_".join(x for x in ((None if single else Path(fname).stem), part) if x) or None
        name = table_name(record["key_prefix"], None if single and len(parts) == 1 else label)
        try:
            sink.replace(name, table)
        except Exception as e:  # noqa: BLE001 - e.g. a type the server refuses: the rest still load
            notes.append("%s: table %s couldn't be written (%s)" % (fname, name, str(e)[:200]))
            continue
        out.append({"table": name, "part": part, "source": fname, "source_sha256": f["sha256"],
                    "rows": table.num_rows, "columns": column_stats(table, sources),
                    "loaded": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")})
    return out, notes


def _child(conn, sink, record, fname, path, single):
    try:
        conn.send(("ok",) + _load_file(sink, record, fname, path, single))
    except BaseException as e:  # noqa: BLE001
        conn.send(("error", str(e)[:200] or type(e).__name__))
    finally:
        conn.close()


def _isolated(sink, record, fname, path, single, seconds) -> tuple[list[dict], list[str]]:
    import multiprocessing as mp
    ctx = mp.get_context("fork")
    here, there = ctx.Pipe(duplex=False)
    p = ctx.Process(target=_child, args=(there, sink, record, fname, path, single), daemon=True)
    p.start()
    there.close()
    try:
        got = here.recv() if here.poll(max(1, seconds)) else None
    except EOFError:   # the child died without a word: killed for memory, or crashed
        got = None
    p.join(10)
    killed_by_us = p.is_alive()
    if killed_by_us:
        p.kill()
        p.join()
    if got is None:
        timed_out = p.exitcode is None or (p.exitcode == -9 and killed_by_us)
        why = ("it took longer than the %d s loading budget" % LOAD_SECONDS if timed_out
               else "the converter ran out of memory or crashed (exit %s)" % p.exitcode)
        return [], ["%s: not loaded as a table (%s)" % (fname, why)]
    if got[0] == "error":
        return [], ["%s: not loaded as a table (%s)" % (fname, got[1])]
    return got[1], got[2]


def load(sink, record: dict, fetch, previous: list[dict] | None = None) -> tuple[list[dict], list[str]]:
    """Tables for a dataset's published files; fetch(name) returns a local copy of one. A table whose
    source bytes are unchanged since the last publication is kept as it is (its metadata is refreshed).

    Never raises for the data's sake: a file that can't be read, is over budget, makes the converter run
    out of memory, or whose table can't be written is skipped with a note, and the others still load.
    Stops starting new files once the dataset's time budget (LOAD_SECONDS) is spent. Returns the
    record's `tables` entries and the notes."""
    t0 = time.monotonic()
    by_name = {f["name"]: f for f in record["files"]}
    prev = {(t["source"], t.get("part")): t for t in previous or []}
    names = sorted(n for n, f in by_name.items() if f.get("format") in TABULAR)
    single = len(names) == 1
    out, notes = [], []
    for fname in names:
        f = by_name[fname]
        old = [t for (s, _), t in prev.items() if s == fname]
        try:
            if old and all(t["source_sha256"] == f["sha256"] and sink.exists(t["table"]) for t in old):
                out += [dict(t) for t in old]
                continue
            left = LOAD_SECONDS - (time.monotonic() - t0)
            if left <= 0:
                notes.append("%s: not loaded as a table (the dataset's %d s loading budget was used up)" % (fname, LOAD_SECONDS))
                continue
            if f.get("size", 0) > MAX_FILE_BYTES:   # before fetching it at all
                raise TooBig("%s MB, over the %d MB limit for loading as a table" % (f"{f['size'] / 1e6:,.0f}", MAX_FILE_BYTES // 10 ** 6))
            path = fetch(fname)
            got, more = (_isolated(sink, record, fname, path, single, left) if ISOLATE
                         else _load_file(sink, record, fname, path, single))
            out += got
            notes += more
        except Exception as e:  # noqa: BLE001 - the dataset still publishes, as files
            notes.append("%s: not loaded as a table (%s)" % (fname, str(e)[:200]))
    for entry in out:
        entry["location"] = sink.location(entry["table"])
        entry["columns"] = describe(entry["columns"], record)
    return out, notes


# ---------------------------------------------------------------- querying

def parse_where(clauses: list[str], columns: list[dict]) -> list[tuple]:
    """'lat>=40' -> ('lat', '>=', 40.0), with the value cast to the column's type. Unknown columns and
    values that don't fit the column are refused (ValueError)."""
    types = {c["name"]: c["type"] for c in columns}
    out = []
    for raw in clauses:
        m = re.fullmatch(r"\s*([A-Za-z0-9_]+)\s*(%s)\s*(.*?)\s*" % "|".join(re.escape(o) for o in OPS), raw or "")
        if not m:
            raise ValueError("filter %r: expected <column><op><value>, op one of %s" % (raw, " ".join(OPS)))
        col, op, val = m.groups()
        if col not in types:
            raise ValueError("no column %r" % col)
        out.append((col, op, cast(val.strip("'\""), types[col])))
    return out


def cast(val: str, typ: str):
    try:
        if typ.startswith(("int", "uint")):
            return int(val)
        if typ.startswith(("float", "double", "halffloat")):
            return float(val)
        if typ == "bool":
            return val.lower() in ("1", "true", "yes")
        if typ.startswith("timestamp"):
            return dt.datetime.fromisoformat(val.replace("Z", ""))
        if typ.startswith("date"):
            return dt.date.fromisoformat(val)
    except ValueError:
        raise ValueError("%r is not a valid %s" % (val, typ)) from None
    return val


AGGREGATES = {"avg": "mean", "min": "min", "max": "max", "sum": "sum", "count": "count"}


def check_aggregate(agg: dict | None, columns: list[dict]) -> dict | None:
    """{"fn": avg|min|max|sum|count, "column": c (any, for count), "by": c or None}, checked against the table."""
    if not agg or not agg.get("fn"):
        return None
    types = {c["name"]: c["type"] for c in columns}
    fn, col, by = agg["fn"], agg.get("column") or None, agg.get("by") or None
    if fn not in AGGREGATES:
        raise ValueError("aggregate %r: one of %s" % (fn, ", ".join(AGGREGATES)))
    if fn != "count" and col not in types:
        raise ValueError("aggregate %s needs a column of the table" % fn)
    if fn in ("avg", "sum") and not types[col].startswith(("int", "uint", "float", "double")):
        raise ValueError("%s(%s): not a numeric column" % (fn, col))
    for c in (col, by):
        if c is not None and c not in types:
            raise ValueError("no column %r" % c)
    return {"fn": fn, "column": col, "by": by}


def sql(entry: dict, columns: list[str], where: list[tuple], limit: int | None, aggregate: dict | None = None,
        order: dict | None = None) -> str:
    """The same question as SQL, for VAST-managed Trino on the cluster's CNodes (catalog vast, schema "bucket/schema")."""
    bucket, schema, name = entry["location"].split("/", 2)

    def lit(v):
        if isinstance(v, str):
            return "'%s'" % v.replace("'", "''")
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, dt.datetime):
            return "TIMESTAMP '%s'" % v.isoformat(sep=" ")
        if isinstance(v, dt.date):
            return "DATE '%s'" % v.isoformat()
        return repr(v)
    if aggregate:
        a = aggregate
        expr = "count(*)" if a["fn"] == "count" and not a["column"] else '%s("%s")' % (a["fn"], a["column"])
        sel = ('"%s", ' % a["by"] if a["by"] else "") + "%s AS %s" % (expr, agg_name(a))
    else:
        sel = ", ".join('"%s"' % c for c in columns)
    q = 'SELECT %s\nFROM vast."%s/%s".%s' % (sel, bucket, schema, name)
    if where:
        q += "\nWHERE " + " AND ".join('"%s" %s %s' % (c, "<>" if o == "!=" else o, lit(v)) for c, o, v in where)
    if aggregate and aggregate["by"]:
        q += '\nGROUP BY "%s"' % aggregate["by"]
    if order:
        q += '\nORDER BY %s%s' % (order["column"] if aggregate and order["column"] == agg_name(aggregate)
                                  else '"%s"' % order["column"], " DESC" if order["desc"] else "")
    elif aggregate and aggregate["by"]:
        q += '\nORDER BY "%s"' % aggregate["by"]
    if limit and not (aggregate and not aggregate["by"]):
        q += "\nLIMIT %d" % limit
    return q


def agg_name(a: dict) -> str:
    return "%s_%s" % (a["fn"], a["column"]) if a["column"] else a["fn"]


def aggregate_rows(table: pa.Table, a: dict, limit: int | None) -> pa.Table:
    """The aggregate over every matching row (the rows themselves were filtered where they are stored)."""
    col = a["column"] or table.column_names[0]
    if a["by"]:
        out = table.group_by(a["by"]).aggregate([(col, "count" if a["fn"] == "count" else AGGREGATES[a["fn"]])])
        out = out.rename_columns([agg_name(a) if n != a["by"] else n for n in out.column_names])
        return out.select([a["by"], agg_name(a)]).sort_by(a["by"])
    if a["fn"] == "count":
        v = table.num_rows if not a["column"] else len(table.column(col)) - table.column(col).null_count
    else:
        v = getattr(pc, AGGREGATES[a["fn"]])(table.column(col)).as_py()
    return pa.table({agg_name(a): [v]})


def summarise(table: pa.Table) -> list[dict]:
    """Count, min, max and mean of each numeric column over every matching row."""
    out = []
    for f, col in zip(table.schema, table.columns):
        if pa.types.is_integer(f.type) or pa.types.is_floating(f.type):
            mm = pc.min_max(col)
            out.append({"column": f.name, "count": len(col) - col.null_count, "min": _stat(mm["min"].as_py()),
                        "max": _stat(mm["max"].as_py()), "mean": _stat(pc.mean(col).as_py())})
    return out


def check_order(order: dict | None, entry: dict, agg: dict | None) -> dict | None:
    if not order or not order.get("column"):
        return None
    allowed = {c["name"] for c in entry["columns"]} if not agg else {x for x in (agg["by"], agg_name(agg)) if x}
    if order["column"] not in allowed:
        raise ValueError("can't sort by %r here (choose from %s)" % (order["column"], ", ".join(sorted(allowed))))
    return {"column": order["column"], "desc": bool(order.get("desc"))}


def _sorted(t: pa.Table, order: dict | None, limit: int | None) -> pa.Table:
    if order:
        t = t.sort_by([(order["column"], "descending" if order["desc"] else "ascending")])
    return t.slice(0, limit) if limit else t


def query(sink, entry: dict, columns: list[str] | None, where: list[str], limit: int | None,
          with_summary: bool = False, aggregate: dict | None = None, order: dict | None = None) -> dict:
    """Read a published table in place: chosen columns, filters ANDed, at most `limit` rows; or, with an
    aggregate, avg/min/max/sum/count over every matching row, optionally per value of one column."""
    names = [c["name"] for c in entry["columns"]]
    columns = [c for c in (columns or names) if c]
    bad = [c for c in columns if c not in names]
    if bad:
        raise ValueError("no column %s" % ", ".join(bad))
    filters = parse_where(where, entry["columns"])
    agg = check_aggregate(aggregate, entry["columns"])
    order = check_order(order, entry, agg)
    if agg:
        need = list(dict.fromkeys(x for x in (agg["by"], agg["column"]) if x)) or [names[0]]
        matched = sink.read(entry["table"], need, filters, None)
        rows = _sorted(aggregate_rows(matched, agg, None), order, limit)
        return {"table": entry["table"], "location": entry["location"], "engine": sink.engine,
                "columns": rows.column_names, "rows": rows, "aggregate": agg, "order": order, "matched": matched.num_rows,
                "summary": [], "sql": sql(entry, columns, filters, limit, agg, order)}
    if order and order["column"] not in columns:
        columns = columns + [order["column"]]
    # Sorting needs every match; otherwise only `limit` rows are read.
    whole = sink.read(entry["table"], columns, filters, None) if order else None
    rows = _sorted(whole, order, limit) if order else sink.read(entry["table"], columns, filters, limit)
    out = {"table": entry["table"], "location": entry["location"], "engine": sink.engine, "columns": columns,
           "rows": rows, "order": order, "sql": sql(entry, columns, filters, limit, None, order)}
    if with_summary:
        whole = whole if whole is not None else rows if limit is None or rows.num_rows < limit else \
            sink.read(entry["table"], columns, filters, None)
        out["matched"], out["summary"] = whole.num_rows, summarise(whole)
    return out
