#!/usr/bin/env python3
"""Stage the demo datasets from data/raw into data/staged/<domain>/<dataset>/, as a researcher might leave them.

"documented" folders carry a short README and a CITATION.cff written from data/SOURCES.md (the verified
facts: authors, licence, DOI). They are deliberately not complete, as real ones aren't: the curator has
to fill in keywords, variable descriptions and coverage.
"dark" folders hold only the data, renamed the way files get renamed on scratch space: no licence, no
creators, nothing but what the file says about itself. Self-describing formats (NetCDF, FITS) still say
a lot; a CSV or a TIFF says almost nothing, which is the point of the demo.

    python3 scripts/stage_demo.py        # rebuilds data/staged/ from data/raw/
"""
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW, STAGED = ROOT / "data/raw", ROOT / "data/staged"

DOCUMENTED = {
    "climate/oisst-v2.1-2024-09-01": {
        "files": ["climate/oisst-avhrr-v02r01.20240901.nc"],
        "readme": """# OISST v2.1 daily SST, 1 September 2024

Daily sea surface temperature analysis downloaded from NOAA NCEI for the ocean heat-content study.
One day of the AVHRR-only "final" product on a quarter-degree global grid.

Please acknowledge NOAA NCEI and cite the dataset DOI below.
""",
        "cff": {"title": "NOAA 0.25-degree Daily Optimum Interpolation Sea Surface Temperature (OISST), Version 2.1",
                "authors": ["Boyin Huang", "Chunying Liu", "Viva F. Banzon", "Eric Freeman", "Garrett Graham",
                            "William Hankins", "Thomas M. Smith", "Huai-Min Zhang"],
                "doi": "10.25921/RE9P-PT57", "license": "CC0-1.0", "date": "2020-04-01"},
    },
    "astronomy/tess-s0001-tic261136679": {
        "files": ["astronomy/tess2018206045859-s0001-0000000261136679-0120-s_lc.fits"],
        "readme": """# TESS sector 1 light curve, TIC 261136679

Two-minute cadence light curve from the TESS Science Processing Operations Center (SPOC), taken from MAST
for the transit-timing work.

This includes data collected with the TESS mission, obtained from the MAST data archive at the Space
Telescope Science Institute (STScI). Funding for the TESS mission is provided by the NASA Explorer Program.
""",
        "cff": {"title": "TESS Light Curves - All Sectors (SPOC 2-minute cadence)",
                "authors": [{"name": "TESS Science Processing Operations Center"}],
                "doi": "10.17909/t9-nmc8-f686", "license": "CC0-1.0", "date": "2018-09-01"},
    },
    "particle-physics/cms-higgs-zz-4l-8tev": {
        "files": ["particle-physics/SMHiggsToZZTo4L.root"],
        "readme": """# SM Higgs to ZZ to 4 leptons, reduced NanoAOD

Simulated CMS events used for the H -> 4l outreach analysis. Derived from CMS record 9356 and reduced to
a flat ntuple for education and outreach.
""",
        "cff": {"title": "SMHiggsToZZTo4L dataset in reduced NanoAOD format for education and outreach",
                "authors": ["Stefan Wunsch"], "doi": "10.7483/OPENDATA.CMS.8FLU.UIQJ", "license": "CC0-1.0",
                "date": "2021-01-01"},
    },
    "genomics/gnomad-v2.1.1-exomes-chr21-slice": {
        "files": ["genomics/gnomad.exomes.r2.1.1.sites.21.vcf.bgz"],
        "readme": """# gnomAD v2.1.1 exome sites, chr21 (first 3,323 sites)

A slice of the gnomAD v2.1.1 exomes sites VCF for chromosome 21 (GRCh37): full header plus the first
3,323 records. Aggregate allele frequencies only; no individual genotypes.

This includes data from the gnomAD v2.1.1 release (https://gnomad.broadinstitute.org).
Cite Karczewski et al. 2020, Nature 581:434-443. Do not attempt to re-identify participants.
""",
        "cff": {"title": "gnomAD v2.1.1 exome sites, chromosome 21 (slice)",
                "authors": [{"name": "Genome Aggregation Database (gnomAD) Consortium"}],
                "doi": None, "license": "CC0-1.0", "date": "2019-03-06",
                "references": ["10.1038/s41586-020-2308-7"]},
    },
    "bioimaging/bbbc039-u2os-nuclei-sample": {
        "files": ["bioimaging/IXMtest_A02_s1_w1051DAA7C-7042-435F-99F0-1E847D9B42CB.tif",
                  "bioimaging/IXMtest_A06_s6_w1B9577918-4973-4A87-BA73-A168AA755527.tif",
                  "bioimaging/IXMtest_A09_s1_w1CE70AD49-290D-4312-82E6-CDC717F32637.tif"],
        "readme": """# BBBC039 sample: U2OS nuclei, Hoechst channel

Three fields of view from BBBC039v1 (Broad Bioimage Benchmark Collection), used to test nucleus
segmentation. We used image set BBBC039v1 Caicedo et al. 2018, available from the Broad Bioimage
Benchmark Collection [Ljosa et al., Nature Methods, 2012].
""",
        "cff": {"title": "BBBC039v1: nuclei of U2OS cells in a chemical screen (sample)",
                "authors": [{"name": "Broad Bioimage Benchmark Collection"}],
                "doi": None, "license": "CC0-1.0", "date": "2018-01-01",
                "references": ["10.1038/nmeth.2083"]},
    },
    "public-sector/dft-aadf-wales": {
        "files": ["public-sector/dft_aadf_region_id_4.csv"],
        "readme": """# DfT annual average daily flow, Wales

Road traffic counts by count point for Wales (region 4), from the Department for Transport road traffic
statistics. Contains public sector information licensed under the Open Government Licence v3.0.
""",
        "cff": {"title": "Road traffic statistics: annual average daily flow by count point, Wales",
                "authors": [{"name": "Department for Transport"}], "doi": None, "license": "OGL-UK-3.0",
                "date": "2025-06-01"},
    },
}

DARK = {
    "climate/scratch-run-0901": {"climate/oisst-avhrr-v02r01.20240901.nc": "out_0901.nc"},
    "particle-physics/user-skims-07": {"particle-physics/SMHiggsToZZTo4L.root": "skim_v7.root"},
    "bioimaging/plate-br21381-export": {
        "bioimaging/IXMtest_A02_s1_w1051DAA7C-7042-435F-99F0-1E847D9B42CB.tif": "img_0001.tif",
        "bioimaging/IXMtest_A06_s6_w1B9577918-4973-4A87-BA73-A168AA755527.tif": "img_0002.tif"},
    "public-sector/export-2025-q3": {"public-sector/dft_aadf_region_id_4.csv": "export_4.csv"},
}


def cff_text(c: dict) -> str:
    lines = ["cff-version: 1.2.0", "message: If you use this dataset, please cite it as below.",
             "type: dataset", f'title: "{c["title"]}"', "authors:"]
    for a in c["authors"]:
        if isinstance(a, dict):
            lines.append(f'  - name: "{a["name"]}"')
        else:
            given, family = a.rsplit(" ", 1)
            lines += [f'  - given-names: "{given}"', f'    family-names: "{family}"']
    if c.get("doi"):
        lines.append(f"doi: {c['doi']}")
    lines += [f"license: {c['license']}", f"date-released: {c['date']}"]
    if c.get("references"):
        lines.append("references:")
        for r in c["references"]:
            lines += ["  - type: article", f"    doi: {r}", '    title: "Reference publication"',
                      "    authors:", '      - name: "See DOI"']
    return "\n".join(lines) + "\n"


def main():
    shutil.rmtree(STAGED, ignore_errors=True)
    for folder, spec in DOCUMENTED.items():
        d = STAGED / folder
        d.mkdir(parents=True)
        for f in spec["files"]:
            shutil.copy(RAW / f, d / Path(f).name)
        (d / "README.md").write_text(spec["readme"])
        (d / "CITATION.cff").write_text(cff_text(spec["cff"]))
    for folder, files in DARK.items():
        d = STAGED / folder
        d.mkdir(parents=True)
        for src, name in files.items():
            shutil.copy(RAW / src, d / name)
    for d in sorted(p for p in STAGED.glob("*/*") if p.is_dir()):
        print("%-48s %s" % (d.relative_to(STAGED), ", ".join(sorted(x.name for x in d.iterdir()))))


if __name__ == "__main__":
    main()
