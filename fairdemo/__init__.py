"""Autonomous FAIR curation on the VAST Data Platform.

The pipeline has three steps, each a Data Engine function in deployment and a CLI command locally:

  profile  one file   -> format, header metadata, checksum            (extract.py)
  curate   one folder -> a proposed record, every field with evidence  (companions.py, curate.py, llm.py)
  publish  a reviewed record -> RO-Crate + Croissant next to the data  (rocrate.py)

fairscore.py scores a record against fixed rules, before and after curation.
"""

__version__ = "0.1.0"
