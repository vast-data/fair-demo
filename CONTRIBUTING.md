# Contributing

Thanks for your interest. This is a demonstration, so the most useful contributions are the ones that make it
easier to try, adapt or trust:

- **Issues** for bugs, unclear docs, or results you can't reproduce. Please say which VAST version and which
  part (local mode, `deploy/de.py`, the portal) you used.
- **Pull requests** for fixes and small improvements. Keep them focused, and run the tests first:

  ```bash
  uv venv --python 3.12 && uv pip install -e '.[dev,portal,function]'
  .venv/bin/python -m pytest -q
  ```

- **New file formats** go in `fairdemo/extract.py`, with a small fixture in `tests/fixtures.py` and a test.
- **New demo datasets** need an open licence that allows reuse, and an entry in `data/sources.json` and
  `data/SOURCES.md` with the source, checksum and licence evidence. Don't commit the data itself.

Never commit `.env`, keys, passwords, or the state files under `deploy/`; `.gitignore` covers them.

By contributing, you agree that your contributions are licensed under the Apache License 2.0 (see `LICENSE`).
