"""A site's look: its palette and logo letter, so partner sites look like the different institutions they are.

FAIR_SITE_THEME names a palette ("teal", the default, or "indigo"); FAIR_SITE_LOGO is the letter in the logo
("F" by default). The portal (portal/app.py) and the landing pages (rocrate.py) both use them.
"""
from __future__ import annotations

import os

PALETTES = {
    # The portal's own palette: what index.html and the landing pages already use.
    "teal": {"light": {"bg": "#f3f6f5", "panel": "#fff", "ink": "#183530", "muted": "#617670", "line": "#dce5e1",
                       "soft": "#edf3f0", "accent": "#087b66", "accent-ink": "#fff"},
             "dark": {"bg": "#101c19", "panel": "#172722", "ink": "#e6f1eb", "muted": "#a0b8ad", "line": "#30473d",
                      "soft": "#20362d", "accent": "#69d5b6", "accent-ink": "#10251e"}},
    # A partner university in the catalog-of-catalogs demo: deep indigo.
    "indigo": {"light": {"bg": "#f4f5fa", "panel": "#fff", "ink": "#1c2240", "muted": "#5f6684", "line": "#dfe2ee",
                         "soft": "#eceef7", "accent": "#3949ab", "accent-ink": "#fff"},
               "dark": {"bg": "#12142a", "panel": "#1a1d38", "ink": "#e8eaf6", "muted": "#a3a8c8", "line": "#33385e",
                        "soft": "#23274a", "accent": "#9fa8ff", "accent-ink": "#12142a"}},
}


def name() -> str:
    n = os.environ.get("FAIR_SITE_THEME", "teal").strip().lower()
    return n if n in PALETTES else "teal"


def logo() -> str:
    return (os.environ.get("FAIR_SITE_LOGO", "F").strip() or "F")[:2]


def _vars(tokens: dict) -> str:
    return ";".join(f"--{k}:{v}" for k, v in tokens.items())


def css(selector_light=":root", dark_selectors=(":root:not([data-theme=\"light\"])",), toggle=True) -> str:
    """The palette as CSS custom properties, light first, then dark: following the system, and (with toggle)
    an explicit data-theme choice either way, as the portal's theme control sets."""
    p = PALETTES[name()]
    out = f"{selector_light}{{{_vars(p['light'])}}}\n"
    out += "@media (prefers-color-scheme:dark){%s{%s}}\n" % (",".join(dark_selectors), _vars(p["dark"]))
    if toggle:
        out += f":root[data-theme=\"dark\"]{{{_vars(p['dark'])}}}\n:root[data-theme=\"light\"]{{{_vars(p['light'])}}}\n"
    return out
