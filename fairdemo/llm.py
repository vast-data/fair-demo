"""OpenAI-compatible chat and embeddings, through any OpenAI-compatible gateway (for example LiteLLM).

Settings come from the environment (see .env.example):
  LITELLM_BASE_URL, LITELLM_API_KEY, FAIR_CHAT_MODEL, FAIR_EMBED_MODEL
and HTTPS_PROXY if the gateway is reached through a proxy.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.request

CHAT_MODEL = os.environ.get("FAIR_CHAT_MODEL", "")    # a chat model that can return JSON
EMBED_MODEL = os.environ.get("FAIR_EMBED_MODEL", "")  # 1024-dimension embeddings (fairdemo/catalog.py DIM)


class LLMError(RuntimeError):
    pass


def _post(path: str, body: dict, timeout: int = 180) -> dict:
    base = os.environ.get("LITELLM_BASE_URL", "").rstrip("/")
    key = os.environ.get("LITELLM_API_KEY", "")
    if not base or not key:
        raise LLMError("LITELLM_BASE_URL and LITELLM_API_KEY must be set")
    data = json.dumps(body).encode()
    last = None
    for attempt in range(3):
        # A new Request each time: urllib marks a request as proxied on first use, and a reused one is
        # retried as a plain-HTTP tunnel to port 80.
        req = urllib.request.Request(base + path, data, {"Authorization": "Bearer " + key,
                                                          "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = LLMError("%s %s: %s" % (e.code, path, e.read().decode(errors="replace")[:300]))
            if e.code not in (429, 500, 502, 503, 504):
                raise last
        except (urllib.error.URLError, TimeoutError) as e:
            last = LLMError("%s: %s" % (path, e))
        time.sleep(2 * (attempt + 1))
    raise last


def chat_json(system: str, user: str, model: str | None = None, max_tokens: int = 3000) -> dict:
    """One chat turn that must return a JSON object."""
    if not (model or CHAT_MODEL):
        raise LLMError("no chat model: set FAIR_CHAT_MODEL")
    d = _post("/v1/chat/completions", {
        "model": model or CHAT_MODEL,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": 0.1,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    })
    text = (d["choices"][0]["message"].get("content") or "").strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, re.S)
        if m:
            return json.loads(m.group(0))
        raise LLMError("model did not return JSON: %r" % text[:300])


def embed(texts: list[str], model: str | None = None) -> list[list[float]]:
    if not (model or EMBED_MODEL):
        raise LLMError("no embedding model: set FAIR_EMBED_MODEL")
    d = _post("/v1/embeddings", {"model": model or EMBED_MODEL, "input": texts})
    return [row["embedding"] for row in sorted(d["data"], key=lambda r: r["index"])]


def models() -> list[str]:
    """The model names this key may use, as the gateway lists them."""
    base = os.environ.get("LITELLM_BASE_URL", "").rstrip("/")
    key = os.environ.get("LITELLM_API_KEY", "")
    if not base or not key:
        raise LLMError("LITELLM_BASE_URL and LITELLM_API_KEY must be set")
    req = urllib.request.Request(base + "/v1/models", headers={"Authorization": "Bearer " + key})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return sorted(m["id"] for m in json.load(r).get("data", []))
    except urllib.error.HTTPError as e:
        raise LLMError("%s /v1/models: %s" % (e.code, e.read().decode(errors="replace")[:300]))

