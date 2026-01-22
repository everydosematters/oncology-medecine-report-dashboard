"""Module for utility functions."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def load_source_cfg(sources_path: str, source_key: str) -> Dict[str, Any]:
    p = Path(sources_path)
    data = json.loads(p.read_text(encoding="utf-8"))
    if source_key not in data:
        raise KeyError(f"Source key '{source_key}' not found in {sources_path}")
    return data[source_key]


def clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def select_one_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    if not selector:
        return None
    el = soup.select_one(selector)
    if not el:
        return None
    return clean_text(el.get_text(" ", strip=True))


def absolutize(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def extract_by_regex(body_text: str, pattern: str) -> Optional[str]:
    if not body_text or not pattern:
        return None
    m = re.search(pattern, body_text, flags=re.IGNORECASE)
    if not m:
        return None
    val = m.group(m.lastindex) if m.lastindex else m.group(0)
    return clean_text(val)
