"""Module for utiliy functions."""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseScraper, AlertRecord


def _load_source_cfg(sources_path: str, source_key: str) -> Dict[str, Any]:
    p = Path(sources_path)
    data = json.loads(p.read_text(encoding="utf-8"))
    if source_key not in data:
        raise KeyError(f"Source key '{source_key}' not found in {sources_path}")
    return data[source_key]


def _clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _select_one_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    if not selector:
        return None
    el = soup.select_one(selector)
    if not el:
        return None
    return _clean_text(el.get_text(" ", strip=True))


def _absolutize(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def _extract_by_regex(body_text: str, pattern: str) -> Optional[str]:
    if not body_text or not pattern:
        return None
    m = re.search(pattern, body_text, flags=re.IGNORECASE)
    if not m:
        return None
    if m.lastindex:
        val = m.group(m.lastindex)
    else:
        val = m.group(0)
    return _clean_text(val)