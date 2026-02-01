"""Utility helpers shared by scrapers."""

from __future__ import annotations

from ast import List
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag


def load_source_cfg(sources_path: str, source_key: str) -> Dict[str, Any]:
    data = json.loads(Path(sources_path).read_text(encoding="utf-8"))
    try:
        return data[source_key]
    except KeyError as e:
        raise KeyError(f"Source key '{source_key}' not found in {sources_path}") from e


def clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def cell_text(cell: Tag) -> str:
    """
    Joins all <p> or nested text inside a cell into one string.
    """
    return clean_text(cell.get_text(" ", strip=True))

def normalize_key(label: str) -> str:
    """
    Turn table labels into consistent snake-ish keys.
    e.g. "Stated Manufacturer" -> "stated_manufacturer"
    """
    label = clean_text(label).rstrip(":")
    label = re.sub(r"[^\w\s]+", "", label)  # drop punctuation
    label = re.sub(r"\s+", "_", label).lower()
    return label


def select_one_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    if not selector:
        return None
    el = soup.select_one(selector)
    if not el:
        return None
    return clean_text(el.get_text(" ", strip=True))

def select_all_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    """
    Return a single cleaned string concatenating text from all elements
    matching the selector.
    """
    if not selector:
        return None

    elements = soup.select(selector)
    if not elements:
        return None

    parts = []
    for el in elements:
        char = el.get_text(" ", strip=True)
        if char:
            char = " ".join(char.split())
            parts.append(char)

    return " ".join(parts) if parts else None


def absolutize(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def extract_by_regex(body_text: str, pattern: str) -> Optional[str]:
    if not body_text or not pattern:
        return None

    m = re.search(pattern, body_text, flags=re.IGNORECASE)
    if not m:
        return None

    # If the regex has capturing groups, take the last captured group.
    value = m.group(m.lastindex) if m.lastindex else m.group(0)
    return clean_text(value)


def parse_nafdac_date(value: Optional[str]) -> Optional[datetime]:
    """
    Parse NAFDAC listing dates like '15-Oct-25' into datetime.
    Returns None if parsing fails.
    """
    if not value:
        return None

    value = value.strip()
    for fmt in ("%d-%b-%y", "%d-%b-%Y", "%d-%B-%y", "%d-%B-%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass

    # Try ISO-ish values (some pages use 2026-01-09)
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_month_year(value: Optional[str]) -> Optional[datetime]:
    """
    Parse month-year strings like:
      - '10-2020'
      - '10/2020'
    into a datetime object (YYYY-MM-01).

    Returns None if parsing fails.
    """
    if not value:
        return None

    value = value.strip()

    # Normalize separator to "-"
    value = re.sub(r"[\/]", "-", value)

    try:
        return datetime.strptime(value, "%m-%Y")
    except ValueError:
        return None


