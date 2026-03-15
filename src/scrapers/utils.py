"""Utility helpers shared by scrapers."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin


def absolutize(base_url: str, href: str) -> str:
    """Join two urls."""

    return urljoin(base_url, href)


def parse_date(value: Optional[str]) -> Optional[datetime]:
    """Parse various date formats encountered in NAFDAC/FDA pages."""

    if not value:
        return None

    value = value.strip()

    # --- 1) Month-Year formats (10-2020, 10/2020) ---
    month_year = re.sub(r"[\/]", "-", value)
    if re.fullmatch(r"\d{2}-\d{4}", month_year):
        try:
            return datetime.strptime(month_year, "%m-%Y")
        except ValueError:
            pass

    # --- 2) NAFDAC-style day-month-year formats ---
    for fmt in ("%d-%b-%y", "%d-%b-%Y", "%d-%B-%y", "%d-%B-%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass

    # --- 3) ISO / ISO-like formats ---
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def normalize_drug_name(name: str) -> str:
    """Normalize drug name according to NCI."""

    if not name:
        return ""

    name = name.lower()

    name = re.sub(r"[®™©]", "", name)

    # remove dosage (e.g., 500mg, 10 ml, etc.)
    name = re.sub(r"\b\d+(\.\d+)?\s*(mg|mcg|g|ml|%)\b", "", name)

    # remove punctuation
    name = re.sub(r"[^a-z0-9\s]", " ", name)

    # collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    return name


def extract_drug_token(text: str) -> str:
    """Extract drug names from a text."""

    if not text:
        return

    # Remove parentheses but keep their contents
    cleaned = re.sub(r"[()]", "", text)

    # Split on whitespace and get the first one
    token = cleaned.split()[0]

    return token.lower()


def read_json(file_path: str) -> dict:
    """Read a json file."""

    with open(file_path, "r") as file:
        data = json.load(file)
    return data
