"""Utility helpers shared by scrapers."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

CANONICAL_MAP = {
    # product
    "product": "product_name",
    "product name": "product_name",
    "name of product": "product_name",
    # batch / lot
    "batch": "batch_number",
    "batch no": "batch_number",
    "batch number": "batch_number",
    "batch number ": "batch_number",
    "lot": "batch_number",
    "lot no": "batch_number",
    "lot number": "batch_number",
    # expiry
    "expiry": "expiry_date",
    "expiry date": "expiry_date",
    "expiration date": "expiry_date",
    "exp date": "expiry_date",
    # manufacture
    "manufacturing date": "date_of_manufacture",
    "manufacture date": "date_of_manufacture",
    "date of manufacture": "date_of_manufacture",
    "mfg date": "date_of_manufacture",
    # manufacturer
    "manufacturer": "stated_manufacturer",
    "stated manufacturer": "stated_manufacturer",
    "stated product manufacturer": "stated_manufacturer",
    "product manufacturer": "stated_manufacturer",
}



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


def normalize_key(label: str, return_none: bool = False) -> Optional[str]:
    """
    Normalize table/header labels into canonical keys used by the pipeline.
    """
    if not label:
        return None

    # --- basic cleanup ---
    label = clean_text(label)
    label = label.rstrip(":")
    label = re.sub(r"[^\w\s]+", " ", label)  # drop punctuation
    label = re.sub(r"\s+", " ", label).lower()

    # exact match first
    if label in CANONICAL_MAP:
        return CANONICAL_MAP[label]

    # partial / contains-based matching (very important)
    for key, canonical in CANONICAL_MAP.items():
        if key in label:
            return canonical

    # fallback: snake_case the cleaned label
    return re.sub(r"\s+", "_", label) if not return_none else None


def select_one_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    if not selector:
        return None
    el = soup.select_one(selector)
    if not el:
        return None
    return clean_text(el.get_text(" ", strip=True))


def absolutize(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def parse_date(value: Optional[str]) -> Optional[datetime]:
    """Parse various date formats encountered in NAFDAC/FDA pages."""

    if not value:
        return None

    value = value.strip()

    # --- 1) Month-Year formats (10-2020, 10/2020) ---
    # Normalize separator
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


def extract_title(title: str) -> str:
    return re.search(r"[-–]\s*(.+)", title).group(1)


def extract_country_from_title(title: str) -> Optional[str]:
    if not title:
        return None

    m = re.search(r"\b(?:in)\s+([A-Z][A-Za-z\s]+)$", title.strip())
    if not m:
        return None

    return m.group(1).strip()


def extract_brand_name_and_generic_name_from_title(
    title: str,
) -> Tuple[Optional[str], Optional[str]]:
    if not title:
        return None, None

    m = re.search(r"([A-Z][A-Za-z0-9\-]*)\s*\(([^)]+)\)", title.strip())
    if not m:
        return None, None

    return m.group(1).strip(), m.group(2).strip()


def table_to_grid(tbl: Tag) -> list[list[str]]:
    # Get all rows
    trs = tbl.select("tr")
    if not trs:
        return []

    # Determine expected column count from the widest row (respecting colspan)
    def row_width(tr: Tag) -> int:
        width = 0
        for cell in tr.find_all(["td", "th"], recursive=False):
            colspan = int(cell.get("colspan", 1) or 1)
            width += colspan
        return width

    ncols = max(row_width(tr) for tr in trs) if trs else 0
    if ncols == 0:
        return []

    grid: list[list[Optional[str]]] = []
    # pending rowspans: col_idx -> (rows_remaining, value)
    pending: dict[int, tuple[int, str]] = {}

    for tr in trs:
        row: list[Optional[str]] = [None] * ncols

        # Prefill from pending rowspans
        for col_idx, (remain, val) in list(pending.items()):
            if remain > 0:
                row[col_idx] = val
                pending[col_idx] = (remain - 1, val)
            if pending[col_idx][0] == 0:
                pending.pop(col_idx, None)

        # Fill with this row's cells
        col_ptr = 0
        for cell in tr.find_all(["td", "th"], recursive=False):
            # Find next empty slot
            while col_ptr < ncols and row[col_ptr] is not None:
                col_ptr += 1
            if col_ptr >= ncols:
                break

            text = cell_text(cell)  # <-- your existing cleaner
            colspan = int(cell.get("colspan", 1) or 1)
            rowspan = int(cell.get("rowspan", 1) or 1)

            # Place across colspan
            for j in range(colspan):
                if col_ptr + j < ncols:
                    row[col_ptr + j] = text

                    # Register rowspan for each column this cell covers
                    if rowspan > 1:
                        pending[col_ptr + j] = (rowspan - 1, text)

            col_ptr += colspan

        grid.append(row)

    # Convert None -> "" and strip
    out: list[list[str]] = []
    for r in grid:
        rr = [(c or "").strip() for c in r]
        # keep the row if it has at least one non-empty cell
        if any(rr):
            out.append(rr)

    return out


def get_first_name(names: str | list[str]) -> str:
    if isinstance(names, list):
        names = names[0]
    return names.split(" ")[0]


def normalize_drug_name(name: str) -> str:
    if not name:
        return ""

    name = name.lower()

    # remove dosage (e.g., 500mg, 10 ml, etc.)
    name = re.sub(r"\b\d+(\.\d+)?\s*(mg|mcg|g|ml|%)\b", "", name)

    # remove punctuation
    name = re.sub(r"[^a-z0-9\s]", " ", name)

    # collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    return name


def extract_drug_tokens(text: str) -> list[str]:
    """
    Takes a string like:
        'Abecma (Idecabtagene Vicleucel)'
    and returns:
        ['abecma', 'idecabtagene', 'vicleucel']
    """
    if not text:
        return []

    # Remove parentheses but keep their contents
    cleaned = re.sub(r"[()]", "", text)

    # Split on whitespace
    tokens = cleaned.split()

    # remove unwanted tokens like in and for
    tokens = [token.lower() for token in tokens]
    tokens = set(tokens) - set(
        [
            "and",
            "for",
            "in",
            "tablets",
            "tablet",
            "injection",
            "injections",
            "pills",
            "pill",
            "sterile",
            "powder",
        ]
    )

    return list(tokens)
