"""Scraper for NAFDAC Nigeria."""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, Tag

from src.models import DrugAlert
from src.database import upsert_df
from scrapers.config import NAFDAC_NG

from .base import BaseScraper
from .utils import absolutize, parse_date

# --- NAFDAC-specific helpers (table parsing, label normalization, etc.) ---

_CANONICAL_MAP = {
    "product": "product_name",
    "product name": "product_name",
    "name of product": "product_name",
    "batch": "batch_number",
    "batch no": "batch_number",
    "batch number": "batch_number",
    "batch number ": "batch_number",
    "lot": "batch_number",
    "lot no": "batch_number",
    "lot number": "batch_number",
    "expiry": "expiry_date",
    "expiry date": "expiry_date",
    "expiration date": "expiry_date",
    "exp date": "expiry_date",
    "manufacturing date": "date_of_manufacture",
    "manufacture date": "date_of_manufacture",
    "date of manufacture": "date_of_manufacture",
    "mfg date": "date_of_manufacture",
    "manufacturer": "stated_manufacturer",
    "stated manufacturer": "stated_manufacturer",
    "stated product manufacturer": "stated_manufacturer",
    "product manufacturer": "stated_manufacturer",
}


def _clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _cell_text(cell: Tag) -> str:
    return _clean_text(cell.get_text(" ", strip=True))


def _normalize_key(label: str, return_none: bool = False) -> Optional[str]:
    if not label:
        return None
    label = _clean_text(label)
    label = label.rstrip(":")
    label = re.sub(r"[^\w\s]+", " ", label)
    label = re.sub(r"\s+", " ", label).lower()
    if label in _CANONICAL_MAP:
        return _CANONICAL_MAP[label]
    for key, canonical in _CANONICAL_MAP.items():
        if key in label:
            return canonical
    return re.sub(r"\s+", "_", label) if not return_none else None


def _select_one_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    if not selector:
        return None
    el = soup.select_one(selector)
    if not el:
        return None
    return _clean_text(el.get_text(" ", strip=True))


def _remove_trademarks(name) -> str:
    return re.sub(r"[®™©]", "", name)


def _table_to_grid(tbl: Tag) -> list[list[str]]:
    trs = tbl.select("tr")
    if not trs:
        return []

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
    pending: dict[int, tuple[int, str]] = {}

    for tr in trs:
        row: list[Optional[str]] = [None] * ncols
        for col_idx, (remain, val) in list(pending.items()):
            if remain > 0:
                row[col_idx] = val
                pending[col_idx] = (remain - 1, val)
            if pending[col_idx][0] == 0:
                pending.pop(col_idx, None)

        col_ptr = 0
        for cell in tr.find_all(["td", "th"], recursive=False):
            while col_ptr < ncols and row[col_ptr] is not None:
                col_ptr += 1
            if col_ptr >= ncols:
                break
            text = _cell_text(cell)
            colspan = int(cell.get("colspan", 1) or 1)
            rowspan = int(cell.get("rowspan", 1) or 1)
            for j in range(colspan):
                if col_ptr + j < ncols:
                    row[col_ptr + j] = text
                    if rowspan > 1:
                        pending[col_ptr + j] = (rowspan - 1, text)
            col_ptr += colspan
        grid.append(row)

    out: list[list[str]] = []
    for r in grid:
        rr = [(c or "").strip() for c in r]
        if any(rr):
            out.append(rr)
    return out


def _get_first_name(names: str | list[str]) -> str:
    if not names:
        return ""
    if isinstance(names, list):
        names = names[0]
    return _remove_trademarks(names.split(" ")[0])


class NafDacScraper(BaseScraper):
    """Scraper for NAFDAC Nigeria recall and alert listings."""

    def __init__(self, start_date: datetime = None) -> None:
        """Initialize scraper with configuration and optional start date filter."""

        super().__init__(
            start_date=start_date,
        )
        self.cfg = NAFDAC_NG
        self.source_id = self.cfg["source_id"]
        self.source_org = self.cfg["source_org"]

    def _get_nafdac_record_id(self, text: str) -> Optional[str]:
        """Get the id of the recall from NAFDAC website."""

        pattern = r"(?:No\.\s*)?(\d{1,3}[A-Z]?/\d{4})"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group()
        return None

    def _extract_country_from_title(self, title: str) -> Optional[str]:
        """Extract country from title."""

        if not title:
            return None

        m = re.search(r"\b(?:in)\s+([A-Z][A-Za-z\s]+)$", title.strip())
        if not m:
            return None

        return m.group(1).strip()

    def _extract_brand_name_and_generic_name_from_title(
        self, title: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Extract brand name and generic name from title."""

        if not title:
            return None, None

        m = re.search(r"([A-Z][A-Za-z0-9\-]*)\s*\(([^)]+)\)", title.strip())
        if not m:
            return None, None

        return _remove_trademarks(m.group(1).strip()), _remove_trademarks(
            m.group(2).strip()
        )

    def _extract_product_name_from_text(self, tag: BeautifulSoup) -> Optional[str]:
        """Given a body text extract the product name."""

        pattern = r"^(.+?)\s+is\s+(?:an\s|a\s|used\s)"
        for p in tag.find_all("p"):
            txt = p.get_text(" ", strip=True)
            m = re.compile(pattern, re.IGNORECASE).search(txt)

            if m and (m.regs[0][0] != 0 or m.regs[0][-1] > 50):
                # parsed the wrong thing def
                continue
            if m:
                return m.group(1).strip()
        return None

    def _extract_product_specs_from_text(
        self, *soup: BeautifulSoup
    ) -> dict[str, list[str]]:
        """Extract specs from a table."""

        result: dict[str, list[str]] = {}

        for strong in soup[-1].find_all("strong"):
            raw_label = strong.get_text(" ", strip=True)
            key = _normalize_key(raw_label, return_none=True)
            if not key:
                continue

            sib = strong.next_sibling
            if not sib:
                continue

            value = (
                sib.strip() if isinstance(sib, str) else sib.get_text(" ", strip=True)
            )
            value = " ".join(value.split())
            # FIXME ['Phesgo® 600mg/600mg/10ml injection', '.']
            if value:
                result.setdefault(key, []).append(value)

        return result

    def _parse_nafdac_table(self, table: Tag) -> dict[str, list[str]]:
        """Returns a normalized column-oriented dict"""

        result: dict[str, list[str]] = {}

        rows = _table_to_grid(table)
        if not rows:
            return result

        ncols = len(rows[0])

        # CASE A: matrix table (>= 3 columns)
        if ncols >= 3:
            headers = [_normalize_key(h) for h in rows[0]]

            # Ensure we have keys
            for h in headers:
                if h:
                    result.setdefault(h, [])

            for r in rows[1:]:
                # Pad/truncate to header length
                if len(r) < ncols:
                    r = r + [""] * (ncols - len(r))
                r = r[:ncols]

                for i, h in enumerate(headers):
                    if not h:
                        continue
                    val = (r[i] or "").strip()
                    if val:
                        result[h].append(val)

            return result

        # CASE B: 2-column key/value table
        if ncols == 2:
            for label, value in rows:
                key = _normalize_key(label)
                val = (value or "").strip()
                if not key or not val:
                    continue
                result.setdefault(key, []).append(val)
            return result
        # fallback
        return result

    def _extract_product_specs(
        self,
        *soup: BeautifulSoup,
    ) -> dict:
        """Extracts the product specification like batch number and name."""

        parsed_table = defaultdict(None)
        for table in soup[-1].find_all("table"):
            table_specs = self._parse_nafdac_table(table)
            if table_specs:
                parsed_table.update(table_specs)
        return parsed_table

    def _parse_listing_page(
        self, soup: BeautifulSoup, listing_url: str
    ) -> List[DrugAlert]:
        """Reads table rows from tbody and extracts all necessary info."""

        listing_cfg = self.cfg.get("listing") or {}
        item_sel = listing_cfg.get("item_selector")
        link_sel = listing_cfg.get("link_selector")
        date_sel = listing_cfg.get("date_selector")
        fields = listing_cfg.get("fields") or {}

        rows = soup.select(item_sel) if item_sel else []

        results: List[DrugAlert] = []

        for row in rows:
            # First check that the alert is a drug, if not move on
            category = (
                _clean_text(
                    row.select_one(fields["category"]).get_text(" ", strip=True)
                )
                or ""
            )
            if not re.search(r"drug", category, re.IGNORECASE):
                # skip this if it is not a drug
                continue

            link_el = row.select_one(link_sel) if link_sel else None
            if not link_el or not link_el.get("href"):
                # TODO handle when no link is provided, just return title
                continue

            detail_url = absolutize(listing_url, link_el["href"])

            publish_date = None
            if date_sel:
                d_el = row.select_one(date_sel)
                publish_date = (
                    _clean_text(d_el.get_text(" ", strip=True)) if d_el else None
                )

            # standardize
            detail_scraped = self.scrape(detail_url)
            parsed = self._parse_detail_page(detail_scraped["html"])

            product_name = (
                parsed.get("product_name")
                or parsed.get("brand_name")
                or parsed.get("generic_name")
                or None
            )

            if not product_name:
                product_name = self._extract_product_name_from_text(
                    detail_scraped["html"]
                )

            query = _get_first_name(product_name)
            drug_name = self.get_nci_name(query)

            if not drug_name:
                continue

            publish_date = parse_date(publish_date)
            if self.start_date and publish_date:
                # FIXME this is so ugly but yeah
                if self.start_date > publish_date:
                    break

            manufacturer = _clean_text(
                row.select_one(fields["company"]).get_text(" ", strip=True)
            )

            record_id = self.make_record_id(
                self.source_id, drug_name, parsed.get("nafdac_record_id")
            )

            more_info = ""

            if parsed.get("batch_number"):
                more_info += "Batch Number: " + ", ".join(parsed.get("batch_number"))
            if parsed.get("expiry_date"):
                more_info += " Expiry Date: " + ", ".join(parsed.get("expiry_date"))

            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_org=self.source_org,
                    source_country=parsed.get("source_country") or "Nigeria",
                    source_url=detail_url,
                    publish_date=publish_date.isoformat(),
                    manufacturer=manufacturer,
                    reason=parsed.get("title"),
                    product_name=drug_name,
                    scraped_at=datetime.now(timezone.utc).isoformat(),
                    more_info=more_info,
                )
            )
        return results

    def _parse_detail_page(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract the contents of detail url."""

        raw_title = _select_one_text(soup, "h1")

        nafdac_record_id = self._get_nafdac_record_id(raw_title)

        title = re.search(r"[-–]\s*(.+)", raw_title)

        title = title.group(1) if title else raw_title

        source_country = self._extract_country_from_title(title)

        brand_name, generic_name = self._extract_brand_name_and_generic_name_from_title(
            title
        )

        if soup.find("table"):
            product_specs = self._extract_product_specs(soup)
        else:

            product_specs = self._extract_product_specs_from_text(soup)

        return {
            "title": title,
            "brand_name": brand_name,
            "generic_name": generic_name,
            "source_country": source_country,
            "nafdac_record_id": nafdac_record_id,
            **product_specs,
        }

    def standardize(self, upload_to_db: bool = False) -> List[DrugAlert]:
        """Standardize the extracted content."""

        listing_url = self.cfg[
            "base_url"
        ]  # Base is sufficient gives all the listings in this case

        listing_scraped = self.scrape(listing_url)
        results = self._parse_listing_page(
            soup=listing_scraped["html"],
            listing_url=listing_scraped.get("final_url") or listing_url,
        )

        if upload_to_db:
            with sqlite3.connect(self.db_path) as conn:
                upsert_df(conn, results)
        return results
