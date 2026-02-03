"""Scraper for NAFDAC Nigeria."""

from __future__ import annotations

from datetime import datetime, timezone
from tracemalloc import start
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from bs4 import BeautifulSoup,Tag
import re

from src.models import DrugAlert

from .base import BaseScraper
from .utils import (
    absolutize,
    clean_text,
    extract_title,
    extract_brand_name_and_generic_name_from_title,
    normalize_key,
    cell_text,
    load_source_cfg,
    select_one_text,
    select_all_text,
    parse_date,
    extract_country_from_title
)


class NafDacScraper(BaseScraper):
    def __init__(self, sources_path: str = "config/sources.json", start_date: datetime = None) -> None:
        self.cfg = load_source_cfg(sources_path, "NAFDAC_NG")
        super().__init__(self.cfg["base_url"], args=self.cfg.get("request") or {}, start_date=start_date)

        self.source_id = self.cfg["source_id"]
        self.source_country = self.cfg["source_country"]
        self.source_org = self.cfg["source_org"]

    def _is_oncology(self, text: str) -> bool:
        filters = self.cfg.get("filters") or {}
        if not filters.get("require_oncology", False):
            return True
        keywords = filters.get("oncology_keywords") or ["oncology", "cancer"]
        hay = (text or "").lower()
        return any(k.lower() in hay for k in keywords)

    def _extract_product_specs_from_text(self, soup: BeautifulSoup) -> dict[str, list[str]]:
        result = {}

        LABEL_MAP = {
            "product name": "product_name",
            "batch no": "batch_number",
            "expiry date": "expiry_date",
            "manufacturing date": "manufacturing_date",
            "stated manufacturer": "stated_manufacturer",
        }

        for strong in soup.find_all("strong"):
            label = strong.get_text(" ", strip=True).rstrip(":").lower()
            if label not in LABEL_MAP:
                continue

            value = strong.next_sibling
            if not value:
                continue

            if isinstance(value, str):
                value = value.strip()
            else:
                value = value.get_text(" ", strip=True)

            if value:
                key = LABEL_MAP[label]
                result.setdefault(key, []).append(value)

        return result

    def _parse_nafdac_table(self, table: Tag) -> dict[str, list[str]]:
        """
        Returns a normalized column-oriented dict, e.g.

        {
        "product_name": [...],
        "batch_number": [...],
        "expiry_date": [...],
        "stated_manufacturer": [...],
        "date_of_manufacture": [...]
        }

        Handles:
        - 3+ column matrix tables (with header row)
        - 2-column key/value tables
        - rowspan/colspan tables (like the Avastin example)
        """
        result: dict[str, list[str]] = {}

        # -------------------------------
        # Helper: expand table to a full grid (rowspan/colspan aware)
        # -------------------------------
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

        rows = table_to_grid(table)
        if not rows:
            return result

        ncols = len(rows[0])

        # -------------------------------
        # CASE A: matrix table (>= 3 columns)
        # First row treated as headers
        # -------------------------------
        if ncols >= 3:
            headers = [normalize_key(h) for h in rows[0]]

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

        # -------------------------------
        # CASE B: 2-column key/value table
        # -------------------------------
        if ncols == 2:
            for label, value in rows:
                key = normalize_key(label)
                val = (value or "").strip()
                if not key or not val:
                    continue
                result.setdefault(key, []).append(val)
            return result

        # -------------------------------
        # fallback
        # -------------------------------
        return result


    
    def _extract_product_specs(self, *soup: BeautifulSoup,
    ) -> dict:
        """
        Extracts the product specification like batch number and name
        """
        for table in soup[-1].find_all("table"):
            parsed_table = self._parse_nafdac_table(table)
            if parsed_table:
                return parsed_table
        return {}

    def _parse_listing_page(self, html: str, listing_url: str) -> List[DrugAlert]:
        """
        Reads table rows from tbody and extracts all necessary info
          - publish_date (col 1)
          - title + detail_url (col 2)
          - alert_type/category/company (optional, from config mapping)
        """
        listing_cfg = self.cfg.get("listing") or {}
        item_sel = listing_cfg.get("item_selector")
        link_sel = listing_cfg.get("link_selector")
        date_sel = listing_cfg.get("date_selector")
        fields = listing_cfg.get("fields") or {}

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select(item_sel) if item_sel else []

        results: List[DrugAlert] = []

        for row in rows:
            # First check that the alert is a drug, if not move on
            category = clean_text(row.select_one(fields["category"]).get_text(" ", strip=True)) or ""
            if not re.search(r"drug",category, re.IGNORECASE):
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
                publish_date = clean_text(d_el.get_text(" ", strip=True)) if d_el else None
            
            
            # standardize
            detail_scraped = self.scrape(detail_url)

            
            
            parsed, is_oncology = self._parse_detail_page(detail_scraped["html"])

            if not is_oncology:
                continue

            publish_date = parse_date(publish_date)

            if self.start_date and publish_date:
                # FIXME this is so ugly but yeah
                if self.start_date > publish_date:
                    break

            manufacturer = clean_text(row.select_one(fields["company"]).get_text(" ", strip=True))
            
            alert_type = clean_text(row.select_one(fields["alert_type"]).get_text(" ", strip=True)) if fields.get("alert_type") and row.select_one(fields["alert_type"]) else None

            record_id = self.make_record_id(
                self.source_id,
                detail_url,
                publish_date,#FIXME why use the title?
                parsed.get("title"),
                manufacturer,
            )
           
            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_org=self.source_org,
                    source_country=parsed.get("source_country") or "Nigeria",
                    source_url=detail_url,
                    publish_date=publish_date, 
                    manufacturer=manufacturer,
                    notes=parsed.get("title"),
                    alert_type=alert_type,
                    product_name=parsed.get("product_name") or parsed.get("brand_name") or parsed.get("generic_name") or None,
                    scraped_at=datetime.now(timezone.utc),
                    brand_name=parsed.get("brand_name"),
                    generic_name=parsed.get("generic_name"),
                    batch_number=parsed.get("batch_number"),
                    expiry_date=parse_date(parsed.get("expiry_date", [None])[0]),
                    date_of_manufacture=parse_date(parsed.get("date_of_manufacture", [None])[0])
                )
            )
            print("="*20)
            print(results)
            print("="*20)
        return results

    def _parse_detail_page(self, html: str) -> Tuple[Dict[str, Any], bool]:
        soup = BeautifulSoup(html, "html.parser")

        title = select_one_text(soup, "h1")
        
        title = extract_title(title)

        source_country = extract_country_from_title(title)

        brand_name, generic_name = extract_brand_name_and_generic_name_from_title(title)
        
        body = select_all_text(soup, "p")

        if not self._is_oncology(body):
            return {}, False

        if soup.find("table"):
            product_specs = self._extract_product_specs(soup)
        else:

            product_specs = self._extract_product_specs_from_text(soup)
    
        return {
            "title": title,
            "brand_name": brand_name,
            "generic_name": generic_name,
            "source_country": source_country,
            **product_specs
        }, True


    def standardize(self) -> List[DrugAlert]:
        listing_url = self.cfg["base_url"] # Base is sufficient gives all the listings in this case
        
        listing_scraped = self.scrape(listing_url)
        records = self._parse_listing_page(
            html=listing_scraped["html"],
            listing_url=listing_scraped.get("final_url") or listing_url,
        )

        return records
