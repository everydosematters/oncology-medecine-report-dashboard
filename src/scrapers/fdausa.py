"""Scraper for US FDA."""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from src.models import DrugAlert

from .base import BaseScraper
from .utils import (
    absolutize,
    clean_text,
    parse_date,
    select_one_text,
    select_all_text,
    table_to_grid,
)


class FDAUSAScraper(BaseScraper):
    def __init__(self, config: dict, start_date: datetime = None) -> None:
        """Init the parent and subclass."""
        if start_date is not None and start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        self.cfg = config
        super().__init__(
            self.cfg["base_url"],
            args=self.cfg.get("request") or {},
            start_date=start_date,
        )
        self.source_id = self.cfg["source_id"]
        self.source_org = self.cfg["source_org"]

    def _is_oncology(self, body_text: str) -> bool:
        filters = self.cfg.get("filters") or {}
        if not filters.get("require_oncology", False):
            return True
        keywords = filters.get("oncology_keywords") or ["oncology", "cancer"]
        hay = (body_text or "").lower()
        return any(k.lower() in hay for k in keywords)

    def _parse_anchor(self, html: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract (brand_text, absolute_url) from <a href="/safety/...">Brand</a>."""
        if not html or "<a" not in html:
            return None, None
        soup = BeautifulSoup(html, "html.parser")
        a = soup.find("a", href=True)
        if not a:
            return None, None
        brand = clean_text(a.get_text(" ", strip=True))
        detail_url = absolutize(self.cfg["base_url"], a["href"])
        return brand, detail_url

    def _parse_date_from_time_html(self, html: str) -> Optional[datetime]:
        """Extract datetime from <time datetime="2026-02-06T05:00:00Z">."""
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        time_el = soup.find("time", datetime=True)
        if not time_el:
            return None
        dt_str = time_el.get("datetime", "").strip()
        if not dt_str:
            return None
        # Handle Z suffix for UTC
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(dt_str)
        except ValueError:
            return parse_date(dt_str)

    def _fetch_ajax_listing(self) -> List[List[str]]:
        """Fetch listing pages via AJAX (script.py style). Returns list of row arrays."""
        listing_cfg = self.cfg.get("listing") or {}
        if listing_cfg.get("type") != "ajax":
            return []

        ajax_url = listing_cfg.get("ajax_url") or self.cfg.get("ajax_url", "")
        params = dict(listing_cfg.get("params") or {})
        pag = listing_cfg.get("pagination") or {}
        page_size = int(pag.get("page_size", 25))
        max_pages = int(pag.get("max_pages", 5))
        headers = (self.cfg.get("request") or {}).get("headers") or {}
        sleep_seconds = float(pag.get("sleep_seconds", 0.4))

        all_rows: List[List[str]] = []

        with requests.Session() as session:
            for page in range(max_pages):
                start = page * page_size
                req_params = dict(params)
                req_params["start"] = start
                req_params["length"] = page_size
                req_params["draw"] = page + 1

                r = session.get(
                    ajax_url,
                    params=req_params,
                    headers=headers,
                    timeout=30,
                )
                r.raise_for_status()
                payload = r.json()

                rows = payload.get("data", [])
                if not rows:
                    break

                all_rows.extend(rows)
                time.sleep(sleep_seconds)

        return all_rows

    def _parse_listing_rows(
        self, raw_rows: List[List[str]]
    ) -> List[Dict[str, Any]]:
        """Convert AJAX row arrays to structured dicts with detail_url, brand_name, etc."""
        cols = (self.cfg.get("listing") or {}).get("columns") or {}
        date_idx = cols.get("date_index", 0)
        brand_link_idx = cols.get("brand_link_index", 1)
        desc_idx = cols.get("description_index", 2)
        company_idx = cols.get("company_index", 5)

        results: List[Dict[str, Any]] = []

        for row in raw_rows:
            if len(row) <= brand_link_idx:
                continue

            brand_name, detail_url = self._parse_anchor(
                row[brand_link_idx] if isinstance(row[brand_link_idx], str) else ""
            )
            if not detail_url:
                continue

            publish_date = self._parse_date_from_time_html(
                row[date_idx] if isinstance(row[date_idx], str) else ""
            )

            description = ""
            if len(row) > desc_idx and row[desc_idx]:
                description = clean_text(str(row[desc_idx])) or ""

            manufacturer = ""
            if len(row) > company_idx and row[company_idx]:
                manufacturer = clean_text(str(row[company_idx])) or ""

            results.append(
                {
                    "detail_url": detail_url,
                    "brand_name": brand_name,
                    "description": description,
                    "manufacturer": manufacturer,
                    "publish_date": publish_date,
                }
            )

        return results

    def _parse_fda_usa_table(self, table) -> Dict[str, List[str]]:
        """
        Parse FDA USA tables with rowspan (product spans multiple lots) or flat rows.
        Returns normalized column-oriented dict like _parse_nafdac_table.
        Handles: product_name, batch_number, expiry_date, ndc.
        """
        result: Dict[str, List[str]] = {}
        rows = table_to_grid(table)
        if not rows:
            return result

        ncols = len(rows[0])

        # NDC pattern: ddddd-ddd-ddd or dddd-dddd-dd
        NDC_RE = re.compile(r"^\d{4,5}-\d{3,4}-\d{1,2}$")
        # Date patterns: MM/YYYY or MM/DD/YYYY
        DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$|^\d{1,2}/\d{4}$")
        # Lot/batch: short alphanumeric, typically < 20 chars
        def looks_like_product(val: str) -> bool:
            v = (val or "").strip()
            return len(v) > 25 or "\n" in v or "mg" in v.lower() or "capsule" in v.lower()

        def detect_col_type(col_values: List[str]) -> str:
            non_empty = [v for v in col_values if (v or "").strip()]
            if not non_empty:
                return "unknown"
            ndc_count = sum(1 for v in non_empty if NDC_RE.match((v or "").strip()))
            date_count = sum(1 for v in non_empty if DATE_RE.match((v or "").strip()))
            product_count = sum(1 for v in non_empty if looks_like_product(v))
            if product_count >= len(non_empty) / 2:
                return "product_name"
            if ndc_count >= len(non_empty) / 2:
                return "ndc"
            if date_count >= len(non_empty) / 2:
                return "expiry_date"
            return "batch_number"

        # Build column -> values, then detect types
        cols: List[List[str]] = [[] for _ in range(ncols)]
        for r in rows:
            for i, c in enumerate(r):
                if i < ncols and (c or "").strip():
                    cols[i].append((c or "").strip())

        for i, col_vals in enumerate(cols):
            if not col_vals:
                continue
            key = detect_col_type(col_vals)
            if key == "unknown":
                key = f"col_{i}"
            result.setdefault(key, []).extend(col_vals)

        return result

    def _parse_summary(self, soup: BeautifulSoup) -> dict:
        # Find the H2 with exact text "Summary"
        h2 = soup.find("h2", string=lambda s: s and s.strip().lower() == "summary")
        if not h2:
            return {}

        # The <dl> is inside the next inset-column in your snippet
        dl = h2.find_next("dl")
        if not dl:
            return {}

        summary = {}
        for dt in dl.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue

            key = dt.get_text(" ", strip=True).rstrip(":")
            val = dd.get_text(" ", strip=True)
            summary[key] = val

        return summary 

    def _parse_detail_page(
        self, soup: BeautifulSoup
    ) -> Tuple[Dict[str, Any], bool]:
        """Parse detail page. Returns (parsed_dict, is_oncology)."""

        dcfg = self.cfg.get("detail_page") or {}
        title = select_one_text(soup, dcfg.get("title_selector", ""))
        body = select_all_text(soup, dcfg.get("body_selector", ""))

        if not self._is_oncology(body or ""):
            return {}, False

        extracted = self._parse_summary(soup)

        
        result = {
            "title": title,
            "notes": extracted.get("Reason for Announcement") or title,
            "brand_name": extracted.get("Brand Name"),
            "company_publish_date": extracted.get("Company Announcement Date"),
            "publish_date": extracted.get("FDA Publish Date"),
            "manufacturer": extracted.get("Company Name"),
        }

        table_el = soup.select_one("table tbody") or soup.select_one("tbody")
        if table_el:
            specs = self._parse_fda_usa_table(table_el)
            result.update(specs)
        return result, True

    def standardize(self) -> List[DrugAlert]:
        """Fetch AJAX listing, scrape each detail page, filter oncology, return DrugAlerts."""
        raw_rows = self._fetch_ajax_listing()
        parsed_rows = self._parse_listing_rows(raw_rows)

        results: List[DrugAlert] = []
        defaults = self.cfg.get("defaults") or {}

        for row in parsed_rows:
            detail_url = row["detail_url"]
            publish_date = row["publish_date"]

            row_parsed = self.scrape(detail_url)
            print("=="*20)
            print(detail_url)
            print(publish_date)
            print("=="*20)

            if self.start_date and publish_date and publish_date < self.start_date:
                break

            parsed, is_oncology = self._parse_detail_page(
                row_parsed['html']
            )

            if not is_oncology:
                continue

            brand_name = row.get("brand_name") or parsed.get("brand_name")
            title = parsed.get("title") or row.get("description") or ""

            record_id = self.make_record_id(
                self.source_id,
                detail_url,
                publish_date,
                title,
                row.get("manufacturer"),
            )

            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_org=self.source_org,
                    source_country=self.cfg.get("source_country", "United States"),
                    source_url=detail_url,
                    publish_date=publish_date,
                    manufacturer=row.get("manufacturer") or None,
                    notes=title or row.get("description"),
                    alert_type=defaults.get("alert_type"),
                    product_name=brand_name or title or None,
                    brand_name=brand_name,
                    generic_name=parsed.get("generic_name"),
                    batch_number=parsed.get("batch_number"),
                    expiry_date=(
                        parse_date(parsed.get("expiry_date")[0])
                        if parsed.get("expiry_date")
                        else None
                    ),
                    date_of_manufacture=(
                        parse_date(parsed.get("date_of_manufacture"))
                        if parsed.get("date_of_manufacture")
                        else None
                    ),
                    scraped_at=datetime.now(timezone.utc),
                )
            )

        return results
