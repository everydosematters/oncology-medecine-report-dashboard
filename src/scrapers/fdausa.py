"""Scraper for US FDA."""

from __future__ import annotations

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
    extract_by_regex,
    parse_date,
    select_one_text,
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

    def _parse_detail_page(
        self, html_or_soup: Any
    ) -> Tuple[Dict[str, Any], bool]:
        """Parse detail page. Returns (parsed_dict, is_oncology)."""
        soup = (
            html_or_soup
            if isinstance(html_or_soup, BeautifulSoup)
            else BeautifulSoup(html_or_soup or "", "html.parser")
        )

        dcfg = self.cfg.get("detail_page") or {}
        title = select_one_text(soup, dcfg.get("title_selector", ""))
        body = select_one_text(soup, dcfg.get("body_selector", ""))

        if not self._is_oncology(body or ""):
            return {}, False

        extracted: Dict[str, Optional[str]] = {}
        for field_name, rule in (dcfg.get("fields") or {}).items():
            if isinstance(rule, dict) and rule.get("strategy") == "regex":
                extracted[field_name] = extract_by_regex(
                    body or "", rule.get("pattern", "")
                )

        return {
            "title": title,
            "body_text": body,
            "brand_name": None,  # from listing
            **extracted,
        }, True

    def standardize(self) -> List[DrugAlert]:
        """Fetch AJAX listing, scrape each detail page, filter oncology, return DrugAlerts."""
        raw_rows = self._fetch_ajax_listing()
        parsed_rows = self._parse_listing_rows(raw_rows)

        results: List[DrugAlert] = []
        defaults = self.cfg.get("defaults") or {}

        for row in parsed_rows:
            detail_url = row["detail_url"]
            publish_date = row["publish_date"]

            if self.start_date and publish_date and publish_date < self.start_date:
                break

            detail_scraped = self.scrape(detail_url)
            parsed, is_oncology = self._parse_detail_page(
                detail_scraped["html"]
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
                        parse_date(parsed.get("expiry_date"))
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
