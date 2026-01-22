"""Scraper for US FDA."""

# scrapers/fda.py
from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseScraper
from src.models import DrugAlert

from .utils import (
    _load_source_cfg,
    _clean_text,
    _select_one_text,
    _absolutize,
    _extract_by_regex,
)


class FDAUSAScraper(BaseScraper):
    """
    Site-specific scraper class for:
      https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts

    - Inherits BaseScraper (shared fetch + sqlite upsert)
    - Reads selectors/defaults from sources.json (FDA_US key)
    - Implements standardize() to output DrugAlert rows
    """

    def __init__(self, sources_path: str = "config/sources.json") -> None:
        self.cfg = _load_source_cfg(sources_path, "FDA_US")
        base_url = self.cfg["base_url"]
        req_args = self.cfg.get("request") or {}
        super().__init__(base_url, args=req_args)

        self.source_id = self.cfg["source_id"]
        self.source_country = self.cfg.get("source_country")
        self.source_org = self.cfg.get("source_org")

    def _is_oncology(self, body_text: str) -> bool:
        filters = self.cfg.get("filters") or {}
        if not filters.get("require_oncology", False):
            return True
        keywords = filters.get("oncology_keywords") or ["oncology", "cancer"]
        hay = (body_text or "").lower()
        return any(k.lower() in hay for k in keywords)

    def _listing_urls(self) -> List[str]:
        listing_cfg = self.cfg.get("listing") or {}
        pag = listing_cfg.get("pagination") or {}
        base = self.cfg["base_url"]

        if not pag:
            return [base]

        ptype = pag.get("type")
        start = int(pag.get("start", 0))
        max_pages = int(pag.get("max_pages", 1))

        urls: List[str] = []
        if ptype == "query_param":
            param = pag["param"]
            for p in range(start, start + max_pages):
                sep = "&" if "?" in base else "?"
                urls.append(f"{base}{sep}{param}={p}")
        else:
            urls.append(base)
        return urls

    def _parse_listing_page(
        self, html: str, listing_url: str
    ) -> List[Tuple[str, Optional[str]]]:
        listing_cfg = self.cfg.get("listing") or {}
        item_sel = listing_cfg.get("item_selector")
        link_sel = listing_cfg.get("link_selector")
        date_sel = listing_cfg.get("date_selector")

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select(item_sel) if item_sel else []

        out: List[Tuple[str, Optional[str]]] = []
        for item in items:
            link_el = item.select_one(link_sel) if link_sel else None
            if not link_el or not link_el.get("href"):
                continue
            detail_url = _absolutize(listing_url, link_el["href"])

            date_txt = None
            if date_sel:
                date_el = item.select_one(date_sel)
                if date_el:
                    date_txt = _clean_text(date_el.get_text(" ", strip=True))

            out.append((detail_url, date_txt))

        # de-dupe while preserving order
        seen = set()
        deduped: List[Tuple[str, Optional[str]]] = []
        for u, d in out:
            if u in seen:
                continue
            seen.add(u)
            deduped.append((u, d))
        return deduped

    def _parse_detail_page(self, html: str) -> Dict[str, Optional[str]]:
        dcfg = self.cfg.get("detail_page") or {}

        soup = BeautifulSoup(html, "html.parser")
        title = _select_one_text(soup, dcfg.get("title_selector", ""))
        body = _select_one_text(soup, dcfg.get("body_selector", ""))
        publish_date = _select_one_text(soup, dcfg.get("publish_date_selector", ""))

        extracted: Dict[str, Optional[str]] = {}
        fields_cfg = dcfg.get("fields") or {}
        for field_name, rule in fields_cfg.items():
            if (rule or {}).get("strategy") == "regex":
                extracted[field_name] = _extract_by_regex(
                    body or "", rule.get("pattern", "")
                )

        return {
            "title": title,
            "body_text": body,
            "publish_date": publish_date,
            **extracted,
        }

    def standardize(self) -> List[DrugAlert]:
        defaults = self.cfg.get("defaults") or {}
        records: List[DrugAlert] = []

        for listing_url in self._listing_urls():
            listing_scraped = self.scrape(listing_url)
            listing_items = self._parse_listing_page(
                html=listing_scraped["html"],
                listing_url=listing_scraped.get("final_url") or listing_url,
            )

            for detail_url, listing_date in listing_items:
                detail_scraped = self.scrape(detail_url)
                parsed = self._parse_detail_page(detail_scraped["html"])

                body_text = parsed.get("body_text") or detail_scraped.get("text") or ""
                if not self._is_oncology(body_text):
                    continue

                title = parsed.get("title")
                publish_date = parsed.get("publish_date") or listing_date

                manufacturer_stated = parsed.get("manufacturer_stated")
                reason = parsed.get("reason")

                record_id = self.make_record_id(
                    self.source_id,
                    detail_scraped.get("final_url") or detail_url,
                    title or "",
                    publish_date or "",
                    manufacturer_stated or "",
                )

                records.append(
                    DrugAlert(
                        record_id=record_id,
                        source_id=self.source_id,
                        source_country=self.source_country,
                        source_org=self.source_org,
                        source_url=detail_scraped.get("final_url") or detail_url,
                        title=title,
                        publish_date=publish_date,
                        manufacturer_stated=manufacturer_stated,
                        manufactured_for=None,
                        therapeutic_category=defaults.get("therapeutic_category"),
                        reason=reason,
                        alert_type=defaults.get("alert_type"),
                        notes=None,
                        body_text=body_text,
                        scraped_at=datetime.now(timezone.utc).isoformat(),
                    )
                )

        return records
