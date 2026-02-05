"""Scraper for US FDA."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple
import requests

from bs4 import BeautifulSoup

from src.models import DrugAlert
import pandas as pd
from io import BytesIO

from .base import BaseScraper
from .utils import (
    load_source_cfg,
    select_one_text,
    absolutize,
    extract_by_regex,
)


class FDAUSAScraper(BaseScraper):
    def __init__(self, config: dict, start_date: datetime = None) -> None:
        """Init the parent and subclass."""

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

    def _listing_urls(self) -> List[str]:
        pag = (self.cfg.get("listing") or {}).get("pagination") or {}
        base = self.cfg["base_url"]

        if not pag:
            return [base]

        if pag.get("type") != "query_param":
            return [base]

        param = pag["param"]
        start = int(pag.get("start", 0))
        max_pages = int(pag.get("max_pages", 1))

        urls: List[str] = []
        for p in range(start, start + max_pages):
            sep = "&" if "?" in base else "?"
            urls.append(f"{base}{sep}{param}={p}")
        return urls

    def _parse_listing_page(self, html: str, listing_url: str):
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table#datatable tbody tr")

        results = []

        for row in rows:
            cols = row.select("td")
            if len(cols) < 6:
                continue  # skip malformed rows

            # Date
            time_el = cols[0].select_one("time")
            date_txt = time_el["datetime"] if time_el else cols[0].get_text(strip=True)

            # Brand + link
            link_el = cols[1].select_one("a")
            if not link_el:
                continue

            detail_url = absolutize(listing_url, link_el["href"])
            brand_name = link_el.get_text(strip=True)

            # Product description
            description = cols[2].get_text(strip=True)

            # Product type
            product_type = cols[3].get_text(strip=True)

            # Recall reason
            recall_reason = cols[4].get_text(strip=True)

            # Company
            company = cols[5].get_text(strip=True)

            results.append(
                {
                    "detail_url": detail_url,
                    "publish_date": date_txt,
                    "brand_name": brand_name,
                    "description": description,
                    "product_type": product_type,
                    "reason": recall_reason,
                    "manufacturer_stated": company,
                }
            )

        return results

    def _parse_detail_page(self, html: str) -> Dict[str, Optional[str]]:
        dcfg = self.cfg.get("detail_page") or {}
        soup = BeautifulSoup(html, "html.parser")

        title = select_one_text(soup, dcfg.get("title_selector", ""))
        body = select_one_text(soup, dcfg.get("body_selector", ""))
        publish_date = select_one_text(soup, dcfg.get("publish_date_selector", ""))

        extracted: Dict[str, Optional[str]] = {}
        for field_name, rule in (dcfg.get("fields") or {}).items():
            if (rule or {}).get("strategy") == "regex":
                extracted[field_name] = extract_by_regex(
                    body or "", rule.get("pattern", "")
                )

        return {
            "title": title,
            "body_text": body,
            "publish_date": publish_date,
            **extracted,
        }

    def _fetch_table(self, base_url: str, params: dict, headers: dict):
        
        r = requests.get(
            base_url,
            params=params,
            headers=headers,
            timeout=30,
        )

        df = pd.read_excel(BytesIO(r.content))
        df['Date'] = pd.to_datetime(df['Date'], format="%m/%d/%Y")
        return df.to_dict('list')


    def standardize(self):
        listing_url = self.cfg[
            "base_url"
        ]  
        params = self.cfg["params"]
        headers = self.cfg["request"]["headers"]


        records = self._fetch_table(listing_url, params, headers)
        

        return records
