"""Scraper for US FDA recalls and safety alerts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List
import re
import requests

from src.models import DrugAlert

from .base import BaseScraper
from .utils import (
    parse_date
)

class FDAUSAScraper(BaseScraper):
    """Scraper for the US FDA recalls/alerts DataTables listing."""

    def __init__(self, config: dict, start_date: datetime = None) -> None:
        """Initialize scraper with configuration and optional start date filter."""
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

    def _get_product_name(self, name: str) -> str:
       return name.split(",")[0]

    def _openfda_date_range(self, start_dt: datetime, end_dt: datetime) -> str:
        return f"[{start_dt:%Y%m%d} TO {end_dt:%Y%m%d}]"

    def _get_manufacturer(self, text: str):
        manufacturer_match = re.search(r"Manufactured\s+by:\s*(.+?)(?=,\s*Distributed\s+by:|,\s*NDC|\.$)", text, re.IGNORECASE)
        return manufacturer_match.group(1).strip() if manufacturer_match else None

    def _get_distributor(self, text: str):
        distributor_match = re.search(r"Distributed\s+by:\s*(.+?)(?=,\s*NDC|\.$)", text, re.IGNORECASE)
        return distributor_match.group(1).strip() if distributor_match else None

    
    def standardize(self) -> List[DrugAlert]:
        """Fetch AJAX listing, scrape each detail page, filter oncology, return DrugAlerts."""
        
        results = []
        # params = {
        #     "search": f"report_date:{self._openfda_date_range(self.start_date, datetime.now())} AND product_type:Drugs",
        #     "limit": 1000
        # }
        params = {
            "search": f"report_date:{self._openfda_date_range(self.start_date, datetime.now())} AND product_type:Drugs",
            "limit": 1000
        }
        resp = requests.get(self.cfg["api_endpoint"], params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # TODO handle pagination if results are many than the limit
        for record in data["results"]:
            description  = record["product_description"]
            product_name = description.split(",")[0]
            query = product_name.split(" ")[0]
            manufacturer = self._get_manufacturer(description)
            distributor = self._get_distributor(description)
        
            if not self.is_oncology(query):
                continue
            
            record_id = self.make_record_id(description)
            
            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_org=self.source_org,
                    source_country=record["country"],
                    source_url=self.cfg["base_url"],
                    publish_date=parse_date(record["report_date"]),
                    manufacturer=manufacturer,
                    distributor=distributor,
                    reason=record["reason_for_recall"],
                    more_info=description + record["code_info"],
                    product_name=product_name ,
                    scraped_at=datetime.now(timezone.utc),
                )
            )

        return results


