"""Scraper for US FDA recalls and safety alerts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List
import re
import requests

from src.models import DrugAlert

from .base import BaseScraper
from .utils import parse_date


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

    def _openfda_date_range(self, start_dt: datetime, end_dt: datetime) -> str:
        """Get the correct syntax of the date to use in the query."""

        return f"[{start_dt:%Y%m%d} TO {end_dt:%Y%m%d}]"

    def _get_manufacturer(self, text: str):
        """Parse the manufacturer from the descripition field."""

        pattern = r"(?:Manufactur(?:ed|e)?\s+by|Mfg|Mfd)\s*:?\s*([^,]+)"
        manufacturer_match = re.search(pattern, text, re.IGNORECASE)
        if manufacturer_match:
            manufacture_match = re.sub(
                r"^\s*by\s*:?\s*", "", manufacturer_match.group(1), flags=re.IGNORECASE
            ).strip()
            return manufacture_match
        return None

    def _get_distributor(self, text: str):
        """Parse the distributor from the descripition field."""

        pattern = (
            r"(?:"
            r"Distribut(?:ed|e)?\s+by"
            r"|Distrib\s+by"
            r"|Dist\s+by"
            r"|Distributor"
            r"|Distributed"
            r")\s*:?\s*([^,]+)"
        )
        distributor_match = re.search(pattern, text, re.IGNORECASE)
        return distributor_match.group(1).strip() if distributor_match else None

    def standardize(self) -> List[DrugAlert]:
        """Call FDA API endpoint to fetch recalls, return DrugAlerts."""

        results = []

        params = {
            "search": f"report_date:{self._openfda_date_range(self.start_date, datetime.now())} AND product_type:Drugs",
            "limit": 1000,
        }

        resp = requests.get(self.cfg["api_endpoint"], params=params, timeout=30)
        
        resp.raise_for_status()
        data = resp.json()

        # TODO handle pagination if results are many than the limit
        for record in data["results"]:
            url = self.cfg["api_endpoint"] + "?search=recall_number:" + record.get("recall_number", "")
            description = record["product_description"]
            product_name = description.split(",")[0]
            query = product_name.split(" ")[0]
            manufacturer = self._get_manufacturer(description)
            distributor = self._get_distributor(description)
            drug_name = self.get_nci_name(query)

            if not drug_name:
                continue

            record_id = self.make_record_id(self.source_id, drug_name, record["report_date"])

            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_org=self.source_org,
                    source_country=record["country"],
                    source_url=url,
                    publish_date=parse_date(record["report_date"]),
                    manufacturer=manufacturer,
                    distributor=distributor,
                    reason=record["reason_for_recall"],
                    more_info=description + " " + record["code_info"],
                    product_name=drug_name,
                    scraped_at=datetime.now(timezone.utc),
                )
            )

        return results
