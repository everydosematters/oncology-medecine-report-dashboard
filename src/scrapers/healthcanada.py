"""Scraper for Health Canada recalls and safety alerts."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

import requests

from scrapers.config import HEALTH_CANADA
from src.database import upsert_df
from src.models import DrugAlert

from .base import BaseScraper
from .utils import parse_date


class HealthCanadaScraper(BaseScraper):
    """Scraper for Health Canada recalls/safety alerts open data JSON feed."""

    def __init__(self, start_date: datetime = None) -> None:
        if start_date is not None and start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)

        super().__init__(start_date=start_date)
        self.cfg = HEALTH_CANADA
        self.source_id = self.cfg["source_id"]
        self.source_org = self.cfg["source_org"]

    def _fetch_feed(self) -> list[dict]:
        """Fetch the full JSON feed."""

        resp = requests.get(self.cfg["api_endpoint"], timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list):
            return data

        raise ValueError("Unexpected Health Canada feed JSON structure.")

    @staticmethod
    def _norm(s: Optional[str]) -> str:
        return (s or "").strip().lower()

    def _is_health_product_recall(self, rec: dict) -> bool:
        """Check if category is for drugs."""

        category = self._norm(rec.get("Category") or rec.get("category"))

        healthish = (
            "health" in category
            or "drug" in category
            or "biologic" in category
            or "pharamaceutical" in category
            or "vaccine" in category
        )

        return healthish

    def _extract_dates(self, rec: dict) -> tuple[Optional[str], Optional[datetime]]:
        """Extract dates.  - publish_dt (datetime|None) for filtering"""

        # Try likely keys from the feed
        date_str = rec.get("Last updated")

        if not date_str:
            return None, None

        dt = parse_date(date_str)
        # Ensure tz-aware for comparisons
        if dt is not None and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt.isoformat() if dt else None), dt

    def standardize(self, upload_to_db: bool = False) -> List[DrugAlert]:
        """Fetch feed, filter, map to DrugAlert list."""

        results: list[DrugAlert] = []
        data = self._fetch_feed()

        for rec in data:
            if not self._is_health_product_recall(rec):
                continue

            publish_date_iso, publish_dt = self._extract_dates(rec)
            if self.start_date and publish_dt and publish_dt < self.start_date:
                continue

            # Title / product name clues
            title = (rec.get("Title") or "").strip()
            product = (rec.get("Product") or "").strip()

            raw_name = product or title
            if not raw_name:
                continue

            # Mirror your FDA approach: pick a query token and map via NCI
            query = raw_name.split(" ")[0]
            drug_name = self.get_nci_name(query)
            if not drug_name:
                continue

            # URL fields in dataset commonly look like a full detail-page URL
            source_url = rec.get("URL")

            reason = rec.get("Issue")

            # Build more_info from a few common fields (best-effort)
            more_info_parts = []

            for k in (
                "Issue",
                "Title",
            ):
                v = rec.get(k)
                if isinstance(v, str) and v.strip():
                    more_info_parts.append(v.strip())
            more_info = ". ".join(more_info_parts) if more_info_parts else None

            record_id = self.make_record_id(self.source_id, drug_name, rec.get("NID"))

            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_org=self.source_org,
                    source_country=self.cfg.get("source_country", "Canada"),
                    source_url=source_url,
                    publish_date=publish_date_iso,
                    manufacturer=None,
                    distributor=None,
                    reason=reason,
                    more_info=more_info,
                    product_name=drug_name,
                    scraped_at=datetime.now(timezone.utc).isoformat(),
                )
            )

        if upload_to_db:
            with sqlite3.connect(self.db_path) as conn:
                upsert_df(conn, results)

        return results
