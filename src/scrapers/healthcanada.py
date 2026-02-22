"""Scraper for Health Canada recalls and safety alerts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List
from typing import Optional
import requests

from src.models import DrugAlert
from src.database import upsert_df

from .base import BaseScraper
from .utils import parse_date
import sqlite3
from scrapers.config import HEALTH_CANADA


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
        """
        Fetch the full JSON feed.
        The response is expected to be a JSON array (list of records) OR
        an object containing a list under a known key (we handle both defensively).
        """
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
        """
        Filter to health-product/drug-type recalls.
        The open dataset includes many categories (food, vehicles, consumer products).
        We keep this flexible because field names can vary.
        """
        category = self._norm(rec.get("Category") or rec.get("category"))
        subcategory = self._norm(rec.get("Subcategory") or rec.get("subcategory"))
        communication_type = self._norm(
            rec.get("Type of communication")
            or rec.get("type_of_communication")
            or rec.get("Type")
            or rec.get("type")
        )
        source = self._norm(rec.get("Source of recall") or rec.get("source_of_recall") or rec.get("Source"))

        # Common signals for Health Canada health/drug-related records
        healthish = (
            "health" in category
            or "health product" in category
            or "drug" in communication_type
            or "medical device" in communication_type
            or "natural health" in subcategory
            or "pharmaceutical" in subcategory
            or source == "health canada"
        )

        return healthish

    def _extract_dates(self, rec: dict) -> tuple[Optional[str], Optional[datetime]]:
        """
        Returns:
          - publish_date_iso (str|None)
          - publish_dt (datetime|None) for filtering
        """
        # Try likely keys from the feed
        date_str = (
            rec.get("Starting date")
            or rec.get("starting_date")
            or rec.get("Recall date")
            or rec.get("recall_date")
            or rec.get("Posting date")
            or rec.get("posting_date")
            or rec.get("Last updated")
            or rec.get("last_updated")
            or rec.get("Date")
            or rec.get("date")
        )
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
            title = (rec.get("Title") or rec.get("title") or "").strip()
            product = (rec.get("Product") or rec.get("product") or "").strip()
            raw_name = title or product
            if not raw_name:
                continue

            # Mirror your FDA approach: pick a query token and map via NCI
            query = raw_name.split(" ")[0]
            drug_name = self.get_nci_name(query)
            if not drug_name:
                continue

            # URL fields in dataset commonly look like a full detail-page URL
            source_url = (
                rec.get("URL")
                or rec.get("Url")
                or rec.get("url")
                or rec.get("Link")
                or rec.get("link")
                or rec.get("Recall URL")
                or rec.get("recall_url")
            )

            # Manufacturer / distributor fields vary; keep best-effort
            manufacturer = (
                rec.get("Manufacturer")
                or rec.get("manufacturer")
                or rec.get("Company")
                or rec.get("company")
                or rec.get("Companies")
                or rec.get("companies")
            )
            distributor = rec.get("Distributor") or rec.get("distributor")

            reason = (
                rec.get("Reason")
                or rec.get("reason")
                or rec.get("Issue")
                or rec.get("issue")
                or rec.get("Issue category")
                or rec.get("issue_category")
            )

            # Build more_info from a few common fields (best-effort)
            more_info_parts = []
            for k in (
                "Summary",
                "summary",
                "Details",
                "details",
                "What you should do",
                "what_you_should_do",
                "Affected products",
                "affected_products",
                "Lot or serial number",
                "lot_or_serial_number",
                "DIN, NPN, DIN-HIM",
                "din_npn_din_him",
            ):
                v = rec.get(k)
                if isinstance(v, str) and v.strip():
                    more_info_parts.append(v.strip())
            more_info = " ".join(more_info_parts) if more_info_parts else None

            # Use something stable as an id seed (identification number if present)
            ident = (
                rec.get("Identification number")
                or rec.get("identification_number")
                or rec.get("ID")
                or rec.get("id")
                or (source_url or raw_name)
            )

            record_id = self.make_record_id(self.source_id, drug_name, str(ident))

            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_org=self.source_org,
                    source_country=self.cfg.get("source_country", "Canada"),
                    source_url=source_url,
                    publish_date=publish_date_iso,
                    manufacturer=manufacturer,
                    distributor=distributor,
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