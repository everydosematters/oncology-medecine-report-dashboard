"""Scraper for NAFDAC Nigeria."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from collections import defaultdict

from bs4 import BeautifulSoup, Tag
import re

from src.models import DrugAlert

from .base import BaseScraper
from .utils import (
    absolutize,
    clean_text,
    extract_title,
    extract_brand_name_and_generic_name_from_title,
    normalize_key,
    table_to_grid,
    get_first_name,
    select_one_text,
    parse_date,
    extract_country_from_title,
)


class NafDacScraper(BaseScraper):
    """Scraper for NAFDAC Nigeria recall and alert listings."""

    def __init__(self, config: dict, start_date: datetime = None) -> None:
        """Initialize scraper with configuration and optional start date filter."""

        self.cfg = config
        super().__init__(
            self.cfg["base_url"],
            args=self.cfg.get("request") or {},
            start_date=start_date,
        )
        self.source_id = self.cfg["source_id"]
        self.source_org = self.cfg["source_org"]

    def _get_nci_name(self, text: str) -> bool:
        """Return True if the given text likely refers to an oncology product."""

        filters = self.cfg.get("filters") or {}
        keywords = filters.get("oncology_keywords") or ["oncology", "oncology"]
        hay = (text or "").lower()
        return any(k.lower() in hay for k in keywords)
        # FIXME do a more specific filter some drugs cause oncology and are being trapped

    def _extract_product_name_from_text(self, tag: BeautifulSoup) -> str | None:
        """Given a body text extract the product name."""

        pattern = r"^(.+?)\s+is\s+(?:an\s|a\s|used\s)"
        # FIXME is used should be included
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
            key = normalize_key(raw_label, return_none=True)
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

        rows = table_to_grid(table)
        if not rows:
            return result

        ncols = len(rows[0])

        # CASE A: matrix table (>= 3 columns)
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

        # CASE B: 2-column key/value table
        if ncols == 2:
            for label, value in rows:
                key = normalize_key(label)
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
        """
        Extracts the product specification like batch number and name
        """
        parsed_table = defaultdict(None)
        for table in soup[-1].find_all("table"):
            table_specs = self._parse_nafdac_table(table)
            if table_specs:
                parsed_table.update(table_specs)
        return parsed_table

    def _parse_listing_page(
        self, soup: BeautifulSoup, listing_url: str
    ) -> List[DrugAlert]:
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

        rows = soup.select(item_sel) if item_sel else []

        results: List[DrugAlert] = []

        for row in rows:
            # First check that the alert is a drug, if not move on
            category = (
                clean_text(row.select_one(fields["category"]).get_text(" ", strip=True))
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
                    clean_text(d_el.get_text(" ", strip=True)) if d_el else None
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

            query = get_first_name(product_name)
            drug_name = self.get_nci_name(query)

            if not drug_name:
                continue

            publish_date = parse_date(publish_date)
            if self.start_date and publish_date:
                # FIXME this is so ugly but yeah
                if self.start_date > publish_date:
                    break

            manufacturer = clean_text(
                row.select_one(fields["company"]).get_text(" ", strip=True)
            )

            record_id = self.make_record_id(self.source_id, drug_name, publish_date)

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
                    publish_date=publish_date,
                    manufacturer=manufacturer,
                    reason=parsed.get("title"),
                    product_name=product_name,
                    scraped_at=datetime.now(timezone.utc),
                    more_info=more_info,
                )
            )
        return results

    def _parse_detail_page(self, soup: BeautifulSoup) -> Dict[str, Any]:

        title = select_one_text(soup, "h1")

        title = extract_title(title)

        source_country = extract_country_from_title(title)

        brand_name, generic_name = extract_brand_name_and_generic_name_from_title(title)

        if soup.find("table"):
            product_specs = self._extract_product_specs(soup)
        else:

            product_specs = self._extract_product_specs_from_text(soup)

        return {
            "title": title,
            "brand_name": brand_name,
            "generic_name": generic_name,
            "source_country": source_country,
            **product_specs,
        }

    def standardize(self) -> List[DrugAlert]:
        listing_url = self.cfg[
            "base_url"
        ]  # Base is sufficient gives all the listings in this case

        listing_scraped = self.scrape(listing_url)
        records = self._parse_listing_page(
            soup=listing_scraped["html"],
            listing_url=listing_scraped.get("final_url") or listing_url,
        )

        return records
