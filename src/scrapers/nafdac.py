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
    extract_by_regex,
    normalize_key,
    cell_text,
    load_source_cfg,
    select_one_text,
    select_all_text,
    parse_date
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
        """
        result: dict[str, list[str]] = {}

        # collect rows as list[list[str]]
        rows: list[list[str]] = []
        for tr in table.select("tr"):
            cells = tr.find_all("td")
            row = [cell_text(td) for td in cells]
            row = [x for x in row if x]
            if row:
                rows.append(row)

        if not rows:
            return result

        # -------------------------------
        # CASE A: 3-column matrix table
        # -------------------------------
        if len(rows[0]) == 3:
            headers = [normalize_key(h) for h in rows[0]]

            for h in headers:
                result.setdefault(h, [])

            for r in rows[1:]:
                if len(r) != 3:
                    continue
                for i, h in enumerate(headers):
                    result[h].append(r[i])

            return result

        # -------------------------------
        # CASE B: 2-column key/value table
        # -------------------------------
        if all(len(r) == 2 for r in rows):
            for label, value in rows:
                key = normalize_key(label)
                if not key:
                    continue
                result.setdefault(key, []).append(value)

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

            title = clean_text(link_el.get_text(" ", strip=True))
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
                publish_date,
                title,
                manufacturer,
            )
           
            results.append(
                DrugAlert(
                    record_id=record_id,
                    source_id=self.source_id,
                    source_country=self.source_country, #FIXME use parsing of the title to know where it is reported
                    source_org=self.source_org,
                    source_url=detail_scraped.get("final_url") or row["detail_url"],
                    title=title,
                    publish_date=publish_date, 
                    manufacturer=manufacturer,
                    alert_type=alert_type,
                    notes=clean_text(row.select_one(fields["category"]).get_text(" ", strip=True)),
                    scraped_at=datetime.now(timezone.utc),
                    brand_name=parsed["brand_name"],
                    generic_name=parsed["generic_name"],
                    batch_number=parsed["batch_number"],
                    expiry_date=parse_date(parsed["expiry_date"][0])
                )
            )
            print("="*20)
            print(results)
            print("="*20)
        return results

    def _parse_detail_page(self, html: str) -> Tuple(Dict[str, Any], bool):
        soup = BeautifulSoup(html, "html.parser")

        title = select_one_text(soup, "h1")
        # body = select_one_text(soup, "div.elementor-widget-container")
        title = re.search(r"[-–]\s*(.+)", title).group(1)
        
        m = re.search(r"([A-Z][A-Za-z0-9\-]*)\s*\(([^)]+)\)", title)
        
        brand_name = m.group(1).strip() if m else None
        generic_name = m.group(2).strip() if m else None
        
        
        body = select_all_text(soup, "p")

        if not self._is_oncology(body):
            return {}, False

        
        
        product_specs = self._extract_product_specs(soup)
        return {
            "title": title,
            "body_text": body,
            "brand_name": brand_name,
            "generic_name": generic_name,
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
