"""Scraper for NAFDAC Nigeria."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from bs4 import BeautifulSoup
import re

from src.models import DrugAlert

from .base import BaseScraper
from .utils import (
    absolutize,
    clean_text,
    extract_by_regex,
    load_source_cfg,
    parse_nafdac_date,
    select_one_text,
    select_all_text
)


class NafDacScraper(BaseScraper):
    def __init__(self, sources_path: str = "config/sources.json") -> None:
        self.cfg = load_source_cfg(sources_path, "NAFDAC_NG")
        super().__init__(self.cfg["base_url"], args=self.cfg.get("request") or {})

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
    
    def _extract_product_specs(*soup: BeautifulSoup,
    ) -> dict:
        """
        Extracts the product specification like batch number and name
        """
        product_specs = defaultdict(list[str])
        
       
        for table in soup[-1].find_all("table"):
            # Gather header-ish text from <strong> or first row
            header_text = " ".join(
                s.get_text(" ", strip=True) for s in table.find_all("strong")
            ).lower()

            # Fallback: sometimes headers aren't in <strong>
            if not header_text:
                first_row = table.find("tr")
                if first_row:
                    header_text = first_row.get_text(" ", strip=True).lower()

            if "product name" not in header_text:
                continue

            # Parse rows
            rows = table.find_all("tr")

            for tr in rows[1:]:  # skip header row
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue
                product = tds[0].get_text(" ", strip=True)
                num = tds[1].get_text(" ", strip=True) # 
                expiry_date = tds[2].get_text(" ", strip=True)

                
                if num:
                    product_specs['batch_num'].append(num)
                if expiry_date:
                    product_specs['expiry_date'].append(expiry_date)
                if product:
                    product_specs['product_names'].append(product)
                    return product_specs
        return product_specs


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

            publish_date = parse_nafdac_date(publish_date)

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
                    source_country=self.source_country,
                    source_org=self.source_org,
                    source_url=detail_scraped.get("final_url") or row["detail_url"],
                    title=title,
                    publish_date=publish_date, #FIXME make this a datetime
                    manufacturer_stated=manufacturer,
                    manufactured_for=None,
                    reason=None,
                    therapeutic_category=None,
                    alert_type=alert_type,
                    notes=clean_text(row.select_one(fields["category"]).get_text(" ", strip=True)),
                    scraped_at=datetime.now(timezone.utc),
                    product_name=parsed["product_names"],
                    batch_number=parsed["batch_num"],
                    expiry_date=parsed["expiry_date"] #FIXME make this a datetime
                )
            )
        return results

    def _parse_detail_page(self, html: str) -> Tuple(Dict[str, Any], bool):
        dcfg = self.cfg.get("detail_page") or {}
        soup = BeautifulSoup(html, "html.parser")

        title = select_one_text(soup, "h1")
        # body = select_one_text(soup, "div.elementor-widget-container")
        body = select_all_text(soup, "p")

        if not self._is_oncology(body):
            return {}, False
        raw_publish = select_one_text(soup, dcfg.get("publish_date_selector", ""))

        extracted: Dict[str, Optional[str]] = {}
        for field_name, rule in (dcfg.get("fields") or {}).items():
            if (rule or {}).get("strategy") == "regex":
                extracted[field_name] = extract_by_regex(body or "", rule.get("pattern", ""))
        
        product_specs = self._extract_product_specs(soup)
        return {
            "title": title,
            "body_text": body,
            "publish_date": parse_nafdac_date(raw_publish),
            **extracted,
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
