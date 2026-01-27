"""Scraper for NAFDAC Nigeria."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from src.models import DrugAlert

from .base import BaseScraper
from .utils import (
    load_source_cfg,
    clean_text,
    select_one_text,
    absolutize,
    extract_by_regex,
    parse_nafdac_date
)


class NafDacScraper(BaseScraper):
    """
    Site-specific scraper class for:
      https://nafdac.gov.ng/category/recalls-and-alerts/

    - Inherits BaseScraper (shared fetch + sqlite upsert)
    - Reads selectors/defaults from sources.json (NAFDAC_NG key)
    - Implements standardize() to output DrugAlert rows
    """

    def __init__(self, sources_path: str = "config/sources.json", start_date: Optional[datetime] = None) -> None:
        self.cfg = load_source_cfg(sources_path, "NAFDAC_NG")
        base_url = self.cfg["base_url"]
        req_args = self.cfg.get("request") or {}
        super().__init__(base_url, args=req_args)

        self.source_id = self.cfg["source_id"]
        self.source_country = self.cfg.get("source_country")
        self.source_org = self.cfg.get("source_org")
        self.start_date = start_date

    def _is_oncology(self, body_text: str) -> bool:
        filters = self.cfg.get("filters") or {}
        if not filters.get("require_oncology", False):
            return True
        keywords = filters.get("oncology_keywords") or ["oncology", "cancer"]
        hay = (body_text or "").lower()
        return any(k.lower() in hay for k in keywords) #FIXME use regular expression it is faster

    def _listing_urls(self) -> List[str]:
        listing_cfg = self.cfg.get("listing") or {}
        pag = listing_cfg.get("pagination") or {}
        base = self.cfg["base_url"]

        if not pag:
            return [base]

        if pag.get("type") != "path":
            return [base]

        pattern = pag["pattern"]  # e.g. "page/{page}/"
        start = int(pag.get("start", 1))
        max_pages = int(pag.get("max_pages", 1))

        urls: List[str] = []
        for p in range(start, start + max_pages):
            suffix = pattern.format(page=p)
            if base.endswith("/"):
                urls.append(base + suffix)
            else:
                urls.append(base + "/" + suffix)
        return urls

    def _parse_listing_page(self, html: str, listing_url: str):
        listing_cfg = self.cfg.get("listing") or {}
        item_sel = listing_cfg.get("item_selector")  # "table tbody tr"
        link_sel = listing_cfg.get("link_selector")  # "td:nth-child(2) a.ninja_table_permalink"
        date_sel = listing_cfg.get("date_selector")  # "td:nth-child(1)"
        fields_sel = listing_cfg.get("fields") or {}

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select(item_sel) if item_sel else []

        out = []
        for row in rows:
            link_el = row.select_one(link_sel) if link_sel else None
            if not link_el or not link_el.get("href"):
                continue

            detail_url = absolutize(listing_url, link_el["href"])
            title = clean_text(link_el.get_text(" ", strip=True))

            publish_date = None
            if date_sel:
                d_el = row.select_one(date_sel)
                if d_el:
                    publish_date = clean_text(d_el.get_text(" ", strip=True))

            # Extract extra columns from listing row
            listing_alert_type = None
            if fields_sel.get("alert_type"):
                a_el = row.select_one(fields_sel["alert_type"])
                listing_alert_type = clean_text(a_el.get_text(" ", strip=True)) if a_el else None

            listing_category = None
            if fields_sel.get("category"):
                c_el = row.select_one(fields_sel["category"])
                listing_category = clean_text(c_el.get_text(" ", strip=True)) if c_el else None

            listing_company = None
            if fields_sel.get("company"):
                co_el = row.select_one(fields_sel["company"])
                listing_company = clean_text(co_el.get_text(" ", strip=True)) if co_el else None

            out.append(
                {
                    "detail_url": detail_url,
                    "title": title,
                    "publish_date": publish_date,
                    "alert_type": listing_alert_type,
                    "category": listing_category,
                    "manufacturer_stated": listing_company,
                }
            )

        # de-dupe by detail_url preserving order
        seen = set()
        deduped = []
        for r in out:
            if r["detail_url"] in seen:
                continue
            seen.add(r["detail_url"])
            deduped.append(r)

        return deduped



    def _parse_detail_page(self, html: str) -> Dict[str, Optional[str]]:
        dcfg = self.cfg.get("detail_page") or {}
        soup = BeautifulSoup(html, "html.parser")

        title = select_one_text(soup, dcfg.get("title_selector", ""))
        body = select_one_text(soup, dcfg.get("body_selector", ""))
        publish_date = select_one_text(soup, dcfg.get("publish_date_selector", ""))

        extracted: Dict[str, Optional[str]] = {}
        for field_name, rule in (dcfg.get("fields") or {}).items():
            if (rule or {}).get("strategy") == "regex":
                extracted[field_name] = extract_by_regex(body or "", rule.get("pattern", ""))

        return {"title": title, "body_text": body, "publish_date": publish_date, **extracted}

    def standardize(self) -> List[DrugAlert]:
        defaults = self.cfg.get("defaults") or {}
        records = []

        for listing_url in self._listing_urls():
            listing_scraped = self.scrape(listing_url)

            rows = self._parse_listing_page(
                html=listing_scraped["html"],
                listing_url=listing_scraped.get("final_url") or listing_url,
            )
            i = 1
            for row in rows:
                # Scrape detail page (your “scrape again” requirement)
                if row['category'] not in ['Drugs', 'Drug', 'Drugs & Biological']:
                    continue
                detail_scraped = self.scrape(row["detail_url"])
                parsed = self._parse_detail_page(detail_scraped["html"])

                # Filter based on the DETAIL page content (not listing)
                body_text = parsed.get("body_text") or detail_scraped.get("text") or ""
                if not self._is_oncology(body_text):
                    continue

                title = parsed.get("title") or row.get("title")
                raw_date = parsed.get("publish_date") or row.get("publish_date")
                publish_date = parse_nafdac_date(raw_date)
                if self.start_date and self.start_date > publish_date:
                    print(publish_date)
                    # break we reached the amount of record we need
                    break


                manufacturer_stated = parsed.get("manufacturer_stated") or row.get("manufacturer_stated")
                reason = parsed.get("reason")

                # Prefer listing alert type if present; else default
                alert_type = row.get("alert_type") or defaults.get("alert_type")

                record_id = self.make_record_id(
                    self.source_id,
                    detail_scraped.get("final_url") or row["detail_url"],
                    title or "",
                    publish_date or "",
                    manufacturer_stated or "",
                )

                print("="*10)
                print(record_id)
                print("="*10)

                records.append(
                    DrugAlert(
                        record_id=record_id,
                        source_id=self.source_id,
                        source_country=self.source_country,
                        source_org=self.source_org,
                        source_url=detail_scraped.get("final_url") or row["detail_url"],

                        # REQUIRED in your model (even if Optional[str])
                        product_name=title,

                        title=title,
                        publish_date=publish_date,
                        manufacturer_stated=manufacturer_stated,
                        manufactured_for=None,
                        therapeutic_category=defaults.get("therapeutic_category"),
                        reason=reason,
                        alert_type=alert_type,
                        notes=row.get("category"),   # optional: store category ("Food"/"Drugs")
                        body_text=body_text,

                        scraped_at=datetime.now(timezone.utc),
                    )
                )
                i +=1

        return records
