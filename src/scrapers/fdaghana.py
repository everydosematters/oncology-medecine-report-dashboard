"""Ghana FDA scraper (wpDataTables AJAX-backed recall table)."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from src.database import upsert_df
from src.models import DrugAlert

from .base import BaseScraper
from .config import FDA_GH


def _safe_json_loads(text: str) -> Dict[str, Any]:
    return json.loads(text.lstrip("\ufeff").strip())


def _detect_column_count_from_html(html: str) -> int:
    """
    Use the visible table header count as the DataTables column count.
    Falls back to 13 (your observed column count).
    """
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        t = " ".join(table.get_text(" ", strip=True).lower().split())
        if "date recall was issued" in t and "product name" in t:
            ths = table.find_all("th")
            if ths:
                return len(ths)
    return 13


def _find_nonce_candidates(html: str) -> List[str]:
    """
    Extract nonce-like tokens from listing HTML (wpDataTables can embed multiple).
    Your successful run found 3 candidates and the working one was used as wdtNonce.
    """
    candidates = set()

    # Common direct patterns
    direct_patterns = [
        r'wdtNonce"\s*:\s*"([^"]+)"',
        r'wdt_nonce"\s*:\s*"([^"]+)"',
        r'"security"\s*:\s*"([^"]+)"',
        r'"nonce"\s*:\s*"([^"]+)"',
        r'"_wpnonce"\s*:\s*"([^"]+)"',
    ]
    for pat in direct_patterns:
        for m in re.finditer(pat, html):
            candidates.add(m.group(1))

    # Contextual extraction near relevant words
    for m in re.finditer(
        r"(?:wdt|datatable|admin-ajax|ajax|nonce|security).{0,500}",
        html,
        flags=re.I | re.S,
    ):
        chunk = m.group(0)
        for tok in re.findall(r"\b[a-f0-9]{8,32}\b", chunk, flags=re.I):
            candidates.add(tok)

    out = [c for c in candidates if 8 <= len(c) <= 32]
    out.sort(key=lambda x: (-len(x), x))
    return out


def _make_dt_payload(
    table_id: str,
    draw: int,
    start: int,
    length: int,
    ncols: int,
    wdt_nonce: str,
) -> Dict[str, str]:
    """
    DataTables-style payload similar to what the browser sends.
    Note: Ghana's working combo used nonce_key = wdtNonce.
    """
    payload: Dict[str, str] = {
        "action": "get_wdtable",
        "table_id": table_id,
        "draw": str(draw),
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "0",
        "order[0][dir]": "desc",
        "wdtNonce": wdt_nonce,
    }

    for i in range(ncols):
        payload[f"columns[{i}][data]"] = str(i)
        payload[f"columns[{i}][name]"] = ""
        payload[f"columns[{i}][searchable]"] = "true"
        payload[f"columns[{i}][orderable]"] = "true"
        payload[f"columns[{i}][search][value]"] = ""
        payload[f"columns[{i}][search][regex]"] = "false"

    return payload


def _looks_like_json_payload(text: str) -> bool:
    t = text.lstrip("\ufeff").strip()
    return t.startswith("{") and t.endswith("}")


def _extract_link_and_text(
    cell_html: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    if not cell_html or not isinstance(cell_html, str):
        return None, None
    soup = BeautifulSoup(cell_html, "html.parser")
    a = soup.find("a", href=True)
    if not a:
        return None, soup.get_text(" ", strip=True) or None

    href = a["href"].strip()
    # Prefer the full value if present
    full_name = (a.get("data-content") or "").strip()
    text = soup.get_text(" ", strip=True) or None
    return href, (full_name or text)


def _is_pdf_url(url: Optional[str]) -> bool:
    return bool(url) and url.lower().split("?")[0].endswith(".pdf")


class GhanaWpDataTablesClient:
    def __init__(self, listing_url: str, ajax_url: str, table_id: str) -> None:
        self.listing_url = listing_url
        self.ajax_url = ajax_url
        self.table_id = table_id
        self.session = requests.Session()

        self._listing_html: Optional[str] = None
        self._ncols: Optional[int] = None
        self._wdt_nonce: Optional[str] = None

    def prime(self) -> None:
        r = self.session.get(
            self.listing_url,
            headers={"User-Agent": FDA_GH["headers"]["User-Agent"]},
            timeout=60,
        )
        r.raise_for_status()
        self._listing_html = r.text
        self._ncols = _detect_column_count_from_html(r.text)

    def discover_wdt_nonce(self) -> str:
        """
        Try each candidate as wdtNonce until we get a JSON response with data.
        This is exactly what made your script succeed.
        """
        if self._listing_html is None or self._ncols is None:
            self.prime()

        assert self._listing_html is not None
        assert self._ncols is not None

        candidates = _find_nonce_candidates(self._listing_html)
        if not candidates:
            raise ValueError(
                "No nonce candidates found in listing HTML; cannot call wpDataTables AJAX."
            )

        # Probe: request first page with each candidate
        for cand in candidates:
            payload = _make_dt_payload(
                table_id=self.table_id,
                draw=1,
                start=0,
                length=10,
                ncols=self._ncols,
                wdt_nonce=cand,
            )
            resp = self.session.post(
                self.ajax_url,
                params={"action": "get_wdtable", "table_id": self.table_id},
                data=payload,
                headers=FDA_GH["headers"],
                timeout=60,
            )

            txt = resp.text.lstrip("\ufeff").strip()
            if not txt or txt in ("0", "-1") or txt.startswith("<"):
                continue

            if _looks_like_json_payload(txt):
                obj = _safe_json_loads(txt)
                rows = obj.get("data") or obj.get("aaData") or []
                if rows:
                    self._wdt_nonce = cand
                    return cand

        raise ValueError("Tried nonce candidates but none produced table JSON data.")

    def fetch_all_rows(self, page_size: int = 200) -> List[List[Any]]:
        """
        Returns list of rows; each row is a list (indexed columns).
        """
        if self._listing_html is None or self._ncols is None:
            self.prime()
        if self._wdt_nonce is None:
            self.discover_wdt_nonce()

        assert self._ncols is not None
        assert self._wdt_nonce is not None

        all_rows: List[List[Any]] = []
        start = 0
        draw = 1

        while True:
            payload = _make_dt_payload(
                table_id=self.table_id,
                draw=draw,
                start=start,
                length=page_size,
                ncols=self._ncols,
                wdt_nonce=self._wdt_nonce,
            )
            resp = self.session.post(
                self.ajax_url,
                params={"action": "get_wdtable", "table_id": self.table_id},
                data=payload,
                headers=FDA_GH["headers"],
                timeout=60,
            )
            resp.raise_for_status()

            txt = resp.text.lstrip("\ufeff").strip()
            if not txt or txt in ("0", "-1") or txt.startswith("<"):
                break

            obj = _safe_json_loads(txt)
            rows = obj.get("data") or obj.get("aaData") or []
            if not rows:
                break

            # ensure list rows
            for r in rows:
                if isinstance(r, list):
                    all_rows.append(r)
                else:
                    # if it ever changes to dict, keep it (but your current output is list)
                    all_rows.append(r)  # type: ignore

            start += len(rows)
            draw += 1

            if len(rows) < page_size:
                break

        return all_rows


class FDAGhanaScraper(BaseScraper):
    """Ghana FDA recall scraper backed by wpDataTables AJAX (table_id=47)."""

    def __init__(self, start_date: datetime | None = None) -> None:
        if start_date is not None and start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)

        super().__init__(start_date=start_date)
        self.cfg = FDA_GH
        self.source_id = self.cfg["source_id"]
        self.source_org = self.cfg["source_org"]
        self.source_country = self.cfg["source_country"]

        # plain HTML fetches (detail pages)
        self._http = requests.Session()
        self._http.headers.update({"User-Agent": FDA_GH["headers"]["User-Agent"]})

    def _fetch_html(self, url: str) -> str:
        r = self._http.get(url, timeout=60)
        r.raise_for_status()
        return r.text

    def _parse_detail_reason(self, url: str) -> Optional[str]:
        """
        Best-effort: extract "Reason for Recall" from HTML detail pages.
        Many records are PDFs; those will skip this.
        """
        try:
            html = self._fetch_html(url)
        except Exception:
            return None

        soup = BeautifulSoup(html, "html.parser")

        # Find label "Reason for Recall:" then grab nearby text
        for tag in soup.find_all(
            ["strong", "b", "h1", "h2", "h3", "h4", "h5", "p", "div"]
        ):
            txt = tag.get_text(" ", strip=True).lower()
            if txt == "reason for recall:":
                # pull next meaningful text block
                nxt = tag.find_next()
                for _ in range(20):
                    if not nxt:
                        break
                    t = nxt.get_text(" ", strip=True)
                    if t and t.lower() not in {"reason for recall:", "announcement"}:
                        return t.strip()
                    nxt = nxt.find_next()

        # Fallback: pick the longest paragraph
        paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        paras = [p for p in paras if p and len(p) > 80]
        return max(paras, key=len).strip() if paras else None

    def _row_to_alert(self, row: List[Any]) -> Optional[DrugAlert]:
        """
        Convert one wpDataTables row (list) into DrugAlert.
        """
        # Defensive: we expect at least 13 columns
        if not isinstance(row, list) or len(row) < 13:
            return None

        # Index mapping (from your examples)
        row_id = str(row[0] or "").strip()
        recall_date_raw = str(row[5] or "").strip()
        product_cell = row[6]  # HTML anchor
        product_type = str(row[7]).strip() if row[7] is not None else None

        manufacturer = str(row[8]).strip() if row[8] is not None else None
        recalling_firm = str(row[9]).strip() if row[9] is not None else None
        batches = str(row[10]).strip() if row[10] is not None else None
        mfg_date_raw = str(row[11]).strip() if row[11] is not None else None
        exp_date_raw = str(row[12]).strip() if row[12] is not None else None

        # Filter: keep only Drug rows when product_type is present
        # (PDF rows often have None type; keep them if you want broader coverage)
        if product_type is not None and product_type.lower() != "drug":
            return None

        source_url, raw_product_name = _extract_link_and_text(
            product_cell if isinstance(product_cell, str) else None
        )
        if not raw_product_name:
            return None

        # Dates
        publish_dt = (
            datetime.strptime(recall_date_raw, "%d/%m/%Y").replace(tzinfo=timezone.utc)
            if recall_date_raw
            else None
        )

        if self.start_date and publish_dt and publish_dt < self.start_date:
            return None

        # Name normalization (use NCI if available; fallback to raw)
        query = raw_product_name.split(" ")[0]
        # print("===="*10)
        # print(raw_product_name)
        # print("===="*10)
        # print()
        nci_name = self.get_nci_name(query)
        if not nci_name:
            return None
        product_name = nci_name or raw_product_name

        # Reason: only attempt for HTML detail pages (not PDFs)
        reason = None
        if source_url and (not _is_pdf_url(source_url)):
            reason = self._parse_detail_reason(source_url)

        # more_info: pack useful structured fields even when reason is missing (PDFs)
        more_info_parts: List[str] = []

        created_at = str(row[2] or "").strip()
        updated_at = str(row[4] or "").strip()

        if created_at:
            more_info_parts.append(f"Row created: {created_at}")
        if updated_at:
            more_info_parts.append(f"Row updated: {updated_at}")
        if batches:
            more_info_parts.append(f"Batch(es): {batches}")
        if mfg_date_raw:
            more_info_parts.append(f"Manufacturing date: {mfg_date_raw}")
        if exp_date_raw:
            more_info_parts.append(f"Expiry date: {exp_date_raw}")
        if source_url and _is_pdf_url(source_url):
            more_info_parts.append("Notice type: PDF")
        more_info = " | ".join(more_info_parts) if more_info_parts else None

        # Stable record_id seed: prefer the href; else row_id
        # FIXME using rowid is not good it changes depending on what the rendering is
        record_id = self.make_record_id(self.source_id, product_name, row_id)

        return DrugAlert(
            record_id=record_id,
            source_id=self.source_id,
            source_org=self.source_org,
            source_country=self.source_country,
            source_url=source_url or self.cfg["listing_url"],
            publish_date=publish_dt.isoformat() if publish_dt else None,
            manufacturer=manufacturer,
            distributor=recalling_firm,
            reason=reason,
            more_info=more_info,
            product_name=product_name,
            scraped_at=datetime.now(timezone.utc).isoformat(),
        )

    def standardize(self, upload_to_db: bool = False) -> List[DrugAlert]:
        client = GhanaWpDataTablesClient(
            listing_url=self.cfg["listing_url"],
            ajax_url=self.cfg["ajax_url"],
            table_id=self.cfg["table_id"],
        )

        rows = client.fetch_all_rows(page_size=200)

        alerts: List[DrugAlert] = []
        for row in rows:
            if not isinstance(row, list):
                continue
            alert = self._row_to_alert(row)
            if alert:
                alerts.append(alert)

        if upload_to_db:
            with sqlite3.connect(self.db_path) as conn:
                upsert_df(conn, alerts)

        return alerts
