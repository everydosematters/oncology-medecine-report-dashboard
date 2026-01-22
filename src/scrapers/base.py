"""Module for the Base Class for All Scrapers."""

from __future__ import annotations

import abc
import hashlib
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import requests
from bs4 import BeautifulSoup

from src.models import DrugAlert


class BaseScraper(abc.ABC):
    # Keywords: tweak freely for your needs
    ONCOLOGY_KEYWORDS = (
        "oncology",
        "cancer",
        "tumour",
        "tumor",
        "malignant",
        "carcinoma",
        "chemotherapy",
        "immunotherapy",
        "radiotherapy",
        "radiation therapy",
        "leukemia",
        "lymphoma",
        "myeloma",
        "metastatic",
    )

    def __init__(
        self,
        url: str,
        *,
        args: Optional[Dict[str, Any]] = None,
        source_country: Optional[str] = None,
        source_org: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        self.url = url
        self.args = args or {}
        self.source_country = source_country
        self.source_org = source_org
        self.timeout = timeout

        # Safety defaults for requests
        self.args.setdefault("headers", {})
        self.args["headers"].setdefault(
            "User-Agent",
            "Mozilla/5.0 (compatible; EDM-Dashboard/1.0; +https://everydosematters.org)",
        )

    def scrape(self, url: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch a URL (or self.url if url is None) and return normalized payload:
          - final_url
          - status_code
          - html
          - text (cleaned)
          - retrieved_at
        """
        target = url or self.url
        resp = requests.get(target, timeout=self.timeout, **self.args)
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # Remove non-content noise
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()

        return {
            "final_url": resp.url,
            "status_code": resp.status_code,
            "html": html,
            "text": text,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }

    def is_oncology_alert(self, body_text: str) -> bool:
        """
        Returns True if the text contains any oncology/cancer keywords.
        (Base default; site scrapers can override with config keywords.)
        """
        if not body_text:
            return False
        hay = body_text.lower()
        return any(k in hay for k in self.ONCOLOGY_KEYWORDS)

    @abc.abstractmethod
    def standardize(self) -> List[DrugAlert]:
        """
        Subclasses implement:
          - scrape listing + detail pages
          - return 0..N DrugAlert objects matching schema
        """
        raise NotImplementedError

    @staticmethod
    def init_db(db_path: str) -> None:
        """
        Create the SQLite table if it doesn't exist.
        """
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS oncology_alerts (
                    record_id TEXT PRIMARY KEY,
                    source_id TEXT,
                    source_country TEXT,
                    source_org TEXT,
                    source_url TEXT NOT NULL,

                    title TEXT,
                    publish_date TEXT,
                    manufacturer_stated TEXT,
                    manufactured_for TEXT,
                    therapeutic_category TEXT,
                    reason TEXT,
                    alert_type TEXT,
                    notes TEXT,

                    body_text TEXT,
                    scraped_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_publish_date ON oncology_alerts(publish_date);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_id ON oncology_alerts(source_id);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_country ON oncology_alerts(source_country);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_org ON oncology_alerts(source_org);"
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _upsert_sqlite(conn: sqlite3.Connection, record: DrugAlert) -> None:
        """
        One-record upsert. Same for all scrapers.
        """
        d = record.model_dump()
        columns = list(d.keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_sql = ", ".join(columns)

        # On conflict, update everything except record_id
        update_cols = [c for c in columns if c != "record_id"]
        update_sql = ", ".join([f"{c}=excluded.{c}" for c in update_cols])

        sql = f"""
            INSERT INTO oncology_alerts ({col_sql})
            VALUES ({placeholders})
            ON CONFLICT(record_id) DO UPDATE SET
                {update_sql};
        """
        conn.execute(sql, [d[c] for c in columns])

    def upload_to_sqlite(self, db_path: str, records: Sequence[DrugAlert]) -> int:
        """
        Shared persistence method: upsert N records into SQLite.
        Returns number of records attempted.
        """
        if not records:
            return 0

        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            for r in records:
                self._upsert_sqlite(conn, r)
            conn.commit()
            return len(records)
        finally:
            conn.close()

    @staticmethod
    def make_record_id(*parts: str) -> str:
        """
        Stable ID for de-duping/upserts.
        Use source + url + date + title (+ manufacturer) etc.
        """
        raw = "||".join([p.strip() for p in parts if p is not None])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
