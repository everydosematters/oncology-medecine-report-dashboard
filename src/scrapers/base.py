"""Base scraper utilities shared across site-specific scrapers."""

from __future__ import annotations

import abc
import hashlib
import re
import sqlite3
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import requests
from bs4 import BeautifulSoup

from src.models import DrugAlert


class BaseScraper(abc.ABC):
    def __init__(
        self,
        url: str,
        *,
        args: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
        start_date: Optional[datetime] = None
    ) -> None:
        self.url = url
        self.args = args or {}
        self.timeout = timeout
        self.start_date = start_date

        # Safe default UA
        self.args.setdefault("headers", {})
        self.args["headers"].setdefault(
            "User-Agent",
            "Mozilla/5.0 (compatible; EDM-Dashboard/1.0; +https://everydosematters.org)",
        )

    def scrape(self, url: Optional[str] = None) -> Dict[str, Any]:
        target = url or self.url
        resp = requests.get(target, timeout=self.timeout, **self.args)
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

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

    @abc.abstractmethod
    def standardize(self) -> List[DrugAlert]:
        raise NotImplementedError

    # ---------------- SQLite ----------------

    @staticmethod
    def init_db(db_path: str) -> None:
        """
        Create a table that matches DrugAlert fields (including product_name).
        Store datetimes as ISO strings.
        """
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS oncology_alerts (
                    record_id TEXT PRIMARY KEY,

                    source_id TEXT NOT NULL,
                    source_country TEXT NOT NULL,
                    source_org TEXT NOT NULL,
                    source_url TEXT NOT NULL,

                    title TEXT,
                    product_name TEXT,
                    manufacturer_stated TEXT,
                    manufactured_for TEXT,
                    reason TEXT,
                    therapeutic_category TEXT,
                    alert_type TEXT,

                    publish_date TEXT,
                    notes TEXT,

                    scraped_at TEXT NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_publish_date ON oncology_alerts(publish_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_source_id ON oncology_alerts(source_id);")
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _to_sql_value(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        return v

    @classmethod
    def _upsert_sqlite(cls, conn: sqlite3.Connection, record: DrugAlert) -> None:
        d = record.model_dump()

        # ensure sqlite-friendly values
        d = {k: cls._to_sql_value(v) for k, v in d.items()}

        columns = list(d.keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_sql = ", ".join(columns)

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

    # ---------------- IDs ----------------

    @staticmethod
    def make_record_id(*parts: Any) -> str:
        """
        Stable ID builder. Accepts strings, datetimes, dates, numbers, etc.
        """
        normalized: List[str] = []
        for p in parts:
            if p is None:
                continue
            if isinstance(p, (datetime, date)):
                normalized.append(p.isoformat())
            else:
                normalized.append(str(p).strip())

        raw = "||".join(normalized)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
