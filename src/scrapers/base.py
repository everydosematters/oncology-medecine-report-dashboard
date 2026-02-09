"""Base scraper utilities shared across site-specific scrapers."""

import hashlib
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List, Optional, final

import requests
from bs4 import BeautifulSoup

from src.models import DrugAlert


class BaseScraper(ABC):
    """Abstract base class for all site-specific scrapers."""

    def __init__(
        self,
        url: str,
        *,
        args: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
        start_date: Optional[datetime] = None,
    ) -> None:
        """Initialize the scraper with a base URL and optional request args."""
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
        """Fetch a URL and return minimal response metadata plus parsed HTML."""
        target = url or self.url
        resp = requests.get(target, timeout=self.timeout, **self.args)
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        return {
            "final_url": resp.url,
            "status_code": resp.status_code,
            "html": soup,
        }

    @abstractmethod
    def standardize(self) -> List[DrugAlert]:
        """Standardize the scraper's data into a list of DrugAlert objects."""
        raise NotImplementedError

    @final
    def make_record_id(*parts: Any) -> str:
        """Build a stable record identifier from heterogeneous parts."""
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

    # TODO is oncology using https://www.cancer.gov/about-cancer/treatment/drugs/cancer-drugs?utm_source=chatgpt.com
