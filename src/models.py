"""Data models for standardized drug alerts."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pydantic as pyd


class DrugAlert(pyd.BaseModel):
    # Identifiers / provenance (required)
    record_id: str
    source_id: str
    source_org: str
    source_url: str

    # Core content (optional but commonly filled)
    source_country: Optional[str] = None
    manufacturer: Optional[str] = None
    distributor: Optional[str] = None

    publish_date: Optional[datetime] = None
    reason: Optional[str] = None
    more_info: Optional[str] = None

    # Always set at scrape-time (required)
    scraped_at: datetime
    product_name: str = None

    model_config = pyd.ConfigDict(
        extra="forbid",  # catch accidental fields like body_text if model doesn't include it
        validate_assignment=True,
    )
