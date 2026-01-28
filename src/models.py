"""Module for Models"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

import pydantic as pyd


AlertType = Literal["Recall", "Safety Alert", "Public Alert", "Recall / Safety Alert"]


class DrugAlert(pyd.BaseModel):
    # Identifiers / provenance (required)
    record_id: str
    source_id: str
    source_country: str
    source_org: str
    source_url: str

    # Core content (optional but commonly filled)
    title: Optional[str] = None
    manufacturer_stated: Optional[str] = None
    manufactured_for: Optional[str] = None
    reason: Optional[str] = None

    therapeutic_category: Optional[str] = None
    alert_type: Optional[AlertType] = None

    publish_date: Optional[datetime] = None
    notes: Optional[str] = None

    # Always set at scrape-time (required)
    scraped_at: datetime
    
    product_name: list[str] | str
    batch_number: Optional[list[str] | str] = None
    expiry_date: Optional[list[str] | str] = None# FIXME this should be a datetime


    model_config = pyd.ConfigDict(
        extra="forbid",  # catch accidental fields like body_text if model doesn't include it
        validate_assignment=True,
    )
