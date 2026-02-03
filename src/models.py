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
    source_org: str
    source_url: str

    # Core content (optional but commonly filled)
    source_country: Optional[str] = None
    manufacturer: Optional[str] = None
    alert_type: Optional[AlertType] = None

    publish_date: Optional[datetime] = None
    notes: Optional[str] = None

    # Always set at scrape-time (required)
    scraped_at: datetime
    product_name: Optional[list[str] | str] = None
    brand_name: Optional[str] = None
    generic_name: Optional[str] = None
    batch_number: Optional[list[str] | str] = None
    expiry_date: Optional[datetime] = None
    date_of_manufacture: Optional[datetime] = None


    model_config = pyd.ConfigDict(
        extra="forbid",  # catch accidental fields like body_text if model doesn't include it
        validate_assignment=True,
    )
