"""Module for Models"""
import pydantic as pyd
from typing import Literal, Optional
from datetime import datetime

class DrugAlert(pyd.BaseModel):
    source_country: str
    source_org: str
    source_url: str
    title: Optional[str]
    manufacturer_stated: Optional[str]
    manufactured_for: Optional[str]
    product_name: Optional[str]
    reason: Optional[str]
    alert_type: Optional[Literal['Recall', 'Safety Alert', 'Public Alert']]
    publish_date: Optional[datetime]
    notes: Optional
    scraped_at: datetime
    record_id: str
