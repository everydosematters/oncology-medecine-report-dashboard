"""Module for Models"""
import pydantic as pyd
from typing import Literal, Optional
from datetime import datetime

class DrugAlert(pyd.BaseModel):
    source_id: str
    source_country: str
    source_org: str
    source_url: str
    title: Optional[str]
    manufacturer_stated: Optional[str]
    manufactured_for: Optional[str]
    product_name: Optional[str]
    reason: Optional[str]
    therapeutic_category: Optional[str]
    alert_type: Optional[Literal['Recall', 'Safety Alert', 'Public Alert', 'Recall / Safety Alert']]
    publish_date: Optional[datetime]
    notes: Optional[str]
    scraped_at: datetime
    record_id: str
