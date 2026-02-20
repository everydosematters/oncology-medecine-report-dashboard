FDA_US = {
    "source_id": "FDA_US",
    "source_country": "United States",
    "source_org": "U.S. Food and Drug Administration (FDA)",
    "base_url": "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
    "api_endpoint": "https://api.fda.gov/drug/enforcement.json",
}

NAFDAC_NG = {
    "source_id": "NAFDAC_NG",
    "source_org": "National Agency for Food and Drug Administration and Control (NAFDAC)",
    "base_url": "https://nafdac.gov.ng/category/recalls-and-alerts/",
    "type": "listing_with_detail_pages",
    "request": {"headers": {"Accept-Language": "en-GB,en;q=0.9"}},
    "listing": {
        "item_selector": "table tbody tr",
        "link_selector": "td:nth-child(2) a.ninja_table_permalink",
        "date_selector": "td:nth-child(1)",
        "fields": {
            "alert_type": "td:nth-child(3)",
            "category": "td:nth-child(4)",
            "company": "td:nth-child(5)",
        },
    },
    "detail_page": {
        "title_selector": "h1.entry-title",
        "body_selector": "div.entry-content",
        "publish_date_selector": "time.entry-date",
    },
    "filters": {
        "oncology_keywords": [
            "oncology",
            "cancer",
            "tumour",
            "chemotherapy",
            "immunotherapy",
        ]
    },
    "defaults": {
        "therapeutic_category": "Oncology",
        "alert_type": "Recall / Safety Alert",
    },
}
