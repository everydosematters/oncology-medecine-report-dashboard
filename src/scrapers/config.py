FDA_US = {
    "source_id": "FDA_US",
    "source_country": "United States",
    "source_org": "U.S. Food and Drug Administration (FDA)",
    "base_url": "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
    "api_endpoint": "https://api.fda.gov/drug/enforcement.json",
}

FDA_GH = {
    "source_id": "FDA_GH",
    "source_org": "Food and Drugs Authority (Ghana)",
    "source_country": "Ghana",
    "listing_url": "https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/",
    "ajax_url": "https://fdaghana.gov.gh/wp-admin/admin-ajax.php",
    "table_id": "47",
    "headers": {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://fdaghana.gov.gh",
        "Referer": "https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/",
    },
}

HEALTH_CANADA = {
    "source_id": "HEALTH_CANADA",
    "source_country": "Canada",
    "source_org": "Health Canada",
    "base_url": "https://recalls-rappels.canada.ca/en",
    "api_endpoint": "https://recalls-rappels.canada.ca/sites/default/files/opendata-donneesouvertes/HCRSAMOpenData.json",
}

NAFDAC_NG = {
    "source_id": "NAFDAC_NG",
    "source_org": "National Agency for Food and Drug Administration and Control (NAFDAC)",
    "base_url": "https://nafdac.gov.ng/category/recalls-and-alerts/",
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
}
