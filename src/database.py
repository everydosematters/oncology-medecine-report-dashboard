"""Database connection utilities."""

import sqlite3


def create_table():
    """Create a table in SQLite."""


    conn = sqlite3.connect("recalls.db")
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS recalls (
        -- Primary identifier
        record_id TEXT PRIMARY KEY,

        -- Provenance
        source_id TEXT NOT NULL,
        source_org TEXT NOT NULL,
        source_url TEXT NOT NULL,
        product_name TEXT,

        -- Optional metadata
        source_country TEXT,
        manufacturer TEXT,
        distributor TEXT,

        -- Dates (stored as ISO 8601 strings)
        publish_date TEXT,
        scraped_at TEXT NOT NULL,

        -- Content
        reason TEXT,
        more_info TEXT
    );
    """
    cursor.execute(query)
    conn.commit()
    conn.close()
