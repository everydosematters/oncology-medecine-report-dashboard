"""Database connection utilities."""

import sqlite3

import pandas as pd

from src.models import DrugAlert


def create_table():
    """Create a table in SQLite."""

    with sqlite3.connect("data/recalls.db") as conn:
        cursor = conn.cursor()

        query = """
        CREATE TABLE IF NOT EXISTS recalls (
            record_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_org TEXT NOT NULL,
            source_url TEXT NOT NULL,
            product_name TEXT,
            source_country TEXT,
            manufacturer TEXT,
            distributor TEXT,
            publish_date TEXT,
            scraped_at TEXT NOT NULL,
            reason TEXT,
            more_info TEXT
        );
        """
        cursor.execute(query)


def upsert_df(conn: sqlite3.Connection, data: list[DrugAlert]) -> None:
    """Upsert to database."""

    if not data:
        return None
    data = [alert.model_dump() for alert in data]
    cols = list(data[0].keys())

    placeholders = ", ".join(["?"] * len(cols))

    sql = f"""
    INSERT INTO recalls ({", ".join(cols)})
    VALUES ({placeholders})
    ON CONFLICT(record_id) DO UPDATE SET
        -- required/provenance fields: always update
        source_id = excluded.source_id,
        source_org = excluded.source_org,
        source_url = excluded.source_url,

        -- optional fields: only update if incoming is NOT NULL
        product_name   = COALESCE(excluded.product_name, recalls.product_name),
        source_country = COALESCE(excluded.source_country, recalls.source_country),
        manufacturer   = COALESCE(excluded.manufacturer, recalls.manufacturer),
        distributor    = COALESCE(excluded.distributor, recalls.distributor),
        publish_date   = COALESCE(excluded.publish_date, recalls.publish_date),
        reason         = COALESCE(excluded.reason, recalls.reason),
        more_info      = COALESCE(excluded.more_info, recalls.more_info),

        -- scraped_at: keep the most recent non-null timestamp
        scraped_at = CASE
            WHEN excluded.scraped_at IS NULL THEN recalls.scraped_at
            WHEN recalls.scraped_at IS NULL THEN excluded.scraped_at
            WHEN excluded.scraped_at > recalls.scraped_at THEN excluded.scraped_at
            ELSE recalls.scraped_at
        END;
    """

    cur = conn.cursor()
    cur.executemany(sql, [tuple(row.get(c) for c in cols) for row in data])


def create_csv():
    """Create a CSV file from the database."""

    with sqlite3.connect("data/recalls.db") as conn:
        cursor = conn.cursor()

        query = "SELECT * FROM recalls"
        cursor.execute(query)
        data = cursor.fetchall()

    column_map = {
        "source_org": "Organization",
        "source_country": "Country",
        "product_name": "Product Name",
        "publish_date": "Publish Date",
        "reason": "Reason",
        "more_info": "More Info",
    }
    df = pd.DataFrame.from_records(
        data,
        columns=[
            "record_id",
            "source_id",
            "Organization",
            "URL",
            "Product Name",
            "Source Country",
            "Manufacturer",
            "Distributor",
            "Publish Date",
            "scraped_at",
            "Reason",
            "More Info",
        ],
    )
    df = df.rename(columns=column_map).drop(columns=["record_id", "source_id", "scraped_at"])
    df = df.sort_values(by="Publish Date", ascending=False).drop_duplicates()
    df.to_csv("data/recalls.csv", index=False)
