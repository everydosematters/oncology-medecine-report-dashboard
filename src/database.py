"""Database connection utilities."""

import sqlite3
import pandas as pd
import json

def create_table():
    """Create a table in SQLite."""

    with sqlite3.connect("recalls.db") as conn:
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


def upsert_df(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Upsert to database."""

    # Convert datetime → ISO string
    df["publish_date"] = df["publish_date"].apply(
        lambda x: x.isoformat()
    )
    df["scraped_at"] = df["scraped_at"].apply(
        lambda x: x.isoformat()
    )

    cols = list(df.columns)

    placeholders = ", ".join(["?"] * len(cols))

    sql = f"""
    INSERT INTO recalls ({", ".join(cols)})
    VALUES ({placeholders})
    ON CONFLICT(record_id) DO UPDATE SET
        -- required/provenance fields: always update (assumes latest scrape is most correct)
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
    cur.executemany(sql, df[cols].itertuples(index=False, name=None))