#!/usr/bin/env python3
"""
Sync HMS researchers from OpenAlex.
Run with: python scripts/sync_hms.py [--limit N]
"""

import argparse
import json
import time
import sqlite3
import httpx
from pathlib import Path
from datetime import datetime

# Config
HMS_ROR = "https://ror.org/03vek6s52"
OPENALEX_EMAIL = "hindex-tracker@example.com"
DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"

# Rate limiting
REQUESTS_PER_SECOND = 9
last_request_time = 0


def rate_limit():
    global last_request_time
    elapsed = time.time() - last_request_time
    min_interval = 1.0 / REQUESTS_PER_SECOND
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    last_request_time = time.time()


def init_db():
    """Initialize database."""
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS researchers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            orcid TEXT,
            h_index INTEGER,
            i10_index INTEGER,
            works_count INTEGER,
            cited_by_count INTEGER,
            two_yr_citedness REAL,
            topics TEXT,  -- JSON array
            affiliations TEXT,  -- JSON array
            counts_by_year TEXT,  -- JSON object
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_h_index ON researchers(h_index DESC)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_cited_by ON researchers(cited_by_count DESC)
    """)

    conn.commit()
    return conn


def fetch_authors(client: httpx.Client, cursor: str = "*", per_page: int = 100) -> dict:
    """Fetch authors from OpenAlex."""
    rate_limit()
    try:
        resp = client.get(
            "https://api.openalex.org/authors",
            params={
                "filter": f"last_known_institutions.ror:{HMS_ROR}",
                "per_page": per_page,
                "cursor": cursor,
                "mailto": OPENALEX_EMAIL
            },
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching: {e}")
        return {"results": [], "meta": {"next_cursor": None}}


def extract_author_data(author: dict) -> dict:
    """Extract relevant fields from OpenAlex author."""
    stats = author.get("summary_stats", {}) or {}

    # Extract topics (top 5)
    topics = []
    for t in author.get("topics", [])[:5]:
        topics.append({
            "name": t.get("display_name"),
            "count": t.get("count")
        })

    # Extract affiliations
    affiliations = []
    for inst in author.get("last_known_institutions", [])[:5]:
        affiliations.append({
            "name": inst.get("display_name"),
            "type": inst.get("type"),
            "country": inst.get("country_code")
        })

    # Extract counts by year (last 10 years)
    counts = {}
    for c in author.get("counts_by_year", []):
        year = c.get("year")
        if year and year >= 2015:
            counts[year] = {
                "works": c.get("works_count", 0),
                "cited": c.get("cited_by_count", 0)
            }

    return {
        "id": author.get("id", "").replace("https://openalex.org/", ""),
        "name": author.get("display_name", "Unknown"),
        "orcid": author.get("orcid"),
        "h_index": stats.get("h_index") or 0,
        "i10_index": stats.get("i10_index") or 0,
        "works_count": author.get("works_count") or 0,
        "cited_by_count": author.get("cited_by_count") or 0,
        "two_yr_citedness": stats.get("2yr_mean_citedness") or 0,
        "topics": json.dumps(topics),
        "affiliations": json.dumps(affiliations),
        "counts_by_year": json.dumps(counts)
    }


def save_author(conn: sqlite3.Connection, data: dict):
    """Save author to database."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO researchers
        (id, name, orcid, h_index, i10_index, works_count, cited_by_count,
         two_yr_citedness, topics, affiliations, counts_by_year, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["id"], data["name"], data["orcid"], data["h_index"],
        data["i10_index"], data["works_count"], data["cited_by_count"],
        data["two_yr_citedness"], data["topics"], data["affiliations"],
        data["counts_by_year"], datetime.now().isoformat()
    ))


def main():
    parser = argparse.ArgumentParser(description="Sync HMS researchers from OpenAlex")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of researchers to sync")
    parser.add_argument("--min-works", type=int, default=0, help="Minimum works count filter")
    args = parser.parse_args()

    print("=" * 60)
    print("HMS Researcher Sync")
    print(f"Started: {datetime.now().isoformat()}")
    if args.limit:
        print(f"Limit: {args.limit} researchers")
    print("=" * 60)

    conn = init_db()
    client = httpx.Client()

    cursor = "*"
    total = 0
    saved = 0

    try:
        while cursor:
            data = fetch_authors(client, cursor)
            results = data.get("results", [])
            meta = data.get("meta", {})

            for author in results:
                works = author.get("works_count", 0) or 0
                if works < args.min_works:
                    continue

                author_data = extract_author_data(author)
                save_author(conn, author_data)
                saved += 1

                if saved % 100 == 0:
                    conn.commit()
                    print(f"  Saved {saved} researchers...")

                if args.limit and saved >= args.limit:
                    break

            total += len(results)

            if args.limit and saved >= args.limit:
                break

            cursor = meta.get("next_cursor")

        conn.commit()

    finally:
        client.close()
        conn.close()

    print()
    print("=" * 60)
    print(f"Sync complete!")
    print(f"Total fetched: {total}")
    print(f"Saved to DB: {saved}")
    print(f"Database: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
