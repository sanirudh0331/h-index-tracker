#!/usr/bin/env python3
"""
Sync researchers from multiple institutions via OpenAlex.
Handles duplicates automatically (researchers at multiple institutions only appear once).

Usage:
    python scripts/sync_institutions.py --institution hms
    python scripts/sync_institutions.py --institution berkeley
    python scripts/sync_institutions.py --institution hms berkeley duke
    python scripts/sync_institutions.py --all
    python scripts/sync_institutions.py --list  # Show available institutions
"""

import argparse
import json
import time
import sqlite3
import httpx
from pathlib import Path
from datetime import datetime

# Config
OPENALEX_EMAIL = "anirudh.sudarshan@utexas.edu"
DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"

# Supported institutions with their ROR IDs
INSTITUTIONS = {
    "hms": {
        "name": "Harvard Medical School",
        "ror": "https://ror.org/03vek6s52"
    },
    "harvard": {
        "name": "Harvard University",
        "ror": "https://ror.org/03vek6s52"
    },
    "berkeley": {
        "name": "UC Berkeley",
        "ror": "https://ror.org/01an7q238"
    },
    "stanford": {
        "name": "Stanford University",
        "ror": "https://ror.org/00f54p054"
    },
    "mit": {
        "name": "MIT",
        "ror": "https://ror.org/042nb2s44"
    },
    "duke": {
        "name": "Duke University",
        "ror": "https://ror.org/00py81415"
    },
    "baylor": {
        "name": "Baylor College of Medicine",
        "ror": "https://ror.org/02pttbw34"
    },
    "johns_hopkins": {
        "name": "Johns Hopkins University",
        "ror": "https://ror.org/00za53h95"
    },
    "ucsf": {
        "name": "UCSF",
        "ror": "https://ror.org/043mz5j54"
    },
    "yale": {
        "name": "Yale University",
        "ror": "https://ror.org/03v76x132"
    },
    "columbia": {
        "name": "Columbia University",
        "ror": "https://ror.org/00hj8s172"
    },
    "upenn": {
        "name": "University of Pennsylvania",
        "ror": "https://ror.org/00b30xv10"
    },
    "uchicago": {
        "name": "University of Chicago",
        "ror": "https://ror.org/024mw5h28"
    },
    "ucla": {
        "name": "UCLA",
        "ror": "https://ror.org/046rm7j60"
    },
    "caltech": {
        "name": "Caltech",
        "ror": "https://ror.org/05dxps055"
    },
    "ut_austin": {
        "name": "UT Austin",
        "ror": "https://ror.org/00hj54h04"
    },
}

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
    """Initialize database with updated schema."""
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
            topics TEXT,
            affiliations TEXT,
            counts_by_year TEXT,
            primary_category TEXT,
            history_computed INTEGER DEFAULT 0,
            slope REAL DEFAULT 0,
            synced_from TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add synced_from column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE researchers ADD COLUMN synced_from TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_h_index ON researchers(h_index DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cited_by ON researchers(cited_by_count DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_synced_from ON researchers(synced_from)")

    # Create h_index_history table if needed
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS h_index_history (
            researcher_id TEXT,
            year INTEGER,
            h_index INTEGER,
            PRIMARY KEY (researcher_id, year)
        )
    """)

    # Create topic_categories table if needed
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS topic_categories (
            topic_name TEXT PRIMARY KEY,
            category TEXT NOT NULL
        )
    """)

    conn.commit()
    return conn


def fetch_authors(client: httpx.Client, ror_id: str, cursor: str = "*", per_page: int = 100) -> dict:
    """Fetch authors from OpenAlex for a specific institution."""
    rate_limit()
    try:
        resp = client.get(
            "https://api.openalex.org/authors",
            params={
                "filter": f"last_known_institutions.ror:{ror_id}",
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


def extract_author_data(author: dict, institution_key: str) -> dict:
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
        "counts_by_year": json.dumps(counts),
        "synced_from": institution_key
    }


def save_author(conn: sqlite3.Connection, data: dict) -> bool:
    """
    Save author to database using INSERT OR IGNORE to prevent duplicates.
    Returns True if new researcher was inserted, False if already existed.
    """
    cursor = conn.cursor()

    # Check if researcher already exists
    existing = cursor.execute(
        "SELECT id, synced_from FROM researchers WHERE id = ?",
        (data["id"],)
    ).fetchone()

    if existing:
        # Update synced_from to include this institution if not already there
        current_synced = existing[1] or ""
        if data["synced_from"] not in current_synced:
            new_synced = f"{current_synced},{data['synced_from']}" if current_synced else data["synced_from"]
            cursor.execute(
                "UPDATE researchers SET synced_from = ?, updated_at = ? WHERE id = ?",
                (new_synced, datetime.now().isoformat(), data["id"])
            )
        return False  # Already existed

    # Insert new researcher
    cursor.execute("""
        INSERT INTO researchers
        (id, name, orcid, h_index, i10_index, works_count, cited_by_count,
         two_yr_citedness, topics, affiliations, counts_by_year, synced_from, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["id"], data["name"], data["orcid"], data["h_index"],
        data["i10_index"], data["works_count"], data["cited_by_count"],
        data["two_yr_citedness"], data["topics"], data["affiliations"],
        data["counts_by_year"], data["synced_from"], datetime.now().isoformat()
    ))
    return True  # New insertion


def sync_institution(conn: sqlite3.Connection, client: httpx.Client,
                     inst_key: str, inst_info: dict, limit: int = None) -> dict:
    """Sync a single institution. Returns stats."""
    print(f"\n{'='*60}")
    print(f"Syncing: {inst_info['name']}")
    print(f"ROR: {inst_info['ror']}")
    print(f"{'='*60}")

    cursor = "*"
    total_fetched = 0
    new_added = 0
    duplicates = 0

    while cursor:
        data = fetch_authors(client, inst_info['ror'], cursor)
        results = data.get("results", [])
        meta = data.get("meta", {})

        for author in results:
            author_data = extract_author_data(author, inst_key)
            is_new = save_author(conn, author_data)

            if is_new:
                new_added += 1
            else:
                duplicates += 1

            total_fetched += 1

            if total_fetched % 500 == 0:
                conn.commit()
                print(f"  Processed {total_fetched}... (new: {new_added}, existing: {duplicates})")

            if limit and total_fetched >= limit:
                break

        if limit and total_fetched >= limit:
            break

        cursor = meta.get("next_cursor")

    conn.commit()

    print(f"\n  Completed {inst_info['name']}:")
    print(f"    Total fetched: {total_fetched}")
    print(f"    New added: {new_added}")
    print(f"    Already existed: {duplicates}")

    return {
        "institution": inst_key,
        "name": inst_info['name'],
        "fetched": total_fetched,
        "new": new_added,
        "duplicates": duplicates
    }


def list_institutions():
    """Print available institutions."""
    print("\nAvailable institutions:")
    print("-" * 50)
    for key, info in sorted(INSTITUTIONS.items()):
        print(f"  {key:15s} - {info['name']}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Sync researchers from institutions via OpenAlex",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/sync_institutions.py --list
  python scripts/sync_institutions.py --institution hms
  python scripts/sync_institutions.py --institution berkeley stanford
  python scripts/sync_institutions.py --all --limit 1000
        """
    )
    parser.add_argument("--institution", "-i", nargs="+",
                        help="Institution(s) to sync (use --list to see options)")
    parser.add_argument("--all", action="store_true",
                        help="Sync all supported institutions")
    parser.add_argument("--list", "-l", action="store_true",
                        help="List available institutions")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit researchers per institution")
    args = parser.parse_args()

    if args.list:
        list_institutions()
        return

    if not args.institution and not args.all:
        parser.print_help()
        print("\nError: Specify --institution or --all")
        return

    # Determine which institutions to sync
    if args.all:
        institutions_to_sync = list(INSTITUTIONS.keys())
    else:
        institutions_to_sync = []
        for inst in args.institution:
            inst_lower = inst.lower()
            if inst_lower not in INSTITUTIONS:
                print(f"Warning: Unknown institution '{inst}'. Use --list to see options.")
            else:
                institutions_to_sync.append(inst_lower)

    if not institutions_to_sync:
        print("No valid institutions to sync.")
        return

    print("=" * 60)
    print("Multi-Institution Researcher Sync")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"Institutions: {', '.join(institutions_to_sync)}")
    if args.limit:
        print(f"Limit per institution: {args.limit}")
    print("=" * 60)

    conn = init_db()
    client = httpx.Client()

    results = []
    try:
        for inst_key in institutions_to_sync:
            inst_info = INSTITUTIONS[inst_key]
            stats = sync_institution(conn, client, inst_key, inst_info, args.limit)
            results.append(stats)
    finally:
        client.close()
        conn.close()

    # Summary
    print("\n" + "=" * 60)
    print("SYNC COMPLETE - SUMMARY")
    print("=" * 60)

    total_new = sum(r['new'] for r in results)
    total_dups = sum(r['duplicates'] for r in results)

    for r in results:
        print(f"  {r['name']:30s} +{r['new']:,} new, {r['duplicates']:,} existing")

    print("-" * 60)
    print(f"  {'TOTAL':30s} +{total_new:,} new, {total_dups:,} existing")
    print(f"\nDatabase: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
