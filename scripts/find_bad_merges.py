#!/usr/bin/env python3
"""
Find researchers with potentially bad OpenAlex merges.
Flags researchers with >10 institutions (likely multiple people merged).
"""

import sqlite3
import httpx
import time
import json
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"
OPENALEX_EMAIL = "anirudh.sudarshan@utexas.edu"

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


def get_institution_count(client: httpx.Client, author_id: str) -> int:
    """Get the number of institutions for an author from OpenAlex."""
    rate_limit()
    try:
        resp = client.get(
            f"https://api.openalex.org/authors/{author_id}",
            params={"mailto": OPENALEX_EMAIL},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        institutions = data.get("last_known_institutions", [])
        return len(institutions)
    except Exception as e:
        print(f"  Error fetching {author_id}: {e}")
        return -1


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Add column for flagging bad merges if it doesn't exist
    try:
        cursor.execute("ALTER TABLE researchers ADD COLUMN institution_count INTEGER")
        cursor.execute("ALTER TABLE researchers ADD COLUMN likely_bad_merge INTEGER DEFAULT 0")
        conn.commit()
        print("Added institution_count and likely_bad_merge columns")
    except sqlite3.OperationalError:
        pass  # Columns already exist

    # Get all researchers (or filter by institution)
    cursor.execute("""
        SELECT id, name, synced_from
        FROM researchers
        WHERE institution_count IS NULL
        ORDER BY cited_by_count DESC
    """)
    researchers = cursor.fetchall()

    print(f"Checking {len(researchers)} researchers for bad merges...")
    print("=" * 60)

    client = httpx.Client()
    bad_merges = []
    checked = 0

    try:
        for author_id, name, synced_from in researchers:
            checked += 1
            inst_count = get_institution_count(client, author_id)

            if inst_count >= 0:
                # Update database
                is_bad = 1 if inst_count > 10 else 0
                cursor.execute(
                    "UPDATE researchers SET institution_count = ?, likely_bad_merge = ? WHERE id = ?",
                    (inst_count, is_bad, author_id)
                )

                if inst_count > 10:
                    bad_merges.append((author_id, name, synced_from, inst_count))
                    print(f"[{checked}] BAD MERGE: {name} ({synced_from}) - {inst_count} institutions")
                elif checked % 500 == 0:
                    print(f"[{checked}] Checked... {len(bad_merges)} bad merges found so far")

            if checked % 1000 == 0:
                conn.commit()

    except KeyboardInterrupt:
        print("\nInterrupted! Saving progress...")
    finally:
        conn.commit()
        client.close()

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total checked: {checked}")
    print(f"Bad merges found (>10 institutions): {len(bad_merges)}")

    if bad_merges:
        print("\nWorst offenders:")
        for aid, name, src, count in sorted(bad_merges, key=lambda x: -x[3])[:20]:
            print(f"  {name:40s} ({src:5s}) - {count} institutions")

    # Query total bad merges in DB
    cursor.execute("SELECT COUNT(*) FROM researchers WHERE likely_bad_merge = 1")
    total_bad = cursor.fetchone()[0]
    print(f"\nTotal flagged as bad merges in database: {total_bad}")

    conn.close()


if __name__ == "__main__":
    main()
