#!/usr/bin/env python3
"""
Compute historical H-index for rising star candidates.
Run with: python scripts/compute_history.py [--limit N]
"""

import argparse
import time
import sqlite3
import httpx
from pathlib import Path
from datetime import datetime

# Config
OPENALEX_EMAIL = "sanirudh0331@gmail.com"
DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"
HISTORY_START = 2015
HISTORY_END = 2025

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


def fetch_works(client: httpx.Client, author_id: str, cursor: str = "*") -> dict:
    """Fetch works for an author."""
    rate_limit()
    # Handle both full URL and just ID
    if author_id.startswith("http"):
        author_id = author_id.split("/")[-1]

    try:
        resp = client.get(
            "https://api.openalex.org/works",
            params={
                "filter": f"author.id:{author_id}",
                "per_page": 200,
                "cursor": cursor,
                "select": "id,publication_year,cited_by_count,counts_by_year",
                "mailto": OPENALEX_EMAIL
            },
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"    Error fetching works: {e}")
        return {"results": [], "meta": {"next_cursor": None}}


def get_all_works(client: httpx.Client, author_id: str) -> list:
    """Fetch all works for an author."""
    all_works = []
    cursor = "*"

    while cursor:
        data = fetch_works(client, author_id, cursor)
        results = data.get("results", [])
        all_works.extend(results)
        cursor = data.get("meta", {}).get("next_cursor")

        if len(all_works) > 2000:  # Safety limit
            break

    return all_works


def calculate_h_index_at_year(works: list, target_year: int) -> int:
    """
    Calculate what H-index would have been at end of target_year.
    """
    cumulative_citations = []

    for work in works:
        pub_year = work.get("publication_year")
        if pub_year is None or pub_year > target_year:
            continue  # Paper not published yet

        # Sum citations up to target_year
        total_citations = 0
        counts_by_year = work.get("counts_by_year", [])

        for cy in counts_by_year:
            if cy.get("year", 0) <= target_year:
                total_citations += cy.get("cited_by_count", 0)

        # Fallback for papers without counts_by_year
        if not counts_by_year and pub_year <= target_year:
            # Use current total as approximation
            total_citations = work.get("cited_by_count", 0)

        cumulative_citations.append(total_citations)

    # Calculate H-index
    cumulative_citations.sort(reverse=True)
    h_index = 0
    for i, citations in enumerate(cumulative_citations):
        if citations >= i + 1:
            h_index = i + 1
        else:
            break

    return h_index


def calculate_slope(history: dict) -> float:
    """Calculate slope using linear regression."""
    if len(history) < 2:
        return 0.0

    years = sorted(history.keys())
    n = len(years)

    sum_x = sum(years)
    sum_y = sum(history[y] for y in years)
    sum_xy = sum(y * history[y] for y in years)
    sum_x2 = sum(y * y for y in years)

    denominator = n * sum_x2 - sum_x * sum_x
    if denominator == 0:
        return 0.0

    slope = (n * sum_xy - sum_x * sum_y) / denominator
    return round(slope, 3)


def main():
    parser = argparse.ArgumentParser(description="Compute H-index history for rising stars")
    parser.add_argument("--limit", type=int, default=100, help="Number of candidates to process")
    args = parser.parse_args()

    print("=" * 60)
    print("Rising Stars - H-Index History Computation")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"Processing: {args.limit} candidates")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get candidates (all researchers without history)
    candidates = cursor.execute("""
        SELECT id, name, h_index, two_yr_citedness, works_count
        FROM researchers
        WHERE history_computed = 0
        ORDER BY two_yr_citedness DESC
        LIMIT ?
    """, (args.limit,)).fetchall()

    print(f"\nFound {len(candidates)} candidates to process\n")

    client = httpx.Client()
    processed = 0
    errors = 0

    try:
        for i, candidate in enumerate(candidates):
            author_id = candidate["id"]
            name = candidate["name"]

            print(f"[{i+1}/{len(candidates)}] {name} (H={candidate['h_index']}, 2yr={candidate['two_yr_citedness']:.1f})")

            try:
                # Fetch all works
                works = get_all_works(client, author_id)
                print(f"    Fetched {len(works)} works")

                # Calculate H-index for each year
                history = {}
                for year in range(HISTORY_START, HISTORY_END + 1):
                    h = calculate_h_index_at_year(works, year)
                    history[year] = h

                    # Save to h_index_history
                    cursor.execute("""
                        INSERT OR REPLACE INTO h_index_history (researcher_id, year, h_index)
                        VALUES (?, ?, ?)
                    """, (author_id, year, h))

                # Calculate slope
                slope = calculate_slope(history)

                # Update researcher
                cursor.execute("""
                    UPDATE researchers
                    SET history_computed = 1, slope = ?
                    WHERE id = ?
                """, (slope, author_id))

                conn.commit()

                print(f"    History: {history[HISTORY_START]} → {history[HISTORY_END]}, slope={slope}")
                processed += 1

            except Exception as e:
                print(f"    ERROR: {e}")
                errors += 1
                continue

    finally:
        client.close()
        conn.close()

    print("\n" + "=" * 60)
    print(f"Complete!")
    print(f"Processed: {processed}")
    print(f"Errors: {errors}")
    print("=" * 60)


if __name__ == "__main__":
    main()
