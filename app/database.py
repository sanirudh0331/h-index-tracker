"""
Database schema for H-Index tracker.
Uses SQLite for persistence.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"


def get_connection():
    """Get database connection."""
    DB_PATH.parent.mkdir(exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    """Initialize database schema."""
    conn = get_connection()
    cursor = conn.cursor()

    # Researchers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS researchers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            institution TEXT NOT NULL,
            orcid TEXT,
            works_count INTEGER,
            cited_by_count INTEGER,
            current_h_index INTEGER,
            slope REAL,
            status TEXT DEFAULT 'pending',  -- pending, backfilled, error
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # H-index history (one row per researcher per year)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS h_index_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            researcher_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            h_index INTEGER NOT NULL,
            FOREIGN KEY (researcher_id) REFERENCES researchers(id),
            UNIQUE(researcher_id, year)
        )
    """)

    # Monthly snapshots (for ongoing monitoring)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monthly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            researcher_id TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,  -- YYYY-MM
            h_index INTEGER NOT NULL,
            works_count INTEGER,
            cited_by_count INTEGER,
            FOREIGN KEY (researcher_id) REFERENCES researchers(id),
            UNIQUE(researcher_id, snapshot_date)
        )
    """)

    # Sync log
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_type TEXT NOT NULL,  -- scan, backfill, monthly
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            records_processed INTEGER,
            notes TEXT
        )
    """)

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


if __name__ == "__main__":
    init_db()
