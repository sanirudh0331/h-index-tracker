import json
import sqlite3
from pathlib import Path
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"

app = FastAPI(title="HMS Researcher Tracker")
templates = Jinja2Templates(directory="templates")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    sort: str = "h_index",
    order: str = "desc",
    search: str = "",
    page: int = 1,
    per_page: int = 50
):
    """Main dashboard showing all researchers."""
    conn = get_db()

    # Valid sort columns
    valid_sorts = ["h_index", "works_count", "cited_by_count", "two_yr_citedness", "name"]
    if sort not in valid_sorts:
        sort = "h_index"
    order_dir = "DESC" if order == "desc" else "ASC"

    # Count total
    if search:
        total = conn.execute(
            "SELECT COUNT(*) FROM researchers WHERE name LIKE ?",
            (f"%{search}%",)
        ).fetchone()[0]
    else:
        total = conn.execute("SELECT COUNT(*) FROM researchers").fetchone()[0]

    # Fetch researchers
    offset = (page - 1) * per_page
    if search:
        researchers = conn.execute(
            f"""SELECT * FROM researchers
                WHERE name LIKE ?
                ORDER BY {sort} {order_dir}
                LIMIT ? OFFSET ?""",
            (f"%{search}%", per_page, offset)
        ).fetchall()
    else:
        researchers = conn.execute(
            f"SELECT * FROM researchers ORDER BY {sort} {order_dir} LIMIT ? OFFSET ?",
            (per_page, offset)
        ).fetchall()

    conn.close()

    # Parse JSON fields
    researchers_data = []
    for r in researchers:
        data = dict(r)
        data["topics"] = json.loads(data["topics"]) if data["topics"] else []
        data["affiliations"] = json.loads(data["affiliations"]) if data["affiliations"] else []
        data["counts_by_year"] = json.loads(data["counts_by_year"]) if data["counts_by_year"] else {}
        researchers_data.append(data)

    total_pages = (total + per_page - 1) // per_page

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "researchers": researchers_data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "sort": sort,
            "order": order,
            "search": search
        }
    )


@app.get("/rising-stars", response_class=HTMLResponse)
async def rising_stars(request: Request):
    """Rising stars - researchers with high H-index growth."""
    conn = get_db()

    # Get researchers with computed history, sorted by slope
    researchers = conn.execute("""
        SELECT * FROM researchers
        WHERE history_computed = 1 AND slope > 0
        ORDER BY slope DESC
        LIMIT 100
    """).fetchall()

    # Get their H-index history
    researchers_data = []
    for r in researchers:
        data = dict(r)
        data["topics"] = json.loads(data["topics"]) if data["topics"] else []
        data["affiliations"] = json.loads(data["affiliations"]) if data["affiliations"] else []

        # Get H-index history
        history = conn.execute("""
            SELECT year, h_index FROM h_index_history
            WHERE researcher_id = ?
            ORDER BY year
        """, (data["id"],)).fetchall()
        data["h_history"] = {row[0]: row[1] for row in history}

        researchers_data.append(data)

    conn.close()

    return templates.TemplateResponse(
        "rising_stars.html",
        {
            "request": request,
            "researchers": researchers_data,
            "total": len(researchers_data)
        }
    )


@app.get("/researcher/{researcher_id}", response_class=HTMLResponse)
async def researcher_detail(request: Request, researcher_id: str):
    """Detail page for a single researcher."""
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM researchers WHERE id = ?",
        (researcher_id,)
    ).fetchone()

    if not row:
        conn.close()
        return HTMLResponse("Researcher not found", status_code=404)

    data = dict(row)
    data["topics"] = json.loads(data["topics"]) if data["topics"] else []
    data["affiliations"] = json.loads(data["affiliations"]) if data["affiliations"] else []
    data["counts_by_year"] = json.loads(data["counts_by_year"]) if data["counts_by_year"] else {}

    # Get H-index history if available
    history = conn.execute("""
        SELECT year, h_index FROM h_index_history
        WHERE researcher_id = ?
        ORDER BY year
    """, (researcher_id,)).fetchall()
    data["h_history"] = {row[0]: row[1] for row in history}

    conn.close()

    return templates.TemplateResponse(
        "researcher.html",
        {"request": request, "r": data}
    )


@app.get("/api/researchers")
async def api_researchers(
    sort: str = "h_index",
    order: str = "desc",
    limit: int = 100
):
    """API endpoint for researcher data."""
    conn = get_db()
    valid_sorts = ["h_index", "works_count", "cited_by_count", "two_yr_citedness"]
    if sort not in valid_sorts:
        sort = "h_index"
    order_dir = "DESC" if order == "desc" else "ASC"

    researchers = conn.execute(
        f"SELECT * FROM researchers ORDER BY {sort} {order_dir} LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()

    return [dict(r) for r in researchers]


@app.get("/api/stats")
async def api_stats():
    """Aggregate statistics."""
    conn = get_db()
    stats = {
        "total_researchers": conn.execute("SELECT COUNT(*) FROM researchers").fetchone()[0],
        "avg_h_index": conn.execute("SELECT AVG(h_index) FROM researchers").fetchone()[0],
        "max_h_index": conn.execute("SELECT MAX(h_index) FROM researchers").fetchone()[0],
        "total_citations": conn.execute("SELECT SUM(cited_by_count) FROM researchers").fetchone()[0],
    }
    conn.close()
    return stats


@app.get("/health")
async def health():
    return {"status": "healthy"}
