import json
import sqlite3
import httpx
from pathlib import Path
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"
OPENALEX_EMAIL = "anirudh.sudarshan@utexas.edu"


def fetch_author_metadata(author_id: str) -> dict:
    """Fetch institution count and alternative names from OpenAlex."""
    try:
        with httpx.Client() as client:
            resp = client.get(
                f"https://api.openalex.org/authors/{author_id}",
                params={"mailto": OPENALEX_EMAIL},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "institution_count": len(data.get("last_known_institutions", [])),
                "alternative_names": data.get("display_name_alternatives", [])
            }
    except Exception:
        return {"institution_count": None, "alternative_names": []}

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


def calculate_slope(history: dict, start_year: int, end_year: int) -> float:
    """Calculate slope for a given year range using linear regression."""
    years = [y for y in range(start_year, end_year + 1) if y in history]
    if len(years) < 2:
        return 0.0

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


@app.get("/rising-stars", response_class=HTMLResponse)
async def rising_stars(
    request: Request,
    sort: str = "slope",
    order: str = "desc",
    page: int = 1,
    per_page: int = 50,
    start_year: int = 2015,
    end_year: int = 2025,
    category: str = ""
):
    """Rising stars - researchers with high H-index growth."""
    conn = get_db()

    # Validate year range
    start_year = max(2015, min(2025, start_year))
    end_year = max(start_year, min(2025, end_year))

    # Valid sort columns
    valid_sorts = ["slope", "h_index", "two_yr_citedness", "name"]
    if sort not in valid_sorts:
        sort = "slope"
    order_dir = "DESC" if order == "desc" else "ASC"

    # Get all categories for dropdown
    categories = [row[0] for row in conn.execute(
        "SELECT DISTINCT primary_category FROM researchers WHERE primary_category IS NOT NULL ORDER BY primary_category"
    ).fetchall()]

    # Count total with computed history (filtered by category if specified)
    if category:
        total = conn.execute(
            "SELECT COUNT(*) FROM researchers WHERE history_computed = 1 AND primary_category = ?",
            (category,)
        ).fetchone()[0]
    else:
        total = conn.execute(
            "SELECT COUNT(*) FROM researchers WHERE history_computed = 1"
        ).fetchone()[0]

    # Get all researchers with computed history (we'll calculate slope and sort in Python)
    if category:
        all_researchers = conn.execute("""
            SELECT * FROM researchers
            WHERE history_computed = 1 AND primary_category = ?
        """, (category,)).fetchall()
    else:
        all_researchers = conn.execute("""
            SELECT * FROM researchers
            WHERE history_computed = 1
        """).fetchall()

    # Get all H-index history at once for efficiency
    all_history = conn.execute("""
        SELECT researcher_id, year, h_index FROM h_index_history
        WHERE year BETWEEN ? AND ?
    """, (start_year, end_year)).fetchall()

    # Build history lookup
    history_lookup = {}
    for row in all_history:
        rid, year, h = row
        if rid not in history_lookup:
            history_lookup[rid] = {}
        history_lookup[rid][year] = h

    # Process researchers and calculate dynamic slope
    researchers_data = []
    for r in all_researchers:
        data = dict(r)
        data["topics"] = json.loads(data["topics"]) if data["topics"] else []
        data["affiliations"] = json.loads(data["affiliations"]) if data["affiliations"] else []

        # Get full history for sparkline
        full_history = conn.execute("""
            SELECT year, h_index FROM h_index_history
            WHERE researcher_id = ?
            ORDER BY year
        """, (data["id"],)).fetchall()
        data["h_history"] = {row[0]: row[1] for row in full_history}

        # Calculate slope for selected year range
        h_in_range = history_lookup.get(data["id"], {})
        data["dynamic_slope"] = calculate_slope(h_in_range, start_year, end_year)

        # Get start and end H-index for the selected range
        data["h_start"] = h_in_range.get(start_year, 0)
        data["h_end"] = h_in_range.get(end_year, data["h_index"])

        researchers_data.append(data)

    # Sort by the selected column
    if sort == "slope":
        researchers_data.sort(key=lambda x: x["dynamic_slope"], reverse=(order == "desc"))
    elif sort == "h_index":
        researchers_data.sort(key=lambda x: x["h_index"], reverse=(order == "desc"))
    elif sort == "two_yr_citedness":
        researchers_data.sort(key=lambda x: x["two_yr_citedness"], reverse=(order == "desc"))
    elif sort == "name":
        researchers_data.sort(key=lambda x: x["name"], reverse=(order == "desc"))

    # Calculate stats for selected year range
    slopes = [r["dynamic_slope"] for r in researchers_data]
    top_slope = max(slopes) if slopes else 0
    avg_slope = sum(slopes) / len(slopes) if slopes else 0
    avg_h = sum(r["h_index"] for r in researchers_data) / len(researchers_data) if researchers_data else 0

    # Paginate
    total_pages = (total + per_page - 1) // per_page
    offset = (page - 1) * per_page
    paginated = researchers_data[offset:offset + per_page]

    conn.close()

    return templates.TemplateResponse(
        "rising_stars.html",
        {
            "request": request,
            "researchers": paginated,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "sort": sort,
            "order": order,
            "start_year": start_year,
            "end_year": end_year,
            "top_slope": top_slope,
            "avg_slope": avg_slope,
            "avg_h_index": avg_h,
            "categories": categories,
            "category": category
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

    # Fetch institution count and alternative names (cache in DB)
    alt_names_raw = data.get("alternative_names")
    if data.get("institution_count") is None or not alt_names_raw:
        metadata = fetch_author_metadata(researcher_id)
        data["institution_count"] = metadata["institution_count"] or data.get("institution_count")
        data["alternative_names"] = metadata["alternative_names"]
        # Cache in database
        if metadata["institution_count"] is not None:
            try:
                conn.execute(
                    "UPDATE researchers SET institution_count = ?, alternative_names = ? WHERE id = ?",
                    (data["institution_count"], json.dumps(metadata["alternative_names"]), researcher_id)
                )
                conn.commit()
            except Exception:
                pass  # Column might not exist yet
    else:
        # Load from database
        data["alternative_names"] = json.loads(alt_names_raw) if alt_names_raw else []

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
