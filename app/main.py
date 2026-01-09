import json
import sqlite3
import httpx
from pathlib import Path
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"
STATIC_PATH = Path(__file__).parent.parent / "static"
OPENALEX_EMAIL = "anirudh.sudarshan@utexas.edu"


def fetch_author_metadata(author_id: str) -> dict:
    """Fetch institution count, alternative names, and social links from OpenAlex."""
    try:
        with httpx.Client() as client:
            resp = client.get(
                f"https://api.openalex.org/authors/{author_id}",
                params={"mailto": OPENALEX_EMAIL},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            ids = data.get("ids", {})
            return {
                "institution_count": len(data.get("last_known_institutions", [])),
                "alternative_names": data.get("display_name_alternatives", []),
                "twitter": ids.get("twitter"),
                "wikipedia": ids.get("wikipedia")
            }
    except Exception:
        return {"institution_count": None, "alternative_names": [], "twitter": None, "wikipedia": None}


def fetch_top_papers(author_id: str, limit: int = 6) -> list:
    """Fetch top papers by citation count from OpenAlex."""
    try:
        with httpx.Client() as client:
            resp = client.get(
                "https://api.openalex.org/works",
                params={
                    "filter": f"author.id:{author_id},type:article",
                    "sort": "cited_by_count:desc",
                    "per_page": limit,
                    "mailto": OPENALEX_EMAIL
                },
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            papers = []
            for w in data.get("results", []):
                # Get DOI link or OpenAlex link
                doi = w.get("doi")
                openalex_id = w.get("id", "").replace("https://openalex.org/", "")
                link = doi if doi else f"https://openalex.org/{openalex_id}"

                papers.append({
                    "title": w.get("title", "Untitled"),
                    "year": w.get("publication_year"),
                    "citations": w.get("cited_by_count", 0),
                    "link": link
                })
            return papers
    except Exception:
        return []


app = FastAPI(title="KdT Talent Scout")
app.mount("/static", StaticFiles(directory=STATIC_PATH), name="static")
templates = Jinja2Templates(directory="templates")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def parse_multi_sort(sort_str: str) -> list:
    """Parse multi-column sort string like 'h_index:desc,cited_by_count:asc'."""
    valid_cols = ["h_index", "works_count", "cited_by_count", "two_yr_citedness", "name"]
    sorts = []
    if not sort_str:
        return [("h_index", "DESC")]
    for part in sort_str.split(","):
        if ":" in part:
            col, direction = part.split(":", 1)
            col = col.strip()
            direction = direction.strip().upper()
            if col in valid_cols and direction in ("ASC", "DESC"):
                sorts.append((col, direction))
        else:
            col = part.strip()
            if col in valid_cols:
                sorts.append((col, "DESC"))
    return sorts if sorts else [("h_index", "DESC")]


@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    sort: str = "h_index:desc",
    search: str = "",
    page: int = 1,
    per_page: int = 50,
    categories: str = "",
    institution: str = "",
    min_h: int = 0
):
    """Main dashboard showing all researchers."""
    conn = get_db()

    # Parse multi-column sort
    sorts = parse_multi_sort(sort)
    order_clause = ", ".join(f"{col} {direction}" for col, direction in sorts)

    # Get all categories for dropdown
    all_categories = [row[0] for row in conn.execute(
        "SELECT DISTINCT primary_category FROM researchers WHERE primary_category IS NOT NULL ORDER BY primary_category"
    ).fetchall()]

    # Get institutions for dropdown
    institutions = [row[0] for row in conn.execute(
        "SELECT DISTINCT synced_from FROM researchers WHERE synced_from IS NOT NULL ORDER BY synced_from"
    ).fetchall()]

    # Parse selected categories from query param
    selected_categories = [c.strip() for c in categories.split(",") if c.strip()] if categories else []

    # Build WHERE clause
    conditions = []
    params = []
    if search:
        conditions.append("name LIKE ?")
        params.append(f"%{search}%")
    if selected_categories:
        placeholders = ",".join("?" * len(selected_categories))
        conditions.append(f"primary_category IN ({placeholders})")
        params.extend(selected_categories)
    if institution:
        conditions.append("synced_from = ?")
        params.append(institution)
    if min_h > 0:
        conditions.append("h_index > ?")
        params.append(min_h)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # Count total
    total = conn.execute(
        f"SELECT COUNT(*) FROM researchers WHERE {where_clause}",
        params
    ).fetchone()[0]

    # Fetch researchers
    offset = (page - 1) * per_page
    researchers = conn.execute(
        f"""SELECT * FROM researchers
            WHERE {where_clause}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset]
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
            "sorts": sorts,
            "search": search,
            "selected_categories": selected_categories,
            "all_categories": all_categories,
            "institution": institution,
            "institutions": institutions,
            "min_h": min_h
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


def parse_rising_stars_sort(sort_str: str) -> list:
    """Parse multi-column sort string for rising stars."""
    valid_cols = ["slope", "h_index", "two_yr_citedness", "cited_by_count", "name"]
    sorts = []
    if not sort_str:
        return [("slope", "DESC")]
    for part in sort_str.split(","):
        if ":" in part:
            col, direction = part.split(":", 1)
            col = col.strip()
            direction = direction.strip().upper()
            if col in valid_cols and direction in ("ASC", "DESC"):
                sorts.append((col, direction))
        else:
            col = part.strip()
            if col in valid_cols:
                sorts.append((col, "DESC"))
    return sorts if sorts else [("slope", "DESC")]


@app.get("/rising-stars", response_class=HTMLResponse)
async def rising_stars(
    request: Request,
    sort: str = "slope:desc",
    page: int = 1,
    per_page: int = 50,
    start_year: int = 2015,
    end_year: int = 2025,
    categories: str = "",
    institution: str = "",
    min_h: int = 0
):
    """Rising stars - researchers with high H-index growth."""
    conn = get_db()

    # Validate year range
    start_year = max(2015, min(2025, start_year))
    end_year = max(start_year, min(2025, end_year))

    # Parse multi-column sort
    sorts = parse_rising_stars_sort(sort)

    # Get all categories for dropdown
    all_categories = [row[0] for row in conn.execute(
        "SELECT DISTINCT primary_category FROM researchers WHERE primary_category IS NOT NULL ORDER BY primary_category"
    ).fetchall()]

    # Get institutions for dropdown
    institutions = [row[0] for row in conn.execute(
        "SELECT DISTINCT synced_from FROM researchers WHERE synced_from IS NOT NULL ORDER BY synced_from"
    ).fetchall()]

    # Parse selected categories from query param
    selected_categories = [c.strip() for c in categories.split(",") if c.strip()] if categories else []

    # Build WHERE clause
    conditions = ["history_computed = 1"]
    params = []
    if selected_categories:
        placeholders = ",".join("?" * len(selected_categories))
        conditions.append(f"primary_category IN ({placeholders})")
        params.extend(selected_categories)
    if institution:
        conditions.append("synced_from = ?")
        params.append(institution)
    if min_h > 0:
        conditions.append("h_index > ?")
        params.append(min_h)

    where_clause = " AND ".join(conditions)

    # Count total with computed history
    total = conn.execute(
        f"SELECT COUNT(*) FROM researchers WHERE {where_clause}",
        params
    ).fetchone()[0]

    # Get all researchers with computed history (we'll calculate slope and sort in Python)
    all_researchers = conn.execute(f"""
        SELECT * FROM researchers
        WHERE {where_clause}
    """, params).fetchall()

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

    # Multi-column sort - build sort key function
    def make_sort_key(item):
        keys = []
        for col, direction in sorts:
            if col == "slope":
                val = item["dynamic_slope"]
            elif col == "h_index":
                val = item["h_index"]
            elif col == "two_yr_citedness":
                val = item["two_yr_citedness"]
            elif col == "cited_by_count":
                val = item["cited_by_count"]
            elif col == "name":
                val = item["name"]
            else:
                val = 0
            # Negate numeric values for DESC order
            if direction == "DESC" and isinstance(val, (int, float)):
                val = -val
            elif direction == "DESC" and isinstance(val, str):
                # For strings, we'll handle this differently
                val = val
            keys.append(val)
        return keys

    # For proper multi-column sorting with mixed directions, use successive stable sorts
    # Sort in reverse order of priority (last column first)
    for col, direction in reversed(sorts):
        reverse_sort = (direction == "DESC")
        if col == "slope":
            researchers_data.sort(key=lambda x: x["dynamic_slope"], reverse=reverse_sort)
        elif col == "h_index":
            researchers_data.sort(key=lambda x: x["h_index"], reverse=reverse_sort)
        elif col == "two_yr_citedness":
            researchers_data.sort(key=lambda x: x["two_yr_citedness"], reverse=reverse_sort)
        elif col == "cited_by_count":
            researchers_data.sort(key=lambda x: x["cited_by_count"], reverse=reverse_sort)
        elif col == "name":
            researchers_data.sort(key=lambda x: x["name"], reverse=reverse_sort)

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
            "sorts": sorts,
            "start_year": start_year,
            "end_year": end_year,
            "top_slope": top_slope,
            "avg_slope": avg_slope,
            "avg_h_index": avg_h,
            "all_categories": all_categories,
            "selected_categories": selected_categories,
            "institution": institution,
            "institutions": institutions,
            "min_h": min_h
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

    # Compute velocity metrics from existing data (using MEDIAN to handle outliers)
    data["data_quality_issues"] = []

    if data["counts_by_year"] and len(data["counts_by_year"]) >= 3:
        yearly_works = [(year, c.get("works", 0)) for year, c in data["counts_by_year"].items()]
        yearly_cited = [(year, c.get("cited", 0)) for year, c in data["counts_by_year"].items()]

        works_values = sorted([w for _, w in yearly_works])
        cited_values = sorted([c for _, c in yearly_cited])

        # Calculate medians
        works_median = works_values[len(works_values) // 2]
        cited_median = cited_values[len(cited_values) // 2]

        data["pub_velocity"] = works_median
        data["citation_velocity"] = cited_median

        # Detect publication anomaly (max > 5x median, where median > 5)
        max_works = max(works_values)
        max_works_year = [y for y, w in yearly_works if w == max_works][0]
        if works_median > 5 and max_works > 5 * works_median:
            data["data_quality_issues"].append({
                "type": "pub_spike",
                "message": f"Publication spike: {max_works:,} papers in {max_works_year} vs median of {works_median}"
            })

        # Detect citation anomaly (max > 10x median, where median > 100)
        max_cited = max(cited_values)
        max_cited_year = [y for y, c in yearly_cited if c == max_cited][0]
        if cited_median > 100 and max_cited > 10 * cited_median:
            data["data_quality_issues"].append({
                "type": "cite_spike",
                "message": f"Citation spike: {max_cited:,} citations in {max_cited_year} vs median of {cited_median:,}"
            })
    else:
        data["pub_velocity"] = None
        data["citation_velocity"] = None

    # Citations per paper
    if data["works_count"] and data["works_count"] > 0:
        data["citations_per_paper"] = round(data["cited_by_count"] / data["works_count"], 1)
    else:
        data["citations_per_paper"] = None

    # Calculate percentile within category
    category = data.get("primary_category")
    h_index = data.get("h_index", 0)
    if category and h_index:
        # Count researchers in same category with lower h-index
        result = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN h_index < ? THEN 1 ELSE 0 END) as below
            FROM researchers
            WHERE primary_category = ?
        """, (h_index, category)).fetchone()
        total_in_category = result[0]
        below_count = result[1]
        if total_in_category > 0:
            percentile = (below_count / total_in_category) * 100
            data["percentile"] = round(percentile, 1)
            data["category_rank"] = total_in_category - below_count
            data["category_total"] = total_in_category
            # Friendly label
            if percentile >= 99:
                data["percentile_label"] = "Top 1%"
            elif percentile >= 95:
                data["percentile_label"] = "Top 5%"
            elif percentile >= 90:
                data["percentile_label"] = "Top 10%"
            elif percentile >= 75:
                data["percentile_label"] = "Top 25%"
            elif percentile >= 50:
                data["percentile_label"] = "Top 50%"
            else:
                data["percentile_label"] = None
        else:
            data["percentile"] = None
            data["percentile_label"] = None
    else:
        data["percentile"] = None
        data["percentile_label"] = None

    # Get H-index history if available
    history = conn.execute("""
        SELECT year, h_index FROM h_index_history
        WHERE researcher_id = ?
        ORDER BY year
    """, (researcher_id,)).fetchall()
    data["h_history"] = {row[0]: row[1] for row in history}

    # Fetch institution count, alternative names, and social links (cache in DB)
    alt_names_raw = data.get("alternative_names")
    if data.get("institution_count") is None or not alt_names_raw:
        metadata = fetch_author_metadata(researcher_id)
        data["institution_count"] = metadata["institution_count"] or data.get("institution_count")
        data["alternative_names"] = metadata["alternative_names"]
        data["twitter"] = metadata["twitter"]
        data["wikipedia"] = metadata["wikipedia"]
        # Cache in database
        if metadata["institution_count"] is not None:
            try:
                conn.execute(
                    """UPDATE researchers SET institution_count = ?, alternative_names = ?,
                       twitter = ?, wikipedia = ? WHERE id = ?""",
                    (data["institution_count"], json.dumps(metadata["alternative_names"]),
                     metadata["twitter"], metadata["wikipedia"], researcher_id)
                )
                conn.commit()
            except Exception:
                pass  # Column might not exist yet
    else:
        # Load from database
        data["alternative_names"] = json.loads(alt_names_raw) if alt_names_raw else []
        data["twitter"] = data.get("twitter")
        data["wikipedia"] = data.get("wikipedia")

    # Check institution count for data quality issues (>= 10 suggests merged profiles)
    if data.get("institution_count") and data["institution_count"] >= 10:
        data["data_quality_issues"].append({
            "type": "institutions",
            "message": f"{data['institution_count']} institutions linked (possible merged profiles)"
        })

    # Fetch top papers for Key Papers section
    papers = fetch_top_papers(researcher_id, limit=6)
    # Sort by year (oldest to newest)
    data["top_papers"] = sorted(papers, key=lambda p: p.get("year") or 0)

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
