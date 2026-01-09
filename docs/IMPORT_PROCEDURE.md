# Institution Import Procedure

This document outlines the steps to import a new institution into the H-Index Tracker.

## Pre-requisites

- [ ] Institution ROR ID added to `INSTITUTIONS` dict in `scripts/sync_institutions.py`
- [ ] OpenAlex API accessible (rate limit: 10 req/sec with email)

To find an institution's ROR ID, search at: https://ror.org/

## Step 1: Sync Researchers

```bash
cd /path/to/h_index_tracker
source venv/bin/activate
python scripts/sync_institutions.py --institution <name>
```

**Time estimate:** ~1-2 hours per 50k researchers

**What it does:**
- Fetches all researchers affiliated with the institution from OpenAlex
- Creates researcher records with: id, name, h_index, i10_index, works_count, cited_by_count, two_yr_citedness, topics, affiliations, counts_by_year, synced_from
- Handles duplicates (researcher at multiple institutions only stored once)

**Verify:**
```bash
sqlite3 data/hindex.db "SELECT COUNT(*) FROM researchers WHERE synced_from = '<name>'"
```

---

## Step 2: Compute H-Index History

```bash
python scripts/compute_history.py
```

**Time estimate:** ~3-5 hours (fetches all works for each researcher without history)

**What it does:**
- For each researcher, fetches all their works from OpenAlex
- Calculates h-index for each year from 2015-2025
- Computes slope (linear regression on h-index growth)
- Updates `h_index_history` table and `history_computed`, `slope` fields

**Note:** Only ~60% of researchers will have slope > 0. This is expected because:
- h=0 researchers: 0.5% have growth
- h=1 researchers: 42% have growth
- h>10 researchers: 97% have growth

**Verify:**
```bash
sqlite3 data/hindex.db "
SELECT COUNT(*) as total,
       SUM(CASE WHEN history_computed = 1 THEN 1 ELSE 0 END) as computed
FROM researchers WHERE synced_from = '<name>'"
```

---

## Step 3: Categorize Topics

```bash
python scripts/categorize_topics.py
```

**Time estimate:** ~2-5 minutes (local processing, no API calls)

**What it does:**
- Maps raw OpenAlex topic names to ~25 curated categories
- Uses keyword matching with priority ordering
- Updates `primary_category` field

**Note:** ~3-5% of researchers may not get a category if:
- They have no topics in OpenAlex
- Their topics don't match any category keywords

**Verify:**
```bash
sqlite3 data/hindex.db "
SELECT COUNT(*) as total,
       SUM(CASE WHEN primary_category IS NOT NULL THEN 1 ELSE 0 END) as categorized
FROM researchers WHERE synced_from = '<name>'"
```

---

## Step 4: Verify Data Quality (Optional)

```bash
python scripts/find_bad_merges.py
```

**Time estimate:** ~1 minute

**What it does:**
- Identifies researchers with potential data quality issues
- Flags profiles that may be merged (multiple people with same name)

---

## Step 5: Final Verification

Run this SQL to check completeness:

```sql
SELECT
    synced_from,
    COUNT(*) as total,
    SUM(CASE WHEN history_computed = 1 THEN 1 ELSE 0 END) as history_done,
    SUM(CASE WHEN slope > 0 THEN 1 ELSE 0 END) as has_slope,
    SUM(CASE WHEN primary_category IS NOT NULL THEN 1 ELSE 0 END) as has_category,
    ROUND(100.0 * SUM(CASE WHEN history_computed = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as history_pct,
    ROUND(100.0 * SUM(CASE WHEN primary_category IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as category_pct
FROM researchers
WHERE synced_from = '<name>'
GROUP BY synced_from;
```

**Expected results:**
- `history_pct`: 100%
- `category_pct`: 95-97%

---

## Lazy-Loaded Fields

These fields are populated automatically when a researcher profile is viewed:

- `institution_count` - Number of affiliated institutions (for data quality warning)
- `alternative_names` - Name variations in OpenAlex
- `twitter` - Twitter/X handle if available
- `wikipedia` - Wikipedia URL if available

No action needed - the app fetches and caches these on first profile view.

---

## Reference: Current Institution Stats

| Institution | Total | History | Slope > 0 | Category |
|-------------|-------|---------|-----------|----------|
| HMS | 91,686 | 100% | 62% | 96% |
| MIT | 39,422 | 100% | 52% | 97% |

---

## Adding a New Institution

If the institution isn't in `sync_institutions.py`, add it:

```python
INSTITUTIONS = {
    # ... existing ...
    "new_inst": {
        "name": "Full Institution Name",
        "ror": "https://ror.org/XXXXXXXX"
    },
}
```

Then follow Steps 1-5 above.

---

## Troubleshooting

### Sync is slow
- OpenAlex rate limit is 10 req/sec with polite email
- Large institutions (50k+ researchers) take 1-2 hours

### Many researchers missing categories
- Check if their topics match keywords in `categorize_topics.py`
- May need to add new keywords for institution-specific research areas

### Duplicate researchers
- The sync script automatically handles duplicates
- Researcher found at multiple institutions is stored once with `synced_from` = first institution
- Additional institutions tracked in `also_found_in` field
