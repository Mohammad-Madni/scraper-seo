# Execution Order & File Dependencies

## Correct Execution Order

### Phase 1: Data Collection

1. **main.py** (Entry point)
   - Creates: `serp_outputs/` (folder with CSV files)
   - Expects: `list.csv`, API credentials in `config.py`
   - Does: Fetches Google Search results from DataForSEO API

### Phase 2: Page Content Processing

2. **on_page_post.py**

   - Creates: `queued_tasks/` (JSON task files), `queued_tasks/_error_summary.csv`
   - Expects: `serp_outputs/` CSV files from main.py
   - Does: Posts content parsing tasks to DataForSEO API

3. **on_page_get.py**

   - Creates: `parsed_content_markdowns/` (MD files with content), `parsed_content_markdowns/_error_summary.csv`
   - Expects: `queued_tasks/` JSON files from on_page_post.py
   - Does: Fetches parsed results and saves as markdown files

4. **missing_serp_outputs.py** (Optional)
   - Creates: Missing files in `parsed_content_markdowns/`
   - Expects: `parsed_content_markdowns/_error_summary.csv` from on_page_get.py
   - Does: Handles missing or failed requests

### Phase 3: Quality Control & Fixing

5. **error-critical.py** ✅ Should run FIRST (uses files from on_page_get.py)

   - Creates: `parsed_content_markdowns2/` folder with retry results, `parsed_content_markdowns2/_retry_organic_report.csv`
   - Expects: `parsed_content_markdowns/_error_summary.csv` from on_page_get.py
   - Does: Retries only CRITICAL & ERROR entries marked as ORGANIC type
   - Note: **This CREATES parsed_content_markdowns2 folder**

6. **check_files_size.py** (Optional audit tool)
   - Creates: `low_quality_content_report.csv`
   - Expects: `parsed_content_markdowns2/` folder (created by error-critical.py)
   - Does: Scans markdown files for low quality content (<5KB) - for reporting/analysis only
   - Note: Run this AFTER error-critical.py to see what still needs fixing

### Phase 4: Smart Fixes

7. **smart_fix.py**

   - Creates: Fixed markdown files in `parsed_content_markdowns/` with API rescues
   - Expects: `parsed_content_markdowns/` from on_page_get.py
   - Does: Rescues low-quality/incomplete files from top 10 ranks using DataForSEO API with full settings
   - Min file size target: 10KB
   - Creates: `parsed_content_markdowns/_final_scan_report.csv`

8. **smart_fix_2.py**
   - Creates: Fixed markdown files in `parsed_content_markdowns2/` with API rescues
   - Expects: `parsed_content_markdowns2/` from error-critical.py
   - Does: Rescues low-quality files with LIGHT settings (switch pool only, no JS rendering)
   - Min file size target: 2KB
   - Creates: `_report_parsed_content_2.csv`

### Phase 5: Finalization

9. **merge.py**
   - Creates: `FINAL_DATABASE/` (complete merged database)
   - Expects: `parsed_content_markdowns/` (original) and `parsed_content_markdowns2/` (fixed) folders
   - Does: Merges original data with fixed/retried data into final output

## CORRECTED ORDER

```
main.py
  ↓
on_page_post.py
  ↓
on_page_get.py
  ↓
[missing_serp_outputs.py - optional]
  ↓
error-critical.py ← Creates parsed_content_markdowns2
  ↓
check_files_size.py ← Optional: audit parsed_content_markdowns2
  ↓
smart_fix.py ← Fix files in parsed_content_markdowns (original)
  ↓
smart_fix_2.py ← Fix files in parsed_content_markdowns2 (retried)
  ↓
merge.py ← Final integration into FINAL_DATABASE
```

## Key Points

- **check_files_size.py** scans `parsed_content_markdowns2` which is CREATED by **error-critical.py**
- error-critical.py must run BEFORE check_files_size.py
- **smart_fix.py** exists and rescues low-quality files in `parsed_content_markdowns` (original)
- **smart_fix_2.py** exists and rescues low-quality files in `parsed_content_markdowns2` (from error-critical retries)
- Both smart_fix files use DataForSEO API to re-fetch content for missing/incomplete files
