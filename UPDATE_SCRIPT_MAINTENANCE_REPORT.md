# Update Script Maintenance Report

Date: 2026-03-04

- Verified that `scripts/population.py` currently fails while loading the projection source URL (`central_trend_2016_base.xlsx`) with HTTP 403 from data.london.gov.uk.
- Confirmed the historical ONS totals source in the same script remains reachable, but the projection source endpoint is blocked/retired for non-browser automated fetches.
- Added first GitHub Actions workflow at `.github/workflows/actions.yml` with schedule + manual dispatch and write permissions.
- No dataset file refresh was committed in this pass because upstream projection fetch is currently unavailable to the existing pipeline.
