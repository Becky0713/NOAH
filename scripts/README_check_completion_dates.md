# Check Completion Dates Script

This script checks the database to see if we have housing projects that are:
- 10+ years old (completed before 2015)
- 20+ years old (completed before 2005)
- 50+ years old (completed before 1985)

## How to Run

### Option 1: Using Environment Variables

Create a `.env` file in the project root with:
```
DB_HOST=your_host
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_password
```

Then run:
```bash
python3 scripts/check_completion_dates.py
```

### Option 2: Using Command Line Arguments

```bash
python3 scripts/check_completion_dates.py \
  --db-host your_host \
  --db-port 5432 \
  --db-name your_db_name \
  --db-user your_db_user \
  --db-password your_password
```

### Option 3: Using Streamlit Secrets (if available)

If you have `.streamlit/secrets.toml` configured, the script will try to read from it.

## What It Does

1. Counts total records in the database
2. Checks how many records have `building_completion_date` vs `project_completion_date`
3. Counts how many projects fall into each age category (10+, 20+, 50+ years)
4. Shows date ranges for both completion date fields
5. Displays sample records for each age category

## Output

The script will show:
- Total number of records
- How many have completion dates
- Count for each age category
- Date ranges
- Sample records for each category

This helps determine if there's enough data to implement the age-based filters.

