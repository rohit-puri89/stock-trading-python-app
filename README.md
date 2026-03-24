# Stock ticker -> Snowflake

Goal - This project pulls active stock tickers from Massive and loads them into Snowflake once per day.

The main script is `script.py`. It handles API pagination + rate limits, resumes the warehouse, and loads the target table.

## `requirements.txt` description

- `requests` - used to call Massive ticker API endpoints.
- `python-dotenv` - loads local `.env` values into environment variables.
- `snowflake-connector-python` - connects to Snowflake and executes SQL inserts.


## Quick start

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python script.py
```

## Required `.env` values

```env
MASSIVE_API_KEY=...
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=MY_WAREHOUSE
SNOWFLAKE_DATABASE=DATAEXPERTIO_BEGINNER_BOOTCAMP
SNOWFLAKE_SCHEMA=SCH_STOCK_TICKER
SNOWFLAKE_TABLE=ticker
```

## Scheduling options

### 1) Local scheduler

`scheduler.py` runs the load daily at 10:45 Pacific (`America/Los_Angeles`, PST/PDT aware).

```bash
.venv/bin/python scheduler.py
```

Important: this works only while your machine is on and the process is running.

### 2) GitHub Actions (recommended)

Workflow file: `.github/workflows/daily-stock-load.yml`

Add these repository secrets in GitHub Actions settings:

- `MASSIVE_API_KEY`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_TABLE`

Then run the workflow once manually from the Actions tab to confirm everything works.

**Manual vs scheduled:** `Run workflow` always runs the Snowflake load. On the cron schedule, the load runs only when the runner’s Pacific time is **10:45** (so a green workflow can still mean “skipped” if you weren’t in that minute—check the job log for “Execute ticker load”).

## Data notes

- `LAST_UPDATED_UTC` is stored as `TIMESTAMP_NTZ` in the table DDL (values inserted via `TO_TIMESTAMP_TZ`)
- `INSERT_TIMESTAMP` is set at load time using `CURRENT_TIMESTAMP()`

## Quick validation query

```sql
SELECT COUNT(*) AS row_count, MAX(INSERT_TIMESTAMP) AS last_insert_ts
FROM DATAEXPERTIO_BEGINNER_BOOTCAMP.SCH_STOCK_TICKER.TICKER;
```

