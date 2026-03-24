import requests
from dotenv import load_dotenv
import os
import time
from collections import deque
import snowflake.connector
from datetime import datetime


load_dotenv()
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")
if not MASSIVE_API_KEY:
    raise RuntimeError("Missing required environment variable: MASSIVE_API_KEY")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "lwc19610.us-east-1")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DATAEXPERTIO_BEGINNER_BOOTCAMP")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "SCH_STOCK_TICKER")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "TICKER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "MY_WAREHOUSE")

if not SNOWFLAKE_USER:
    raise RuntimeError("Missing required environment variable: SNOWFLAKE_USER")
if not SNOWFLAKE_PASSWORD:
    raise RuntimeError("Missing required environment variable: SNOWFLAKE_PASSWORD")

LIMIT = 1000
CALLS_PER_WINDOW = 5
SLEEP_SECONDS = 65
WINDOW_SECONDS = 60
MAX_429_RETRIES = 5

api_call_count = 0
call_timestamps = deque()


def fetch_json(url: str) -> dict:
    global api_call_count
    while True:
        now = time.time()
        while call_timestamps and now - call_timestamps[0] >= WINDOW_SECONDS:
            call_timestamps.popleft()

        if len(call_timestamps) >= CALLS_PER_WINDOW:
            wait_time = (call_timestamps[0] + WINDOW_SECONDS) - now + 1
            wait_time = max(wait_time, 1)
            print(f"Reached {CALLS_PER_WINDOW} calls/min window. Sleeping {int(wait_time)}s...")
            time.sleep(wait_time)
            continue

        retry_count = 0
        while retry_count <= MAX_429_RETRIES:
            response = requests.get(url, timeout=30)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                backoff_seconds = int(retry_after) if retry_after and retry_after.isdigit() else SLEEP_SECONDS
                retry_count += 1
                if retry_count > MAX_429_RETRIES:
                    response.raise_for_status()
                print(f"429 rate limit hit. Retry {retry_count}/{MAX_429_RETRIES} in {backoff_seconds}s...")
                time.sleep(backoff_seconds)
                continue

            response.raise_for_status()
            data = response.json()
            api_call_count += 1
            call_timestamps.append(time.time())
            if api_call_count % CALLS_PER_WINDOW == 0:
                print(f"Made {api_call_count} API calls so far.")
            return data

def fetch_all_tickers() -> list[dict]:
    url = (
        f"https://api.massive.com/v3/reference/tickers?"
        f"market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={MASSIVE_API_KEY}"
    )
    data = fetch_json(url)
    ticker_list = []

    for ticker_item in data["results"]:
        ticker_list.append(ticker_item)

    while "next_url" in data:
        data = fetch_json(data["next_url"] + f"&apiKey={MASSIVE_API_KEY}")
        for ticker_item in data["results"]:
            ticker_list.append(ticker_item)

    return ticker_list

def write_tickers_to_snowflake(ticker_list: list[dict]) -> int:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        TICKER STRING,
        NAME STRING,
        MARKET STRING,
        LOCALE STRING,
        PRIMARY_EXCHANGE STRING,
        TYPE STRING,
        ACTIVE BOOLEAN,
        CURRENCY_NAME STRING,
        CIK STRING,
        LAST_UPDATED_UTC STRING,
        INSERT_TIMESTAMP TIMESTAMP_NTZ
    )
    """
    alter_last_updated_sql = f"""
    ALTER TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    ALTER COLUMN LAST_UPDATED_UTC SET DATA TYPE TIMESTAMP_TZ
    """
    add_insert_timestamp_column_sql = f"""
    ALTER TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    ADD COLUMN IF NOT EXISTS INSERT_TIMESTAMP TIMESTAMP_NTZ
    """
    truncate_sql = f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        TICKER, NAME, MARKET, LOCALE, PRIMARY_EXCHANGE, TYPE, ACTIVE, CURRENCY_NAME, CIK, LAST_UPDATED_UTC, INSERT_TIMESTAMP
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
    """

    rows = [
        (
            ticker_item.get("ticker"),
            ticker_item.get("name"),
            ticker_item.get("market"),
            ticker_item.get("locale"),
            ticker_item.get("primary_exchange"),
            ticker_item.get("type"),
            ticker_item.get("active"),
            ticker_item.get("currency_name"),
            ticker_item.get("cik"),
            ticker_item.get("last_updated_utc"),
        )
        for ticker_item in ticker_list
    ]

    with snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"ALTER WAREHOUSE {SNOWFLAKE_WAREHOUSE} RESUME")
            cur.execute(create_table_sql)
            cur.execute(add_insert_timestamp_column_sql)
            cur.execute(truncate_sql)
            cur.executemany(insert_sql, rows)

    return len(rows)


def run_ticker_export() -> int:
    ticker_list = fetch_all_tickers()
    written_rows = write_tickers_to_snowflake(ticker_list)
    print(
        f"Wrote {written_rows} rows to "
        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
    )
    return written_rows


if __name__ == "__main__":
    run_ticker_export()