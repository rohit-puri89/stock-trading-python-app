import time
from datetime import datetime
from zoneinfo import ZoneInfo

from script import run_ticker_export

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
TARGET_HOUR = 9
TARGET_MINUTE = 0


def run_scheduler() -> None:
    last_run_date = None

    while True:
        now_pt = datetime.now(PACIFIC_TZ)
        run_time_reached = (now_pt.hour, now_pt.minute) >= (TARGET_HOUR, TARGET_MINUTE)

        if run_time_reached and last_run_date != now_pt.date():
            print(f"Starting scheduled run at {now_pt.isoformat()}")
            run_ticker_export()
            last_run_date = now_pt.date()

        time.sleep(30)


if __name__ == "__main__":
    run_scheduler()
