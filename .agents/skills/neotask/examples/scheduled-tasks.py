"""TaskScheduler delayed, periodic, and cron tasks."""
import time
from neotask import TaskScheduler


async def my_task(data):
    print(f"Executing: {data}")
    return {"done": True}


def main():
    scheduler = TaskScheduler(executor=my_task)

    # Delayed — run after 2 seconds
    scheduler.submit_delayed({"type": "delayed"}, delay_seconds=2)

    # Periodic — every 3 seconds
    scheduler.submit_interval({"type": "periodic"}, interval_seconds=3)

    # Cron — every minute
    scheduler.submit_cron({"type": "cron"}, "*/1 * * * *")

    time.sleep(10)
    scheduler.shutdown()


if __name__ == "__main__":
    main()
