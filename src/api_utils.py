import asyncio
import random
from datetime import datetime, timedelta
from functools import wraps
from json import JSONDecodeError
from time import time
from typing import Optional

import httpx
import pendulum
from pendulum.datetime import DateTime

retry_counter = 0


class RateLimiter:
    def __init__(self, max_calls: int, seconds: int):
        self._max_calls = max_calls
        self._period = seconds
        self._calls = 0
        self._start_time = time()

    async def acquire(self):
        while True:
            current_time = time()
            elapsed = current_time - self._start_time

            if elapsed > self._period:
                self._calls = 0
                self._start_time = current_time

            if self._calls < self._max_calls:
                self._calls += 1
                return
            else:
                await asyncio.sleep(0.1)


def rate_limiter_decorator(rate_limiter):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            await rate_limiter.acquire()
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def async_retry(max_retries=5, retry_delay=10, jitter=0.25):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    response = await func(*args, **kwargs)
                    return response

                except JSONDecodeError as e:
                    print(f"Retrying: {retries + 1} / {max_retries}")
                    print(f"JSONDecodeError: {e}")

                except httpx.ReadTimeout as e:
                    print(f"Retrying: {retries + 1} / {max_retries}")
                    print(f"ReadTimeout: {e}")

                except httpx.ConnectError as e:
                    print(f"Retrying: {retries + 1} / {max_retries}")
                    print(f"ReadTimeout: {e}")

                except httpx.ReadError as e:
                    print(f"Retrying: {retries + 1} / {max_retries}")
                    print(f"ReadTimeout: {e}")

                except httpx.ConnectTimeout as e:
                    print(f"Retrying: {retries + 1} / {max_retries}")
                    print(f"ReadTimeout: {e}")

                except httpx.RemoteProtocolError as e:
                    print(f"Retrying: {retries + 1} / {max_retries}")
                    print(f"ReadTimeout: {e}")

                await asyncio.sleep(retry_delay + random.uniform(-retry_delay * jitter, retry_delay * jitter))
                retries += 1
                global retry_counter
                retry_counter += 1

            raise Exception(f"Failed to fetch data after {max_retries} retries")

        return wrapper

    return decorator


@async_retry(max_retries=5, retry_delay=10, jitter=0.25)
async def post(url: str, headers: dict, json: Optional[dict], timeout: int = 120, verify: bool = True, params: Optional[dict] = None):
    async with httpx.AsyncClient(verify=verify) as client:
        response = await client.post(url=url, headers=headers, json=json, timeout=timeout, params=params)
        return response.json()


@async_retry(max_retries=5, retry_delay=5, jitter=0.25)
async def fetch(url: str, headers: dict, params: Optional[dict] = None, debug: bool = False, timeout: int = 60):
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers, params=params, timeout=timeout)
        if debug:
            print(f"fetched {response.url} - got {response.status_code}")
        return response.json()


def construct_backfill_range_map(start_date: str) -> dict[int, list[tuple[DateTime, DateTime]]]:
    """
    Construct a dictionary that maps every year to a list of month ranges starting from the start date to the end of yesterday.

    >>> backfill_map = construct_backfill_range_map('2022-01-01')
    >>> backfill_map.keys() # dict_keys([2022, 2023, 2024])
    >>> backfill_map[2022] # [(datetime(2022, 1, 1, 0, 0), datetime(2022, 1, 31, 23, 59, 59, 999999)), ...]

    Args:
        start_date (str): The start date in the format 'YYYY-MM-DD'.

    Returns:
        dict[int, list[tuple[str, str]]]: A dictionary that maps every year to a list of month ranges.
    """
    start = pendulum.parse(start_date)

    yesterday = pendulum.yesterday().replace(hour=23, minute=59, second=59, microsecond=999999)

    year_month_ranges = {}

    if isinstance(start, DateTime):
        while start <= yesterday:
            end_of_month = start.end_of("month").replace(hour=23, minute=59, second=59, microsecond=999999)

            if end_of_month > yesterday:
                end_of_month = yesterday

            start_of_month = start.start_of("day")

            year = start.year
            if year not in year_month_ranges:
                year_month_ranges[year] = []

            year_month_ranges[year].append((start_of_month, end_of_month))
            start = end_of_month.add(seconds=1).start_of("day")

        return year_month_ranges
    else:
        raise ValueError("Start date invalid")


def get_yesterdays_date_range() -> tuple[datetime, datetime]:
    """
    Get a tuple for the first and last seconds of yesterday for use in a date range query.

    >>> beginning_of_yesterday = get_yesterdays_date_range()[0]
    >>> end_of_yesterday = get_yesterdays_date_range()[1]

    Returns:
        Tuple[datetime, datetime]: A tuple containing datetime objects for the first and last seconds of yesterday.
    """
    yesterday = datetime.now() - timedelta(days=1)
    return (datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0), datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59))
