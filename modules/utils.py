
from datetime import datetime, timedelta

def get_theoretical_scheduled_runs(date: datetime, start_date: datetime, interval: int) -> set[datetime]:
    # f : N -> [date - 24h, date[
    # f(x) = start_date + n*interval
    # interval in miliseconds
    interval //= 1000000
    day_before = date - timedelta(hours=24)
    n1 = (day_before - start_date).total_seconds()//interval
    n2 = (date - start_date).total_seconds()//interval
    lower_bound = start_date + timedelta(seconds=n1*interval)
    upper_bound = start_date + timedelta(seconds=n2*interval)

    if lower_bound < day_before:
        lower_bound += timedelta(seconds=interval)
    if upper_bound >= date:
        upper_bound -= timedelta(seconds=interval)

    date_range = set()
    while lower_bound <= upper_bound:
        date_range.add(lower_bound)
        lower_bound += timedelta(seconds=interval)
    return date_range
