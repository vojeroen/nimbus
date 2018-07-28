import datetime

import pytz


def unix_to_ts(unix_timestamp):
    return pytz.utc.localize(datetime.datetime.utcfromtimestamp(unix_timestamp))


def ts_to_unix(timestamp):
    diff = timestamp - pytz.utc.localize(datetime.datetime(1970, 1, 1))
    return int(diff.total_seconds())


def get_utc_int():
    return ts_to_unix(pytz.utc.localize(datetime.datetime.utcnow()))
