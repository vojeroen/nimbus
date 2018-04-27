import datetime

import pytz


def decode(obj):
    if isinstance(obj, bytes):
        return obj.decode()
    elif isinstance(obj, list):
        return [decode(o) for o in obj]
    elif isinstance(obj, dict):
        return dict([(decode(k), decode(v)) for k, v in obj.items()])
    else:
        return obj


def extract_source_from_message(message):
    empty = message.index(b'')
    return message[0:empty]


def extract_content_from_message(message):
    empty = message.index(b'')
    content = message[empty + 1:]
    assert len(content) == 1
    return content[0]


def unix_to_ts(unix_timestamp):
    return pytz.utc.localize(datetime.datetime.utcfromtimestamp(unix_timestamp))


def ts_to_unix(timestamp):
    diff = timestamp - pytz.utc.localize(datetime.datetime(1970, 1, 1))
    return int(diff.total_seconds())


def get_utc_int():
    return ts_to_unix(pytz.utc.localize(datetime.datetime.utcnow()))
