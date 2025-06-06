from time import time

TIMEFRAMES = {
    '1m': 1,
    '3m': 3,
    '5m': 5,
    '15m': 15,
    '30m': 30,
    '1h': 60,
    '2h': 120,
    '4h': 240,
    '6h': 360,
    '12h': 720,
    '1d': 1440,
    '3d': 4320,
    '1w': 10080,
    '1M': 43200}


def timeframe_to_timestamp(timeframe):
    #  tf in minutes
    return timeframe * 60


def time_before(seconds):
    return int(round((time.time() - seconds) * 1000))

