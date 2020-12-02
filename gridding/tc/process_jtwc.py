import pandas as pd
from jtwc_tools import *
import numpy as np

def safeSubset(lst, idx):
    if len(lst) <= idx:
        return np.nan
    else:
        return str(lst[idx])

def getLat(lst):
    rawlat = safeSubset(lst, 6)
    if rawlat[-1] == 'N':
        sign = 1
    else:
        sign = -1
    return float(rawlat[:-1]) / 10 * sign

def getLon(lst):
    rawlon = safeSubset(lst, 7)
    if rawlon[-1] == 'W':
        sign = -1
    else:
        sign = 1
    return float(rawlon[:-1]) / 10 * sign

def getTime(lst):
    timestamp = safeSubset(lst, 2)
    return timestamp[-3:-1] + '00'

def getDate(lst):
    ts = safeSubset(lst, 2)
    return '-'.join([ts[:4], ts[4:6], ts[6:8]])

def getL(lst):
    return np.nan

def getClass(lst):
    return safeSubset(lst, 10)

def getSeason(lst):
    ts = safeSubset(lst, 2)
    return ts[:4]

def getNum(lst):
    rawnum = safeSubset(lst, 1)
    return int(rawnum)

def getID(lst):
    season = getSeason(lst)
    basin = safeSubset(lst, 0)
    num = getNum(lst)
    return basin + f'{num:02d}' + season

def getTimestamp(lst):
    ts = safeSubset(lst, 2)
    year = int(ts[:4])
    month = int(ts[4:6])
    day = int(ts[6:8])
    hour = int(ts[8:10])
    return pd.Timestamp(
            year=year,
            month=month,
            day=day,
            hour=hour,
            )

def getWind(lst):
    return int(safeSubset(lst, 8))

def getPress(lst):
    return int(safeSubset(lst, 9))


def convert_df(df):
    n, _ = df.shape
    df = pd.DataFrame({
        'ID':       df.apply(lambda r: getID(r), axis=1),           # ID
        'NAME':     ['UNNAMED' for _ in range(n)],                  # NAME
        'DATE':     df.apply(lambda r: getDate(r), axis=1),         # DATE
        'TIME':     df.apply(lambda r: getTime(r), axis=1),         # TIME
        'L':        ['  ' for _ in range(n)],                       # L
        'CLASS':    df.apply(lambda r: getClass(r), axis=1),        # CLASS
        'LAT':      df.apply(lambda r: getLat(r), axis=1),          # LAT
        'LONG':     df.apply(lambda r: getLon(r), axis=1),          # LONG
        'WIND':     df.apply(lambda r: getWind(r), axis=1),         # WIND
        'PRESS':    df.apply(lambda r: getPress(r), axis=1),        # PRESS
        'SEASON':   df.apply(lambda r: getSeason(r), axis=1),       # SEASON
        'NUM':      df.apply(lambda r: getNum(r), axis=1),          # NUM
        'TIMESTAMP':df.apply(lambda r: getTimestamp(r), axis=1),    # TIMESTAMP
        }).drop_duplicates()
    if not df['TIMESTAMP'].is_unique:
        raise ValueError('Duplicate rows')
    return df
