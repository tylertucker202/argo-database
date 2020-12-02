import pandas as pd
import numpy as np

def safeSubset(lst, idx):
    if len(lst) >= idx:
        return np.nan
    else:
        return lst[idx]

def getLat(lst):
    rawlat = safeSubset(lst, 7)
    if rawlat == np.nan:
        return rawlat
    if rawlat[-1] == 'N':
        sign = 1
    else:
        sign = -1
    return float(rawlat[:-1]) / 10 * sign

def getLon(lst):
    rawlon = safeSubset(lst, 8)
    if rawlon == np.nan:
        return rawlon
    if rawlon[-1] == 'W':
        sign = 1
    else:
        sign = -1
    return float(rawlon[:-1]) / 10 * sign

def getTime(lst):
    timestamp = safeSubset(lst, 2)
    if timestamp == np.nan:
        return timestamp
    return timestamp[-3:-1] + '00'

def getDate(lst):
    ts = safeSubset(lst, 2)
    if ts == np.nan:
        return ts
    return '-'.join(ts[:4], ts[4:6], ts[6:8])

def getL(lst):
    return np.nan

def getClass(lst):
    return safeSubset(lst, 10)

def getSeason(lst):
    ts = safeSubset(lst, 2)
    if ts == np.nan:
        return ts
    return ts[:4]

def getNum(lst):
    rawnum = safeSubset(lst, 1)
    return int(rawnum)

def getID(lst):
    season = getSeason(lst)
    basin = safeSubseet(lst, 0)
    num = getNum(lst)
    return basin + num + season

def getTimestamp(lst):
    ts = safeSubset(lst, 2)
    if ts == np.nan:
        return ts
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




