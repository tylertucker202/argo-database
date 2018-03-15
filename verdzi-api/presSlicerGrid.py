#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 13:44:17 2018

@author: tyler
"""
import requests
import pandas as pd
import numpy as np
import pdb

def create_idx(lat, lon, layer, delta):
    row = (180 - lat) / delta
    col = (360 - lon) / delta
    idx = (1 + row)*col*layer
    return idx

def agg_gridded(df, measKeys):
    #group and aggregate over latbin and lonbin
    aggDf = pd.DataFrame()
    grouped = df.groupby(level=['latbin', 'lonbin'])
    for tdx, group in grouped:
        nMeas = group.shape[0]
        gridIdx = group['idx'].values[0]
        latbin, lonbin = tdx
        if nMeas == 0:
            continue
        try:
            aggMean = group[group != -999].mean()
            aggStd = group[group != -999].std()
        except KeyError:
            pdb.set_trace()
            grouped.columns
        for key in measKeys:
            if not key in group.columns:
                continue
            aggDf.at[gridIdx, key+'Mean'] = aggMean[key]
            aggDf.at[gridIdx, key+'Std'] = aggStd[key]
        aggDf.at[gridIdx, 'nProf'] = nMeas
        aggDf.at[gridIdx, 'latbin'] = latbin
        aggDf.at[gridIdx, 'lonbin'] = lonbin
    return aggDf

def bin_layer_df(df, delta, layer):
    to_bin = lambda x: np.floor(x / delta) * delta
    df['latbin'] = df.lat.map(to_bin)
    df['lonbin'] = df.lon.map(to_bin)
    df['idx'] = create_idx(df['latbin'], df['lonbin'], layer, delta).astype('int')
    df.set_index(['latbin', 'lonbin'], inplace=True)
    return df

def get_ocean_slice(startDate, endDate, presRange='[0,30]'):
    baseURL = 'http://www.argovis.com/gridding/presSlice/'
    startDateQuery = '?startDate=' + startDate
    endDateQuery = '&endDate=' + endDate
    presRangeQuery = '&presRange=' + presRange
    url = baseURL + startDateQuery + endDateQuery + presRangeQuery
    resp = requests.get(url)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    oceanSliceProfiles = resp.json()
    return oceanSliceProfiles