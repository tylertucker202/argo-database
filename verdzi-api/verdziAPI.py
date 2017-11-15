#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  4 21:28:22 2017

@author: tyler
"""

import requests
import pandas as pd
import pdb
import numpy as np
from datetime import datetime

def _get_profile(profile_number):
    resp = requests.get('http://www.argovis.com/catalog/profiles/'+profile_number)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    profile = resp.json()
    return profile

def _get_platform_profiles(platform_number):
    resp = requests.get('http://www.argovis.com/catalog/platforms/'+platform_number)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    platformProfiles = resp.json()
    return platformProfiles

def _parse_into_df(profiles):
    #initialize dict
    meas_keys = profiles[0]['measurements'][0].keys()
    df = pd.DataFrame(columns=meas_keys)
    for profile in profiles:
        profileDf = pd.DataFrame(profile['measurements'])
        profileDf['cycle_number'] = profile['cycle_number']
        profileDf['profile_id'] = profile['_id']
        profileDf['lat'] = profile['lat']
        profileDf['lon'] = profile['lon']
        profileDf['date'] = profile['date']
        df = pd.concat([df, profileDf])
    return df

def _get_selection_profiles(startDate, endDate, shape, presRange=None):
    baseURL = 'http://www.argovis.com/selection/profiles'
    
    startDateQuery = '?startDate=' + startDate
    endDateQuery = '&endDate=' + endDate
    shapeQuery = '&shape='+shape
    if not presRange == None:
        pressRangeQuery = '&presRange=' + presRange
        url = baseURL + startDateQuery + endDateQuery + pressRangeQuery + shapeQuery
    else:
        url = baseURL + startDateQuery + endDateQuery + shapeQuery
    resp = requests.get(url)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    selectionProfiles = resp.json()
    return selectionProfiles

def _quality_control_df(df, presTH=2, tempTH=2, psalTH=2):
    df[['pres_qc','psal_qc', 'temp_qc']] = df[['pres_qc','psal_qc', 'temp_qc']].astype(int)
    # quality control
    df = df[df['pres_qc'] < 2]
    df = df[df['temp_qc'] < 2]
    df = df[df['psal_qc'] < 2]            
    return df

def _get_ocean_df_from_csv(oceanFileName, presRange, presIntervals, startDate, endDate, nElem):
    """queries database with shapes found in oceanFileName. Appends an 
    aggregated temperature and salinity mean, along with a profile count
    in each shape. Used to generate sparse matricies for SVD.
    """
    oceanDf = pd.read_csv(oceanFileName)
    oceanDf.set_index('idx', inplace=True)

    for row in oceanDf.itertuples():
        shapeStr = '['+row.shape+']'

        selectionProfiles = _get_selection_profiles(startDate, endDate, shapeStr, presRange)
        if len(selectionProfiles) == 0:
            continue
        df = _parse_into_df(selectionProfiles)
        df = _quality_control_df(df)
        if df.shape[0] == 0: #  move on if qc removes everything
            continue
        # aggregate into pressure intervals            
        for ldx, pres in presIntervals:
            try:
                if df[ (df['pres'] < pres[1]) & (df['pres'] > pres[0])].shape[0] > 0: # sometimes there aren't any profiles at certain depth intervals
                    df.loc[ (df['pres'] <= pres[1]) & (df['pres'] > pres[0]), 'ldx'] = ldx
                    df.loc[ (df['pres'] <= pres[1]) & (df['pres'] > pres[0]), 'presMin'] = pres[0]
                    df.loc[ (df['pres'] <= pres[1]) & (df['pres'] > pres[0]), 'presMax'] = pres[1]
            except ValueError:
                pdb.set_trace()
                df[ (df['pres'] < pres[1]) & (df['pres'] > pres[0])].shape[0]
        try:
            grouped = df.groupby('ldx')
        except KeyError:
            pdb.set_trace()
            df.shape
        for ldx, group in grouped:
            nMeas = group.shape[0]
            if nMeas == 0:
                continue
            aggMean = group[['temp', 'psal']].mean()
            idx = row.Index + ldx * nElem
            oceanDf.set_value(idx, 'aggTemp', aggMean.temp)
            oceanDf.set_value(idx, 'aggPsal', aggMean.psal)
            oceanDf.set_value(idx, 'nProf', nMeas)
    pdb.set_trace()
    oceanDf.dropna(axis=0, how='any', thresh=2, subset=['aggTemp', 'aggPsal', 'nProf'], inplace=True)
    return oceanDf

def get_ocean_csv(oceanFileName, startDate, endDate, minPres, maxPres, dPres):
    """queries argovis database over large gridded space and small time scales.
    Ocean file name contains coordinates.
    Output is saves as a csv.
    Start date and End date are usually about 10-30 days
    """
    presBin = np.arange(minPres, maxPres+dPres, dPres)
    presIntervals = []
    for idx, pres in enumerate(presBin[0:-1]):
        interval = presBin[idx:idx+2]
        presIntervals.append([idx, interval])

    queryDf = _get_ocean_df_from_csv(oceanFileName, presRange, presIntervals, startDate, endDate, nElem)
    queryDf.to_csv('df_0-1500_2017-10-15to2017-10-15_oneDeg.csv')


def get_ocean_time_series(seriesStartDate, seriesEndDate, shape, presRange='[0, 30]'):
    """Queries argovis database over long time scales but over one shape.
    shape should have a radius of no more than a few degrees.
    Pressure range should about 10-50 dbar.
    """
    
    selectionProfiles = _get_selection_profiles(seriesStartDate, seriesEndDate, shape, presRange)
    if len(selectionProfiles) == 0:
        return
    df = _parse_into_df(selectionProfiles)
    df = _quality_control_df(df)
    
    #provide a time index for date ranges
    datesSet = get_dates_set(12)
    dateFormat = '%Y-%m-%dT%H:%M:%S.%fZ'
    df['date'] =  pd.to_datetime(df['date'], format=dateFormat)
    for idx, dates in enumerate(datesSet):
        startDate = datetime.strptime(dates[0], '%Y-%m-%d')
        endDate = datetime.strptime(dates[1], '%Y-%m-%d')
        try:
            if df[ (df['date'] < endDate) & (df['date'] > startDate)].shape[0] > 0: # sometimes there aren't any profiles at certain depth intervals
                df.loc[ (df['date'] <= endDate) & (df['date'] > startDate), 'tIndex'] = idx
                df.loc[ (df['date'] <= endDate) & (df['date'] > startDate), 'startDate'] = startDate
                df.loc[ (df['date'] <= endDate) & (df['date'] > startDate), 'endDate'] = endDate
        except ValueError:
            pdb.set_trace()
            df[ (df['date'] < startDate) & (df['date'] > endDate)].shape[0]

    df = df[np.isfinite(df['tIndex'])]
    df.dropna(axis=1, how='any', inplace=True)
    #group and aggregate over tIndex
    tsDf = pd.DataFrame()
    grouped = df.groupby('tIndex')

    for tdx, group in grouped:
        nMeas = group.shape[0]
        if nMeas == 0:
            continue
        aggMean = group[['temp', 'psal']].mean()
        aggStd = group[['temp', 'psal']].std()
        startDate = group['startDate'].values[0]
        endDate = group['endDate'].values[0]
        tsDf.set_value(tdx, 'tempMean', aggMean.temp)
        tsDf.set_value(tdx, 'psalMean', aggMean.psal)
        tsDf.set_value(tdx, 'tempStd', aggStd.temp)
        tsDf.set_value(tdx, 'psalStd', aggStd.psal)
        tsDf.set_value(tdx, 'startDate', startDate)
        tsDf.set_value(tdx, 'endDate', endDate)
        tsDf.set_value(tdx, 'nProf', nMeas)
    
    

    return tsDf
            

        
def get_dates_set(period=12):
    n_rows = int(np.floor(365/period))
    datesSet = []
    for year in range(2004, 2018):
        yearSet = np.array_split(pd.date_range(str(year)+'-01-01', str(year)+'-12-31'), n_rows)
        datesSet = datesSet + yearSet
    keepEnds = lambda x: [x[0].strftime(format='%Y-%m-%d'), x[-1].strftime(format='%Y-%m-%d')]
    datesSet = list(map(keepEnds, datesSet))
    return datesSet

if __name__ == '__main__':
    oceanFileName = 'oceanCoordsAtOneDeg.csv'
    nElem = 180*360
    presRange = '[0, 1500]'
    startDate='2017-10-15'
    endDate='2017-10-30'
    minPres = 0
    maxPres = 1500
    dPres = 30
    
    #make timeseries with periods of every n days
    shape = '[[[-18.6,31.7],[-18.6,37.7],[-5.9,37.7],[-5.9,31.7],[-18.6,31.7]]]'
    tsDf = get_ocean_time_series('2004-01-01', '2017-12-31', shape, presRange='[0, 30]')
    
    tsDf['tempMean'].plot()
    tsDf['psalMean'].plot()
    
    #query the data abse
    
    
    #make sparse matrix column
    
        