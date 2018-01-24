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
import sqlite3
import glob
import os
import re
import logging

def _get_profile(profile_number):
    resp = requests.get('http://www.argovis.com/catalog/profiles/'+profile_number)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        errResp = "Error: Unexpected response {}".format(resp)
        logging.warning(errResp)
        return errResp
    profile = resp.json()
    return profile

def _get_platform_profiles(platform_number):
    resp = requests.get('http://www.argovis.com/catalog/platforms/'+platform_number)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        errResp = "Error: Unexpected response {}".format(resp)
        logging.warning(errResp)
        return errResp
    platformProfiles = resp.json()
    return platformProfiles

def _parse_into_df(profiles):
    #initialize dict
    try:
        meas_keys = profiles[0]['measurements'][0].keys()
    except TypeError:
        if isinstance(profiles, str):
            logging.warning('profiles are strings. Not going to add df')
        return

        profiles[0]['measurements'][0]
    df = pd.DataFrame(columns=meas_keys)
    for profile in profiles:
        profileDf = pd.DataFrame(profile['measurements'])
        profileDf['cycle_number'] = profile['cycle_number']
        profileDf['profile_id'] = profile['_id']
        profileDf['lat'] = profile['lat']
        profileDf['lon'] = profile['lon']
        profileDf['date'] = profile['date']
        df = pd.concat([df, profileDf])
    
    #  there has to be pressure and temperature columns.
    try:
        if not 'psal' in df.columns:
            df['psal'] = np.NaN
            df['psal_qc'] = np.NaN
        if not 'temp' in df.columns:
            df['temp'] = np.NaN
            df['temp_qc'] = np.NaN
    except:
        pdb.set_trace()
        df.columns
    return df

def _get_selection_profiles(startDate, endDate, shape, presRange=None):
    
    #baseURL = 'http://www.argovis.com/selection/profiles'
    baseURL = 'http://localhost:3000/selection/profiles' #use if running locally
    
    startDateQuery = '?startDate=' + startDate
    endDateQuery = '&endDate=' + endDate
    shapeQuery = '&shape='+shape.replace(' ','')
    if not presRange == None:
        pressRangeQuery = '&presRange=' + presRange
        url = baseURL + startDateQuery + endDateQuery + pressRangeQuery + shapeQuery
    else:
        url = baseURL + startDateQuery + endDateQuery + shapeQuery
    resp = requests.get(url)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    if resp.status_code == 500:
        pdb.set_trace()
        return "Error: 500 status {}".format(resp)
    selectionProfiles = resp.json()
    return selectionProfiles

def _quality_control_df(df, presTH='1', tempTH='1', psalTH='1'):
    try:
        df = df[df['pres_qc'] == presTH ]
        df = df[df['temp_qc'] == tempTH ]
        df = df[df['psal_qc'] == psalTH ]   
    except KeyError:
        pdb.set_trace()
        df.columns         
    return df

def get_ocean_df(oceanFileName, subGrid=None):
    oceanDf = pd.read_csv(oceanFileName)
    oceanDf.set_index('idx', inplace=True)
    if type(subGrid) != type(None):
        oceanDf = oceanDf[(oceanDf.lat >= subGrid['latMin']) &
                          (oceanDf.lat <= subGrid['latMax']) &
                          (oceanDf.lon >= subGrid['lonMin']) &
                          (oceanDf.lon <= subGrid['lonMax'])]
        print('oceanDF searching for {} areas'.format(oceanDf.shape[0]))
    return oceanDf

def get_ocean_df_from_csv(oceanDf, startDate, endDate, presRange, presIntervals, nElem):
    """queries argovis database over large gridded space and small time scales.
    Ocean file name contains coordinates.
    Output is saves as a csv.
    Start date and End date are usually about 10-30 days
    """
    
    for row in oceanDf.itertuples():
        shapeStr = row.polyShape
        selectionProfiles = _get_selection_profiles(startDate, endDate, shapeStr, presRange)
        if len(selectionProfiles) == 0:
            continue
        try:
            df = _parse_into_df(selectionProfiles)
        except TypeError:
            pdb.set_trace()
            logging.warning('Type Error encountered. Shape is: {}. Not going to add'.format(shapeStr))
            continue
        if df.shape[0] == 0: #  move on if selection profiles don't turn up anything.
            continue

        #df = _quality_control_df(df)  #currently only qc values of 1 are included

        # aggregate into pressure intervals            
        for ldx, pres in presIntervals:
            try:
                if df[ (df['pres'] < pres[1]) & (df['pres'] > pres[0])].shape[0] > 0: # sometimes there aren't any profiles at certain depth intervals
                    df.loc[ (df['pres'] <= pres[1]) & (df['pres'] > pres[0]), 'ldx'] = ldx
                    df.loc[ (df['pres'] <= pres[1]) & (df['pres'] > pres[0]), 'presMin'] = pres[0]
                    df.loc[ (df['pres'] <= pres[1]) & (df['pres'] > pres[0]), 'presMax'] = pres[1]
            except ValueError:
                pdb.set_trace()
                logging.warning('Value Error encountered.')
                df[ (df['pres'] < pres[1]) & (df['pres'] > pres[0])].shape[0]
        if not 'ldx' in df.columns:  # sometimes there are no measurements in the given pressure intervals
            continue
        try:
            grouped = df.groupby('ldx')
        except KeyError:
            pdb.set_trace()
            logging.warning('Key Error encountered. Df shape is: {}'.format(df.shape()))
        for ldx, group in grouped:
            nMeas = group.shape[0]
            if nMeas == 0:
                continue
            aggMean = group[['temp', 'psal']].mean()
            idx = row.Index + ldx * nElem
            oceanDf.set_value(idx, 'aggTemp', aggMean.temp)
            oceanDf.set_value(idx, 'aggPsal', aggMean.psal)
            oceanDf.set_value(idx, 'nProf', nMeas)
    oceanDf.dropna(axis=0, how='any', thresh=2, subset=['aggTemp', 'aggPsal', 'nProf'], inplace=True)
    return oceanDf


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

def get_pres_intervals(minPres, maxPres, dPres):
    """
    create a set of pressure intervals with an even distance
    
    """
    presBin = np.arange(minPres, maxPres+dPres, dPres)
    presIntervals = []
    for idx, pres in enumerate(presBin[0:-1]):
        interval = presBin[idx:idx+2]
        presIntervals.append([idx, interval])
    return presIntervals

def get_dates_set(period=12):
    """
    create a set of dates split into n periods
    """
    n_rows = int(np.floor(365/period))
    datesSet = []
    for year in range(2004, 2018):
        yearSet = np.array_split(pd.date_range(str(year)+'-01-01', str(year)+'-12-31'), n_rows)
        datesSet = datesSet + yearSet
    keepEnds = lambda x: [x[0].strftime(format='%Y-%m-%d'), x[-1].strftime(format='%Y-%m-%d')]
    datesSet = list(map(keepEnds, datesSet))
    return datesSet

def merge_csvs():
    """
    merges completed csvs into one. This should be used for 'small' data sets.
    """
    df = pd.DataFrame()
    columnFiles = glob.glob(os.path.join(os.getcwd(), 'out', '*.csv'))
    columnFiles = [x for x in columnFiles if re.search(r'column', x)]
    for file in columnFiles:
        colDf = pd.read_csv(file)
        colDf.set_index('idx', inplace=True)
        df = pd.concat([df, colDf], axis = 1)
    return df

def get_space_time_dates():
    """
    Breaks year up into 37 segments. 
    The first 36 segments are ten day chunks, 
    followed by a 5 day chunk for 12-27 to 12-31.
    Leap years have an additional day at the fifth index, the
    02-21 to 03-01 segment.
    """
    datesSet = []
    for year in range(2004, 2018):
        yearSet = np.array_split(pd.date_range(str(year)+'-01-01', str(year)+'-12-26'), 36)
        yearSet.append(pd.date_range(str(year)+'-12-27', str(year)+'-12-31'))
        datesSet = datesSet + yearSet
    keepEnds = lambda x: [x[0].strftime(format='%Y-%m-%d'), x[-1].strftime(format='%Y-%m-%d')]
    datesSet = list(map(keepEnds, datesSet))
    return datesSet 
        
if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='verdziAPI.log',
                        level=logging.WARNING)
    oceanFileName = 'out/grid-coords/oceanCoordsAtOneDeg.csv'
    nElem = 180*360
    presRange = '[0, 120]'
    startDate='2017-10-15'
    endDate='2017-10-30'
    minPres = 0
    maxPres = 30
    dPres = 30

    #df = merge_csvs()
    #df.to_csv('southPac.csv')
    #make csv of globe for small date range
    #oceanDf = get_ocean_csv(oceanFileName, startDate, endDate, minPres, maxPres, dPres, nElem)
    #oceanDf.to_csv('out/df_0-1500_2017-10-15to2017-10-15_oneDeg.csv')
    #make timeseries with periods of every n days
    #shape = '[[[-18.6,31.7],[-18.6,37.7],[-5.9,37.7],[-5.9,31.7],[-18.6,31.7]]]'
    #tsDf = get_ocean_time_series('2004-01-01', '2017-12-31', shape, presRange='[0, 30]')
    #tsDf.to_csv('out/gilbralter.csv')
    #tsDf['tempMean'].plot()
    #tsDf['psalMean'].plot()
"""    
    #make as set of csvs with time index
    start = datetime.now()
    #connex = sqlite3.connect("out/oneDeg12DayAvg.db")
    
    datesSet = get_dates_set(period=30)
    print('number of dates: {}'.format(len(datesSet)))
    
    presIntervals = get_pres_intervals(minPres, maxPres, dPres)
    subGrid = {'latMin': -40, 'latMax': 15, 'lonMin': -178, 'lonMax': -95}
    oceanDfCoords = get_ocean_df(oceanFileName, subGrid)
    
    largeDf = pd.DataFrame()
    for tdx, dates in enumerate(datesSet):
        #if tdx >= 59: 
        #    continue
        startDate, endDate = dates
        oceanDf = get_ocean_df_from_csv(oceanDfCoords, startDate, endDate, presRange, presIntervals, nElem)
        aggDf = oceanDf[['aggTemp','aggPsal', 'nProf']]
        aggDf.columns = ['T'+str(tdx), 'S'+str(tdx), 'n'+str(tdx)]
        largeDf = pd.concat([largeDf, aggDf], axis = 1)
        
        #make new database will merge tables later...
        #connex = sqlite3.connect("out/column_tdx_" + str(tdx) + ".db")
        #aggDf.to_sql(name='data', con=connex, if_exists="replace", index=True)
        
        #make a csv
        aggDf.to_csv("out/column_tdx_" + str(tdx) + ".csv")
        print('time index: {}'.format(tdx))
        timeTick = datetime.now()
        print(timeTick.strftime(format='%Y-%m-%d %H:%M'))
        dt = timeTick-start
        print('running for: {}'.format(dt))
    largeDf.to_csv("southPacData.csv")
    
    
    #make sparse matrix column
"""    
        