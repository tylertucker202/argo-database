#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 09:56:36 2018

@author: gstudent4
"""

import verdziAPI as ver
import presSlicerGrid as ps
import pandas as pd
from datetime import datetime
import logging
import pdb
if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='space-time-grid.log',
                        level=logging.WARNING)
    logging.debug('Start of log file')

    start = datetime.now()
    nElem = 720*1440
    presRange = '[0,5500]' #used to query database
    delta = 1 #delta lat-lon degree
    intervals = [[0,7.5], 
                [7.5,12],
                [12,20], 
                [20,30], 
                [30,50], 
                [50,75], 
                [75,100],
                [100,125], 
                [125,150], 
                [150,200], 
                [200,250], 
                [250,300], 
                [300,400], 
                [400,500], 
                [500,600], 
                [600,700], 
                [700,800], 
                [800,900], 
                [900,1000], 
                [1000,1100], 
                [1100,1200], 
                [1200,1300], 
                [1300,1400], 
                [1400,1500], 
                [1500,1750], 
                [1750,2000]]

    presIntervals = []
    for idx, interval in enumerate(intervals):
        presIntervals.append([idx+1, interval])
    datesSet = ver.get_space_time_dates()
    print('should be 37: {}'.format(len(datesSet[0:37])))
    print('should be 37: {}'.format(len(datesSet[-37:])))


    for tdx, dates in enumerate(datesSet):
        #if job breaks at a certain point, use continueAtIdx to skip what has already been created. 
        continueAtIdx = 165
        if tdx < continueAtIdx:
           continue
        print('time index: {}'.format(tdx))
        startDate, endDate = dates

        colNames = ['lat', 'lon', 'T'+str(tdx), 'Tsd'+str(tdx), 'S'+str(tdx), 'Ssd'+str(tdx), 'n'+str(tdx)]
        df = pd.DataFrame(columns=colNames)
        for layer, presRange in presIntervals:
            presRangeStr = str(presRange).replace(' ','')
            sliceProfiles = ps.get_ocean_slice(startDate, endDate, presRangeStr)
            if len(sliceProfiles) == 0:
                continue
            measKeys = ver.get_platform_measurements(sliceProfiles)
            measKeys = [s for s in measKeys if s != ''] # ignore profiles that dont report anything
            sliceDf = ver.parse_into_df(sliceProfiles)
            sliceDf = ps.bin_layer_df(sliceDf, delta, layer)
            sliceDf = ps.agg_gridded(sliceDf, measKeys)
            aggDf = sliceDf[['latbin', 'lonbin', 'tempMean', 'tempStd', 'psalMean', 'psalStd', 'nProf']]
            aggDf.columns = colNames
            df = pd.concat([df, aggDf], axis = 0)
            #make a csv
        df.to_csv("out/space-time-grid/column_tdx_" + str(tdx) + ".csv")
        print('time index: {}'.format(tdx))
        timeTick = datetime.now()
        print(timeTick.strftime(format='%Y-%m-%d %H:%M'))
        dt = timeTick-start
        print('running for: {}'.format(dt))
