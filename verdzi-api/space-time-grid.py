#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 09:56:36 2018

@author: gstudent4
"""

import verdziAPI as ver
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
    oceanFileName = 'out/grid-coords/oceanCoordsAtQuarterDeg.csv'
    nElem = 720*1440
    presRange = '[0, 5500]' #used to query database
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
                    [1750,2000], 
                    [2000,2500], 
                    [2500,3000], 
                    [3000,3500], 
                    [3500,4000], 
                    [4000,4500], 
                    [4500,5000], 
                    [5000,5500]]
    presIntervals = []
    for idx, interval in enumerate(intervals):
        presIntervals.append([idx, interval])
    datesSet = ver.get_space_time_dates()
    print('should be 37: {}'.format(len(datesSet[0:37])))
    print('should be 37: {}'.format(len(datesSet[-37:])))
    oceanDfCoords = ver.get_ocean_df(oceanFileName)
    oceanDfCoords['polyShape'] = oceanDfCoords['polyShape'].apply(lambda x: x.replace(' ',''))
    oceanDfCoords['polyShape'] = oceanDfCoords['polyShape'].apply(lambda x: '['+x+']')

    for tdx, dates in enumerate(datesSet):
        #if job breaks at a certain point, use continueAtIdx to skip what has already been created. 
        continueAtIdx = 4
        if tdx < continueAtIdx:
           continue
        print('time index: {}'.format(tdx))
        startDate, endDate = dates
        try:
            oceanDf = ver.get_ocean_df_from_csv(oceanDfCoords,
                                                startDate,
                                                endDate,
                                                presRange,
                                                presIntervals,
                                                nElem)
            aggDf = oceanDf[['aggTemp','aggPsal', 'nProf']]
            aggDf.columns = ['T'+str(tdx), 'S'+str(tdx), 'n'+str(tdx)]
            #make a csv
            aggDf.to_csv("out/space-time-grid/column_tdx_" + str(tdx) + ".csv")
        except:
            pdb.set_trace()
            logging.warning('Somthing broke for tdx:{}. File not created'.format(tdx))
            pass
        print('time index: {}'.format(tdx))
        timeTick = datetime.now()
        print(timeTick.strftime(format='%Y-%m-%d %H:%M'))
        dt = timeTick-start
        print('running for: {}'.format(dt))