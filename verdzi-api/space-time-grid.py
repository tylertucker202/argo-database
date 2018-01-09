#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 09:56:36 2018

@author: gstudent4
"""

import verdziAPI as ver
import pandas as pd
from datetime import datetime

if __name__ == '__main__':
    start = datetime.now()
    oceanFileName = 'out/grid-coords/oceanCoordsAtQuarterDeg.csv'
    nElem = 720*1440
    presRange = '[0, 5500]' #used to query database
    presIntervals = [[0, [0, 10]], 
                    [1, [10, 20]], 
                    [2, [20, 30]], 
                    [3, [30, 50]], 
                    [4, [50, 75]], 
                    [5, [75, 100]],
                    [6, [100, 125]], 
                    [7, [125, 150]], 
                    [8, [150, 200]], 
                    [9, [200, 250]], 
                    [10, [250, 300]], 
                    [11, [300, 400]], 
                    [12, [400, 500]], 
                    [13, [500, 600]], 
                    [14, [600, 700]], 
                    [15, [700, 800]], 
                    [16, [800, 900]], 
                    [17, [900, 1000]], 
                    [18, [1000, 1100]], 
                    [19, [1100, 1200]], 
                    [20, [1200, 1300]], 
                    [21, [1300, 1400]], 
                    [22, [1400, 1500]], 
                    [23, [1500, 1750]], 
                    [24, [1750, 2000]], 
                    [25, [2000, 2500]], 
                    [26, [2500, 3000]], 
                    [27, [3000, 3500]], 
                    [28, [3500, 4000]], 
                    [29, [4000, 4500]], 
                    [30, [4500, 5000]], 
                    [31, [5000, 5500]]]
    datesSet = ver.get_space_time_dates()
    print('should be 37: {}'.format(len(datesSet[0:37])))
    print('should be 37: {}'.format(len(datesSet[-37:])))
    oceanDfCoords = ver.get_ocean_df(oceanFileName)
    for tdx, dates in enumerate(datesSet):
        #if job breaks at a certain point, use continueAtIdx to skip what has already been created. 
        continueAtIdx = 9999
        if tdx >= continueAtIdx:
           continue
        startDate, endDate = dates
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
        print('time index: {}'.format(tdx))
        timeTick = datetime.now()
        print(timeTick.strftime(format='%Y-%m-%d %H:%M'))
        dt = timeTick-start
        print('running for: {}'.format(dt))