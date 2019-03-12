#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  4 20:09:27 2017

@author: tyler
"""

import os
import numpy as np
import pandas as pd
import pdb

def make_ocean_coord_Json(latM, lonM, maskM, percThresh=50.0):
    oceanCoords = []
    it = np.nditer(latM, flags=['multi_index'])
    gridIndex = 0
    gridArray = []
    while not it.finished:
        gridDict = {'idx': gridIndex, 'lon': lonM[it.multi_index], 'lat': latM[it.multi_index]}
        gridArray.append(gridDict)
        if maskM[it.multi_index] < percThresh: #ignore cells with certain number of land mass
            it.iternext()
            gridIndex += 1
            continue
        else:
            #defines polygon from single point. mongoDB queries in lon,lat
            polyShape = [[lonM[it.multi_index] + reso/2., latM[it.multi_index] - reso/2.],
                        [lonM[it.multi_index] + reso/2., latM[it.multi_index] + reso/2.],
                        [lonM[it.multi_index] - reso/2., latM[it.multi_index] + reso/2.],
                        [lonM[it.multi_index] - reso/2., latM[it.multi_index] - reso/2.],
                        [lonM[it.multi_index] + reso/2., latM[it.multi_index] - reso/2.]]
            gridDict['polyShape'] = polyShape
            oceanCoords.append(gridDict)
            it.iternext()
            gridIndex += 1

    outputFileName = 'oceanCoordsAt'+key+'.csv'
    oceanDf = pd.DataFrame(oceanCoords)
    oceanDf.set_index('idx', inplace=True)
    oceanDf.to_csv(outputFileName)

    gridFileName = 'worldGridAt'+key+'.csv'
    gridDf = pd.DataFrame(gridArray)
    gridDf.set_index('idx', inplace=True)
    gridDf.to_csv(gridFileName)
if __name__ == '__main__':
    
    homeDir = os.getcwd()
    gridDir = os.path.join(homeDir, 'lat_lon_grid_coords_xdeg')
    maskDir = os.path.join(homeDir, 'land_ocean_masks_xdeg')
    
    resolutions = {'OneDeg':('1d',1), 'HalfDeg':('hd', .5), 'QuarterDeg':('qd', .25)}
    
    for key in resolutions.keys():
        resoTxt = resolutions[key][0]
        reso = resolutions[key][1]
        latFileName = os.path.join(gridDir, 'lat_grid_coord_'+resoTxt+'.asc')
        lonFileName = os.path.join(gridDir, 'lon_grid_coord_'+resoTxt+'.asc')
        oceanMask = os.path.join(maskDir, 'ocean_percent2_'+resoTxt+'.asc')

        latM = np.loadtxt(latFileName, skiprows=6)
        lonM = np.loadtxt(lonFileName, skiprows=6)
        maskM = np.loadtxt(oceanMask, skiprows=6)
        
        make_ocean_coord_Json(latM, lonM, maskM)

    
