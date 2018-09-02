#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug  5 11:44:42 2018

@author: tyler
"""
import os
import glob
import pdb
import logging
from numpy import ma as ma
from scipy.io import savemat
from openArgoNC import openArgoNcFile

def get_file_names(mirrorPath, regexFlag=True):

    pathFiles = os.path.join(os.getcwd(), 'argovis-mat-python')
    pathFlags = os.path.join(os.getcwd(), 'argovis-data')
    filenameList = os.path.join(pathFlags, 'prof_fname_mirror.txt')
    if regexFlag:
        dacs = glob.glob(os.path.join(mirrorPath, '*'))
        file_names = []
        for dac in dacs:
            fileNames = file_names+glob.glob(os.path.join(mirrorPath, dac, '**', 'profiles', '*.nc'))
    else:
        with open(filenameList) as f:
            fileNames = f.readlines()
        fileNames = [mirrorPath + os.sep + x.strip('\n') for x in fileNames]
    return fileNames, pathFiles, mirrorPath, pathFlags, filenameList


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                    filename='matFlagScanner.log',
                    level=logging.DEBUG)
    localDir = os.getcwd()
    import os
    myhost = os.uname()[1]
    if myhost == 'carby':
        logging.debug('in carby')
        mirrorPath = os.path.join(localDir, os.pardir, 'troublesome-files')
    elif myhost == 'kadavu.ucsd.edu':
        logging.debug('in kadavu')
        mirrorPath = os.path.join(os.sep, 'home', 'tylertucker', 'ifremer')
    else:
        logging.debug('hostname unrecognized')
        mirrorPath = os.path.join(os.sep, 'home', 'tylertucker', 'ifremer')

    logging.debug('starting argoNCSandbox')
    fileNames, filePaths, argoMirrorPath, flagPaths, filenameList = get_file_names(mirrorPath, True)
    flagVariables = ['flag_bad_pos_time','flag_bad_data','flag_bad_TEMP','flag_bad_PSAL','flag_no_file',]
    flags = {}
    for flagVar in flagVariables:
        flags[flagVar] = []
    
    myArgoNCHelper = openArgoNcFile()

    for idx, file in enumerate(fileNames):
        fileName = os.path.join(argoMirrorPath, file)
        logging.debug(file)
        try:
            myArgoNCHelper.create_profile_data_if_exists(fileName)
            ncFile = myArgoNCHelper.get_profile_data()
        except Exception as err:
            logging.warning('File: {0} not included. Reason: {1}'.format(file, err))
        pdb.set_trace()
        
        for flagVar in flagVariables:
            flags[flagVar].append(ncFile[flagVar])
            if (ncFile[flagVar] !=0 ):
                logging.debug(flagVar+' = '+str(ncFile[flagVar]))

        if (ma.remainder(idx+1,100000)==0):
            logging.debug(str(idx/len(fileNames)*100) + '% done')
            # save the flags
            savemat(os.path.join(flagPaths,'open_Argo_nc_flags.mat'), flags, oned_as='row')
    # save at the end
    savemat(os.path.join(flagPaths,'open_Argo_nc_flags.mat'), flags, oned_as='row')
    
