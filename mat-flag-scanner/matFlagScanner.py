#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug  5 11:44:42 2018

@author: tyler
"""
import os
import glob
import logging
from numpy import ma as ma
from scipy.io import savemat
from open_argo_nc import open_Argo_ncfile

def dummy_get_file_names(troublePath):
    dacs = glob.glob(os.path.join(troublePath, '*'))
    file_names = []
    for dac in dacs:
        file_names = file_names+glob.glob(os.path.join(troublePath, dac, '**', 'profiles', '*.nc'))
        
    path_files = os.path.join(os.getcwd(), 'argovis-mat-python')
    path_flags = os.path.join(os.getcwd(), 'argovis-data')
    # filelist in input
    filename_list = os.path.join(path_flags, 'prof_fname_mirror.txt')
    path_argo_mirror = troublePath
    
    return file_names, path_files, path_argo_mirror, path_flags, filename_list

if __name__ == '__main__':

    localDir = os.getcwd()
    troublePath = os.path.join(localDir, os.pardir, 'troublesome-files')
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                    filename='matFlagScanner.log',
                    level=logging.DEBUG)
    logging.debug('starting argoNCSandbox')
    fileNames, filePaths, argoMirrorPath, flagPaths, filenameList = dummy_get_file_names(troublePath)
    flagVariables = ['flag_bad_pos_time','flag_bad_data','flag_bad_TEMP','flag_bad_PSAL','flag_no_file',]
    flags = {}
    for flagVar in flagVariables:
        flags[flagVar] = []

    for idx, file in enumerate(fileNames):
        logging.debug(file)
        ncFile = open_Argo_ncfile( os.path.join(argoMirrorPath, file), filePaths)
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
    