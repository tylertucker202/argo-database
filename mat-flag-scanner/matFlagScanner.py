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
from open_argo_nc import open_Argo_ncfile

def get_file_names(mirrorPath):
    # alternative way to get list of profiles using regex
    '''
    dacs = glob.glob(os.path.join(mirrorPath, '*'))
    file_names = []
    for dac in dacs:
        file_names = file_names+glob.glob(os.path.join(mirrorPath, dac, '**', 'profiles', '*.nc'))
    '''
    # get list of profiles from text file.
    path_files = os.path.join(os.getcwd(), 'argovis-mat-python')
    path_flags = os.path.join(os.getcwd(), 'argovis-data')
    # filelist in input
    filename_list = os.path.join(path_flags, 'prof_fname_mirror.txt')
    with open(filename_list) as f:
        file_names = f.readlines()
    file_names = [mirrorPath + os.sep + x.strip('\n') for x in file_names]
    pdb.set_trace()
    path_argo_mirror = mirrorPath
    return file_names, path_files, path_argo_mirror, path_flags, filename_list


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
    fileNames, filePaths, argoMirrorPath, flagPaths, filenameList = get_file_names(mirrorPath)
    flagVariables = ['flag_bad_pos_time','flag_bad_data','flag_bad_TEMP','flag_bad_PSAL','flag_no_file',]
    flags = {}
    for flagVar in flagVariables:
        flags[flagVar] = []

    for idx, file in enumerate(fileNames):
        logging.debug(file)
        ncFile = open_Argo_ncfile(os.path.join(argoMirrorPath, file), filePaths)
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
    
