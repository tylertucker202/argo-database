#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 15:55:28 2018

@author: tyler
"""

import os
import sys
import pdb
import re
sys.path.append('..')
from argoDatabase import argoDatabase
import unittest
import warnings
import pandas as pd
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class measToDfTest(unittest.TestCase):

    def setUp(self):
        self.OUTPUTDIR = os.path.join(os.getcwd(), 'test-files')
        self.dbName = 'argo-test'
        self.collectionName = 'profiles'
        self.verificationErrors = []
        self.basinFilename = './../basinmask_01.nc'
        self.replaceProfile=False
        self.qcThreshold='1'
        self.dbDumpThreshold=1000
        self.removeExisting=False
        self.testMode=True
        self.addToDb=True
        self.basinFilename=self.basinFilename
        self.ad = argoDatabase(self.dbName,
                          self.collectionName,
                          self.replaceProfile,
                          self.qcThreshold,
                          self.dbDumpThreshold,
                          self.removeExisting,
                          self.testMode,
                          self.basinFilename,
                          self.addToDb)                

    def tearDown(self):
        return
    
    def test_noisy_platform(self):
        #check platform with noisy bgc meas
        platform = ['3901498']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)].head()
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            bgcDf = pd.DataFrame(doc['bgcMeas'])
            self.assertEqual(bgcDf.dropna(axis=1, how='all').shape[1], bgcDf.shape[1], 'should not contain empty columns')
    
    def test_bbp_platform(self):
        #check platform with bbp parameter
        platform = ['5903586']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)].head()
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            self.assertTrue('BBP700' in doc['station_parameters_in_nc'], 'should have bbp700')
            self.assertTrue('DOXY' in doc['station_parameters_in_nc'], 'should have DOXY')
            self.assertTrue('CHLA' in doc['station_parameters_in_nc'], 'should have CHLA')
            self.assertTrue('NITRATE' in doc['station_parameters_in_nc'], 'should have NITRATE')
            
    
    def test_all_bad_temp(self):
        #nc with bad qc on temp should not be created
        profiles = ['6900287_8']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        self.assertEqual(len(self.ad.documents), 0, "profile 6900287_8 should not be added")
    

if __name__ == '__main__':
    unittest.main()