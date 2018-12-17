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
import numpy as np
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
        #  check platform with noisy bgc meas
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
    
    def test_bgc_platform(self):
        #  check platform with bgc parameter
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
            
            df = pd.DataFrame(doc['bgcMeas'])
            self.assertGreater(df.shape[0], 0, 'bgcMeas should have values')
            dfBGC = df.drop(['pres', 'pres_qc'], axis=1)
            dfBGC.replace(-999, np.nan, inplace=True)
            beforeShape = dfBGC.shape[0]
            dfBGC.dropna(axis=0, how='all', inplace=True)
            afterShape = dfBGC.shape[0]
            self.assertEqual(beforeShape, afterShape, 'There shall be no empty bgcMeas fields')
    
    def test_all_bad_temp(self):
        #  nc with bad qc on temp should not be created
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

    def test_big_bgc(self):
        #  check platform with large bgc measurements. Merge should be less than 5000 rows.
        platform = ['3902124']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)].head()
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            df = pd.DataFrame(doc['bgcMeas'])
            dfBGC = df.drop(['pres', 'pres_qc'], axis=1)
            dfBGC.replace(-999, np.nan, inplace=True)
            beforeShape = dfBGC.shape[0]
            dfBGC.dropna(axis=0, how='all', inplace=True)
            afterShape = dfBGC.shape[0]
            self.assertLess(beforeShape, 5000, 'bgc df looking really big')
            self.assertEqual(beforeShape, afterShape, 'There shall be no empty bgcMeas fields')

    def test_missing_bgc(self):
        #  check platform with adjusted bgc parameter that has been masked.
        platform = ['1901499']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)].head()
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            df = pd.DataFrame(doc['bgcMeas'])
            self.assertGreater(df.shape[0], 0, 'bgcMeas should have values')
            dfBGC = df.drop(['pres', 'pres_qc'], axis=1)
            dfBGC.replace(-999, np.nan, inplace=True)
            beforeShape = dfBGC.shape[0]
            dfBGC.dropna(axis=0, how='all', inplace=True)
            afterShape = dfBGC.shape[0]
            self.assertEqual(beforeShape, afterShape, 'There shall be no empty bgcMeas fields')

    def test_missing_pres_in_bgc(self):
        #  Case when mergeDfs returns an empty dataframe (all nan in rows and columns)
        profiles = ['6901659_1', '6901473_500', '6902547_1', '6901657_1']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            df = pd.DataFrame( doc['bgcMeas'])
            self.assertGreater(df.shape[0], 0, 'should contain bgcMeas')
    
    def test_float_conversion_in_bgc(self):
        #  Case when mergeDfs returns an empty dataframe (all nan in rows and columns)
        profiles = ['4901167_122']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            self.assertFalse('bgcMeas' in doc.keys(), 'should not contain bgcMeas')

    def test_deep(self):
        #  Check that deep profiles are added
        profiles = ['6901762_46', '6901762_8', '5905235_5']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        self.assertTrue(len(self.ad.documents) > 0, 'should have deep meas')
        for doc in self.ad.documents:
            df = pd.DataFrame(doc['measurements'])
            lastPres = df['pres'].max()
            qcColNames = [k for k in df.columns.tolist() if '_qc' in k]  
            self.assertTrue(doc['isDeep'], 'isDeep field should have been added')
            self.assertTrue('pres_qc' in qcColNames, 'missing pressure qc')
            self.assertTrue('temp_qc' in qcColNames, 'missing temp qc')
            self.assertTrue('psal_qc' in qcColNames, 'missing psal qc')
            self.assertGreater(lastPres, 2000, 'profile should be deeper than 2000 dbar')
if __name__ == '__main__':
    unittest.main()
