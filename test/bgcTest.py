#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 17 07:15:00 2018

@author: tyler
"""

import os
import sys
import pdb
import re
import numpy as np
sys.path.append('..')
from argoDatabase import argoDatabase
import unittest
from datetime import datetime
import pandas as pd
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class bgcTest(unittest.TestCase):

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

        self.optionalKeys = \
                [['POSITIONING_SYSTEM',str],
                ['PLATFORM_TYPE',str],
                ['DATA_MODE',str],
                ['PI_NAME',str],
                ['WMO_INST_TYPE',str],
                ['pres_max_for_TEMP',float],
                ['pres_min_for_TEMP',float],
                ['pres_max_for_PSAL',float],
                ['pres_min_for_PSAL',float],
                ['containsBGC',int],
                ['VERTICAL_SAMPLING_SCHEME',str], # should be required
                ['bgcMeas',list]]
        self.requiredKeys = \
                [['max_pres', float],
                ['measurements', list],
                ['date', datetime],
                ['date_qc', float],
                ['position_qc', float],
                ['cycle_number', int],
                ['lat', float],
                ['lon', float],
                ['dac', str],
                ['geoLocation', dict],
                ['platform_number', str],
                ['station_parameters', list],
                ['nc_url', str],
                ['DIRECTION', str],
                ['_id', str],
                ['station_parameters_in_nc', list],
                ['BASIN', int]]
                

    def tearDown(self):
        return

    def test_bgc(self):
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        profiles = ['5904663_1', '5904663_67', '5903260_219', '5903260_1']
        df = df[ df['_id'].isin(profiles)]
        self.ad.testMode = False
        self.ad.removeExisting = True
        self.ad.replaceProfile=True
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)

        coll = self.ad.create_collection()
        for _id in profiles:
            doc = coll.find_one({'_id': _id})
            self.assertIsInstance(doc, dict, 'doc should exist')
            self.assertIsInstance(doc['containsBGC'], int, '_id: {} should have bgc'.format(_id))
            self.assertIsInstance(doc['bgcMeas'], list)
            self.assertIsInstance(doc['bgcMeas'][0], dict)

   
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
            
    def test_all_bad_bgc_meas(self):
        #  Case when mergeDfs returns an empty dataframe (all nan in rows and columns)
        profiles = ['5903956_236']
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
            self.assertFalse('containsBGC' in doc.keys(), 'should not contain bgcMeas')
    
if __name__ == '__main__':
    unittest.main()