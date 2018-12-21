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
import random
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class netCDFToDocTest(unittest.TestCase):

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

    def test_document_creation(self):
        self.ad.addToDb = False
        self.ad.testMode = True
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)

        self.ad.add_locally(self.OUTPUTDIR, [files[0]])
        self.assertIsInstance(self.ad.documents, list, 'should be list')

        self.assertIsInstance(self.ad.documents[0], dict, 'should be dict')
        
    def test_required_keys(self):
        self.ad.addToDb = False
        self.ad.testMode = True
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)

        self.ad.add_locally(self.OUTPUTDIR, random.sample(files, 20))
        self.assertIsInstance(self.ad.documents, list, 'should be list')
        for doc in self.ad.documents:
            docKeys = doc.keys()
            
            for key, itemType in self.requiredKeys:
                self.assertIn(key, docKeys, 'missing key: {}'.format(key))
                self.assertIsInstance(doc[key], itemType)
            
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

    def test_optional_keys(self):
        #Used to find out why some fields are missing
        profiles = [ '5905059_1', '5905059_100', '5905059_99', '5905059_98', '5904663_97', '2900784_297' '2901182_8']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        
        incompleteDocs = ['5904663_97',  ['pres_max_for_PSAL'],
                          '2900784_297', ['pres_max_for_TEMP', 'pres_min_for_TEMP'],
                          '2900784_297', ['VERTICAL_SAMPLING_SCHEME'] #should be mandatory
                         ]
        
        #files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        self.ad.add_locally(self.OUTPUTDIR, files)

        for doc in self.ad.documents:
            for key, itemType in self.optionalKeys:
                try:
                    item = doc[key]
                except KeyError:
                    _id =  doc['_id']
                    if key not in [ 'containsBGC', 'bgcMeas']:
                        print('profile: {0} missing key: {1}'.format(key, _id))

                #self.assertIn(key, doc.keys(), 'profile: {0} missing key: {1}'.format(key, doc['_id']))
                #self.assertIsInstance(item, itemType)
                
    def test_check_profiles(self):
        
        #5904663_68 is missing position. Position qc should be a 9.
        #2903207_72 was reported to be missing pos sys, but it has it here.
        #5903593, 5904663 are reported to be missing bgc data.
        profiles = [ '5904663_68', '2903207_72', '5904663_97', '2900784_297', '2901182_8']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.ad.replaceProfile=True
        self.addToDb=True
        self.ad.add_locally(self.OUTPUTDIR, files)

        coll = self.ad.create_collection()
        for _id in profiles:
            doc = coll.find_one({'_id': _id})
            self.assertTrue(True)

    def test_deep(self):
        platform = ['5905164', '5905234']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        self.assertEqual(len(self.ad.documents), 8, "8 deep profiles should be added")

    def test_non_deep(self):
        platform = ['5904398']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        doc = self.ad.documents[0]
        self.assertFalse('isDeep' in doc.keys(), 'should not have isDeep key')

if __name__ == '__main__':
    unittest.main()