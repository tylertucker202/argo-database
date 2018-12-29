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
from argoDBClass import argoDBClass
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class netCDFToDocTest(argoDBClass):
    
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
            docKeys = doc.keys()
            for key, itemType in self.optionalKeys:
                if key in docKeys:
                    self.assertEqual(type(doc[key]), itemType, 'item type should match for key: {}'.format(key))
                else:
                    if key not in [ 'containsBGC', 'bgcMeas', 'isDeep' ]:
                        print('profile: {0} missing key: {1}'.format(key, doc['_id']))

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

    def test_ascending_profiles(self):
        #profile should be acending
        profiles = [ '2902534_142']
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
            self.assertTrue(doc['DIRECTION']!='D', 'should be acending')
            
    ''' TODO: FIND A DECENDING FLOAT TO CHECK
    def test_decending_profiles(self):
        #profile should be decending
        profiles = [ '6901762_46']
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
        pdb.set_trace()
        for _id in profiles:
            doc = coll.find_one({'_id': _id})
            self.assertTrue(doc['DIRECTION']=='D', 'should be decending')
    '''

if __name__ == '__main__':
    unittest.main()