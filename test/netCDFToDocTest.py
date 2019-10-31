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
sys.path.append('../add-profiles/')
from argoDatabase import argoDatabase
import addFunctions as af
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
        df = self.df
        files = df.file.tolist()

        self.ad.add_locally(self.OUTPUTDIR, [files[2]])
        self.assertIsInstance(self.ad.documents, list, 'should be list')

        self.assertIsInstance(self.ad.documents[0], dict, 'should be dict')
        
    def test_required_keys(self):
        self.ad.addToDb = False
        df = self.df
        files = df.file.tolist()

        self.ad.add_locally(self.OUTPUTDIR, random.sample(files, 20))
        self.assertIsInstance(self.ad.documents, list, 'should be list')
        for doc in self.ad.documents:
            docKeys = doc.keys()
            for key, itemType in self.requiredKeys:
                self.assertIn(key, docKeys, 'missing key: {}'.format(key))
                item = doc[key]
                self.assertIsInstance(item, itemType, 'profile {2} key {0} is not of type {1}'.format(key, itemType, doc['_id']))

    def test_optional_keys(self):
        '''Used to find out why some fields are missing'''
        profiles = [ '5905059_1', '5905059_100', '5905059_99', '5905059_98', '5904663_97', '2900784_297' '2901182_8']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.addToDb = False
        files = df.file.tolist()
        
        incompleteDocs = ['5904663_97',  ['pres_max_for_PSAL'],
                          '2900784_297', ['pres_max_for_TEMP', 'pres_min_for_TEMP'],
                          '2900784_297', ['VERTICAL_SAMPLING_SCHEME'] #should be mandatory
                         ]
        
        self.ad.add_locally(self.OUTPUTDIR, files)

        for doc in self.ad.documents:
            docKeys = doc.keys()
            for key, itemType in self.optionalKeys:
                if key in docKeys:
                    item = doc[key]
                    self.assertEqual(type(doc[key]), itemType, 'item type should match for key: {}'.format(key))
                    self.assertIsInstance(item, itemType)
                else:
                    if key not in [ 'containsBGC', 'bgcMeas', 'isDeep' ]:
                        print('profile: {0} missing key: {1}'.format(key, doc['_id']))

                #self.assertIn(key, doc.keys(), 'profile: {0} missing key: {1}'.format(key, doc['_id']))

    def test_ascending_profiles(self):
        '''profile should be acending'''
        profiles = [ '2902534_142']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.addToDb=True
        self.ad.add_locally(self.OUTPUTDIR, files)

        coll = self.ad.create_collection()
        for _id in profiles:
            doc = coll.find_one({'_id': _id})
            self.assertTrue(doc['DIRECTION']!='D', 'should be acending')
            
    ''' TODO: FIND A DECENDING FLOAT TO CHECK
    def test_decending_profiles(self):
        profile should be decending
        profiles = [ '6901762_46']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.addToDb=True
        self.ad.add_locally(self.OUTPUTDIR, files)

        coll = self.ad.create_collection()
        pdb.set_trace()
        for _id in profiles:
            doc = coll.find_one({'_id': _id})
            self.assertTrue(doc['DIRECTION']=='D', 'should be decending')
    '''

    def test_check_profiles(self):
        
        '''5904663_68 is missing position. Position qc should be a 9.
        2903207_72 was reported to be missing pos sys, but it has it here.
        5903593, 5904663 are reported to be missing bgc data.'''
        profiles = [ '5904663_68', '2903207_72', '5904663_97', '2900784_297', '2901182_8', '6901676_30']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.addToDb=True
        self.ad.add_locally(self.OUTPUTDIR, files)

        self.assertTrue(True) # just checks if these dont crash routine

    def test_check_masked_adjusted_profiles(self):
        '''
        profiles that have been adjusted can have masked values. in this case, masked values are filled with NaN.
        '''
        profiles = ['6901676_30']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.addToDb=True
        self.ad.add_locally(self.OUTPUTDIR, files)

        coll = self.ad.create_collection()
        for _id in profiles:
            doc = coll.find_one({'_id': _id})
            keys = doc['measurements'][0].keys()
            
            self.assertFalse('psal' in keys, 'psal should have been removed from _id {}'.format(_id))
if __name__ == '__main__':
    unittest.main()
