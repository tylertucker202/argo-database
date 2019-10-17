# -*- coding: utf-8 -*-
"""
Created on Sat Nov 17 07:15:00 2018

@author: tyler
"""

import os
import sys
import pdb
import re
sys.path.append('..')
sys.path.append('../add-profiles/')
from argoDatabase import argoDatabase
import addFunctions as af
import unittest
from datetime import datetime
import pandas as pd
import warnings
from numpy import warnings as npwarnings
from argoDBClass import argoDBClass
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class deepTest(argoDBClass):


    # def test_deep_profiles(self):
    #     #  Check that deep profiles are added
    #     profiles = ['6901762_46', '6901762_8', '5905235_5', '4902325_16']
    #     #profiles = ['4902325_16']
    #     df = self.df
    #     df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
    #     df = df[ df['_id'].isin(profiles)]
    #     self.ad.addToDb = False
    #     files = df.file.tolist()
    #     self.ad.add_locally(self.OUTPUTDIR, files)
    #     self.assertTrue(len(self.ad.documents) > 0, 'should have deep meas')
    #     for doc in self.ad.documents:
    #         df = pd.DataFrame(doc['measurements'])
    #         lastPres = df['pres'].max()
    #         qcColNames = [k for k in df.columns.tolist() if '_qc' in k]
            
    #         self.assertTrue(doc['isDeep'], 'isDeep field should have been added')
    #         self.assertTrue('pres_qc' in qcColNames, 'missing pressure qc')
    #         self.assertTrue('temp_qc' in qcColNames, 'missing temp qc')
    #         self.assertTrue('psal_qc' in qcColNames, 'missing psal qc')
    #         self.assertGreater(lastPres, 2000, 'profile should be deeper than 2000 dbar')


    # def test_deep(self):
    #     platform = ['5905164', '5905234']
    #     df = self.df
    #     df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
    #     df = df[ df['platform'].isin(platform)]
    #     self.ad.addToDb = False
    #     files = df.file.tolist()
    #     self.ad.add_locally(self.OUTPUTDIR, files)
    #     self.assertEqual(len(self.ad.documents), 8, "8 deep profiles should be added")
        
    # def test_deep_qc(self):
    #     #  Check that deep profiles are added
    #     profiles = ['6901762_46', '6901762_8', '5905235_5']
    #     df = self.df
    #     df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
    #     df = df[ df['_id'].isin(profiles)]
    #     self.ad.addToDb = False
    #     files = df.file.tolist()
    #     self.ad.add_locally(self.OUTPUTDIR, files)
    #     self.assertTrue(len(self.ad.documents) > 0, 'should have deep meas')
    #     for doc in self.ad.documents:
    #         df = pd.DataFrame(doc['measurements'])
    #         lastPres = df['pres'].max()
    #         qcColNames = [k for k in df.columns.tolist() if '_qc' in k]
            
    #         self.assertTrue(doc['isDeep'], 'isDeep field should have been added')
    #         self.assertTrue('pres_qc' in qcColNames, 'missing pressure qc')
    #         self.assertTrue('temp_qc' in qcColNames, 'missing temp qc')
    #         self.assertTrue('psal_qc' in qcColNames, 'missing psal qc')
    #         self.assertGreater(lastPres, 2000, 'profile should be deeper than 2000 dbar')

    def test_non_deep(self):
        platform = ['5904398']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['platform'].isin(platform)]
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        self.assertTrue(len(self.ad.documents) > 0, 'data should be added')
        doc = self.ad.documents[0]
        self.assertFalse('isDeep' in doc.keys(), 'should not have isDeep key')

if __name__ == '__main__':
    unittest.main()
