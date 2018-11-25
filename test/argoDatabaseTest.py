#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 17 06:48:17 2018

@author: tyler
"""

import os
import glob
import pdb
import sys
import numpy as np
sys.path.append('..')
from argoDatabase import argoDatabase
import unittest

class argoDatabaseTest(unittest.TestCase):

    def setUp(self):
        self.DATADIR = os.path.join(os.getcwd(), 'test-files')
        self.dbName = 'argo-test'
        self.collectionName = 'profiles'
        self.verificationErrors = []
        self.basinFilename = './../basinmask_01.nc'
        self.replaceProfile=False
        self.qcThreshold='1'
        self.dbDumpThreshold=1000
        self.removeExisting=False
        self.testMode=True
        self.basinFilename=self.basinFilename
        self.ad = argoDatabase(self.dbName,
                          self.collectionName,
                          self.replaceProfile,
                          self.qcThreshold,
                          self.dbDumpThreshold,
                          self.removeExisting,
                          self.testMode,
                          self.basinFilename)
        files = self.ad.get_file_names_to_add(self.DATADIR)
        singleFile = [files[0]]
        self.ad.add_locally(os.path.curdir, singleFile)

    def tearDown(self):
        return

    def test_init(self):
        self.ad = argoDatabase(self.dbName,
                          self.collectionName,
                          self.replaceProfile,
                          self.qcThreshold,
                          self.dbDumpThreshold,
                          self.removeExisting,
                          self.testMode,
                          self.basinFilename)
        self.assertEqual(self.ad.dbName, self.dbName)
        self.assertEqual(self.ad.home_dir, os.getcwd())
        self.assertEqual(self.ad.replaceProfile, self.replaceProfile)
        self.assertEqual(self.ad.url, 'ftp://ftp.ifremer.fr/ifremer/argo/dac/')
        self.assertEqual(self.ad.qcThreshold, self.qcThreshold)
        self.assertEqual(self.ad.dbDumpThreshold, self.dbDumpThreshold)
        self.assertEqual(self.ad.removeExisting, self.removeExisting)
        self.assertEqual(self.ad.testMode, self.testMode)
        self.assertEqual(self.ad.documents, [])
        self.assertIsInstance(self.ad.basin, np.ma.core.MaskedArray)
        self.assertIsInstance(self.ad.basin[0], np.int32)
        self.assertEqual(self.ad.basin.shape, (41088,))
        self.assertIsInstance(self.ad.coords, np.ma.core.MaskedArray)
        self.assertEqual(self.ad.coords.shape, (41088, 2))
        self.assertIsInstance(self.ad.coords[0,0], np.float64)

    def test_basin(self):
        lat = -77.5
        lon = -178.5
        self.assertEqual(self.ad.get_basin(lat, lon), 10)
        self.assertEqual(self.ad.get_basin(0, 0), 1)
        self.assertEqual(self.ad.get_basin(30, 0), 3)
        self.assertEqual(self.ad.get_basin(-30, 90), 11)
        
        doc = {'lat':lat, 'lon':lon}
        self.assertEqual(self.ad.add_basin(doc, '')['BASIN'], 10)
        
        self.assertEqual(self.ad.add_basin({'lat':np.NaN, 'lon':0}, '')['BASIN'], -999)

    def test_create_collection(self):
        coll = self.ad.create_collection()
        collIndexes = ['_id_', 'date_-1', 'platform_number_-1', 'cycle_number_-1', 'dac_-1', 'geoLocation_2dsphere', 'containsBGC_-1']
        self.assertEqual(coll.name, 'profiles')
        for key in collIndexes:
            self.assertIn(key, sorted(list(coll.index_information())))
    
    def test_get_file_names_to_add(self):
        files = self.ad.get_file_names_to_add(self.DATADIR)
        self.assertIsInstance(files, list)
        self.assertGreater(len(files), 0)
        
        dfFiles = self.ad.create_df_of_files(files)
        columns = ['file', 'filename', 'profile', 'prefix', 'platform']
        self.assertEqual(len(columns), dfFiles.shape[1])
        for col in dfFiles.columns.tolist():
            self.assertIn(col, columns)

        includeDacs = ['csio', 'kordi']
        files2 = self.ad.get_file_names_to_add(self.DATADIR, includeDacs)
        dfFiles2 = self.ad.create_df_of_files(files2)
        dfFiles2['dac'] = dfFiles2['file'].apply(lambda x: x.split('/')[-4])
        dacs = dfFiles2.dac.unique().tolist()
        self.assertEqual(len(dacs), len(includeDacs))
        for dac in dacs:
            self.assertIn(dac, includeDacs)
        
    def test_remove_duplicate_if_mixed(self):
        dupFiles = []
        dupFiles += glob.glob(os.path.join(self.DATADIR, '**', '**', 'profiles', '*.nc'))
        files = self.ad.get_file_names_to_add(self.DATADIR)
        
        self.assertNotEqual(len(files), len(dupFiles))
    
        #  check if Core files have been removed
        dfDup = self.ad.create_df_of_files(dupFiles)
        duplicatePlatforms = dfDup[ dfDup.prefix.isin(['MR', 'MD']) ].platform.unique().tolist()
        df = self.ad.create_df_of_files(files)
        ndPlatform = df[ df.prefix.isin(['R', 'D'])].platform.unique().tolist()
        for platform in duplicatePlatforms:
            self.assertNotIn(platform, ndPlatform)
            
    def test_all_locally(self):
        return
    
    def test_remove_profiles(self):
        return

    def test_format_param(self):
        return
    
    def test_make_profile_doc(self):
        return
    
    def test_add_single_profile(self):
        return
    
    def test_add_many_profiles(self):
        return

if __name__ == '__main__':
    unittest.main()
