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
import re
sys.path.append('..')
sys.path.append('../add-profiles/')
from argoDatabase import argoDatabase
import addFunctions as af
import unittest
from argoDBClass import argoDBClass

class argoDatabaseTest(argoDBClass):

    def test_init(self):
        self.ad = argoDatabase(self.dbName,
                          self.collectionName,
                          self.qcThreshold,
                          self.dbDumpThreshold,
                          self.removeExisting,
                          self.basinFilename, 
                          self.addToDb, 
                          self.removeAddedFileNames)
        self.assertTrue(os.path.exists(self.OUTPUTDIR), 'check output directory {}'.format(self.OUTPUTDIR))
        self.assertEqual(self.ad.dbName, self.dbName)
        self.assertEqual(self.ad.home_dir, os.getcwd())
        self.assertEqual(self.ad.url, 'ftp://ftp.ifremer.fr/ifremer/argo/dac/')
        self.assertEqual(self.ad.qcThreshold, self.qcThreshold)
        self.assertEqual(self.ad.dbDumpThreshold, self.dbDumpThreshold)
        self.assertEqual(self.ad.removeExisting, self.removeExisting)
        self.assertEqual(self.ad.documents, [])
        self.assertEqual(self.ad.removeAddedFileNames, self.removeAddedFileNames)
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
        self.assertEqual(self.ad.get_basin(30, 0), 4)
        self.assertEqual(self.ad.get_basin(-30, 90), 3)
        doc = {'lat':lat, 'lon':lon}
        self.assertEqual(self.ad.add_basin(doc, '')['BASIN'], 10)
        self.assertEqual(self.ad.add_basin({'lat':np.NaN, 'lon':0}, '')['BASIN'], -999)

    def test_create_collection(self):
        coll = self.ad.create_collection()
        collIndexes = ['_id_', 'date_-1', 'platform_number_-1', 'dac_-1', 'geoLocation_2dsphere']
        self.assertEqual(coll.name, 'profiles')
        for key in collIndexes:
            self.assertIn(key, sorted(list(coll.index_information())))

    def test_remove_profiles(self):
        profiles = ['6901762_46', '6901762_8']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.ad.addToDb=True
        self.ad.add_locally(self.OUTPUTDIR, files)

        #create collection and create custom key.
        coll = self.ad.create_collection()
        myKey = 'notUpdated'
        for _id in profiles:
            coll.find_one_and_update({"_id": _id}, {'$set': {myKey: True}})
        self.ad.documents = []
        # new doc should replace old one, removing myKey from document
        self.ad.add_locally(self.OUTPUTDIR, files)
        
        for _id in profiles:
            doc = coll.find_one({'_id': _id})
            self.assertTrue(isinstance(doc, dict), 'doc was not found')
            self.assertTrue(myKey not in doc.keys(), 'document was not replaced with new one')
            doc = None
    
    def test_dump_threshold(self):
        profiles = ['6901762_46', '6901762_8']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.ad.addToDb=True
        self.ad.dbDumpThreshold = 2
        self.ad.add_locally(self.OUTPUTDIR, files)
        docLength = len(self.ad.documents)
        self.assertEqual(docLength, 0, 'documents should have been reinitialized: doc length: {}'.format(docLength))
        
    def test_delete_list_of_files(self):
        profiles = ['6901762_46.nc', '6901762_8.nc']
        dummyDir = 'dummyTmp'
        if not os.path.exists(dummyDir):
            os.mkdir(dummyDir)
        files = []
        # create files
        for profile in profiles:
            file = os.path.join(os.getcwd(), dummyDir, profile)
            files.append(file)
            cmd = 'touch ' + file
            os.system(cmd)
            self.assertTrue(os.path.exists(file), 'file should have been created')
        # remove files
        self.ad.delete_list_of_files(files)
        for file in files:
            self.assertFalse(os.path.exists(file), 'path should have been deleted')



    def test_data_mode(self):
        '''
        data mode should be added to documents.
        '''
        profiles = ['2902476_145', '2902476_146', '2902476_147', '2902476_20', '1900722_4']
        df = self.df
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        files = df.file.tolist()
        
        self.ad.removeExisting = True
        self.ad.addToDb=False
        self.ad.dbDumpThreshold = 2
        self.ad.add_locally(self.OUTPUTDIR, files)
        for doc in self.ad.documents:
            self.assertTrue('DATA_MODE' in doc.keys(), 'data_mode should have been added')

    # def test_format_param(self):
    #     return
    
    # def test_make_profile_doc(self):
    #     return
    
    # def test_add_single_profile(self):
    #     return
    
    # def test_add_many_profiles(self):
    #     return

if __name__ == '__main__':
    unittest.main()

