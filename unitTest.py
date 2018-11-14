#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import pdb
import sys
#sys.path('..')
from argoDatabase import argoDatabase
import unittest

class grid_and_area_unit_tests(unittest.TestCase):

    def setUp(self):
        self.OUTPUTDIR = os.path.join(os.getcwd(), 'test-files')
        self.dbName = 'argo'
        self.collectionName = 'profiles'
        self.verificationErrors = []
        self.ad = argoDatabase(self.dbName,
                          self.collectionName,
                          replaceProfile=False,
                          qcThreshold='1', 
                          dbDumpThreshold=10000,
                          removeExisting=False,
                          testMode=True)
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR, howToAdd='profiles')
        singleFile = [files[0]]
        self.ad.add_locally(os.path.curdir, singleFile)
        self.optionalKeys = \
                ['POSITIONING_SYSTEM',
                'PLATFORM_TYPE',
                'DATA_MODE',
                'PI_NAME',
                'WMO_INST_TYPE',
                'pres_max_for_TEMP',
                'pres_min_for_TEMP',
                'pres_max_for_PSAL',
                'pres_min_for_PSAL',
                'containsBGC',
                'bgcMeas']
        self.requiredKeys = \
                ['max_pres',
                'measurements',
                'date',
                'date_qc',
                'position_qc',
                'cycle_number',
                'lat',
                'lon',
                'dac',
                'geoLocation',
                'platform_number',
                'station_parameters',
                'nc_url',
                'DIRECTION',
                '_id',
                'VERTICAL_SAMPLING_SCHEME',
                'STATION_PARAMETERS_inMongoDB',
                'BASIN']

    def tearDown(self):
        return
    
    def test_init(self):   
        return
    
    def test_document_creation(self):
        pdb.set_trace()
        self.assertIsInstance(self.ad.documents, list, 'should be list')
        self.assertIsInstance(self.ad.documents[0], dict, 'should be dict')
        #print(self.ad.documents)
        
    def test_fields_exist(self):
        
        docKeys = self.ad.documents[0].keys()
        for key in self.requiredKeys:
            self.assertIn(key, docKeys, 'missing key: {}'.format(key))
    
    def test_strings(self):
        return

    def test_ints(self):
        return

    def test_floats(self):
        return

if __name__ == '__main__':
    unittest.main()


