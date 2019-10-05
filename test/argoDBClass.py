import os
import sys
import pdb
sys.path.append('..')
sys.path.append('../add-profiles/')
from argoDatabase import argoDatabase
import addFunctions as af
import unittest
from datetime import datetime
from numpy import float64

#  Sometimes netcdf contain nan. This will suppress runtime warnings.

class argoDBClass(unittest.TestCase):

    def setUp(self):
        self.OUTPUTDIR = os.path.join(os.getcwd(), 'test-files')
        self.dbName = 'argo-test'
        self.collectionName = 'profiles'
        self.verificationErrors = []
        self.basinFilename = os.path.join(os.getcwd(), os.pardir, 'basinmask_01.nc')
        self.qcThreshold='1'
        self.dbDumpThreshold=1000
        self.removeExisting=False
        self.addToDb=True
        self.removeAddedFileNames = False
        self.df = af.get_df_to_add(self.OUTPUTDIR)
        self.ad = argoDatabase(self.dbName,
                          self.collectionName,
                          self.qcThreshold,
                          self.dbDumpThreshold,
                          self.removeExisting,
                          self.basinFilename,
                          self.addToDb, 
                          self.removeAddedFileNames)

        self.optionalKeys = \
                [['POSITIONING_SYSTEM',str],
                ['PLATFORM_TYPE',str],
                ['DATA_MODE',str],
                ['PI_NAME',str],
                ['WMO_INST_TYPE',str],
                ['pres_max_for_TEMP',float64],
                ['pres_min_for_TEMP',float64],
                ['pres_max_for_PSAL',float64],
                ['pres_min_for_PSAL',float64],
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
        coll = self.ad.create_collection()
        coll.drop()