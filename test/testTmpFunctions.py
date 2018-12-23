#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 17 07:15:00 2018

@author: tyler
"""

import os
import sys
import pdb
sys.path.append('..')
sys.path.append('../add-profiles')
from argoDatabase import argoDatabase
import unittest
from datetime import datetime
import pandas as pd
import warnings
from numpy import warnings as npwarnings
import tmpFunctions as tf
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class testTmpFunctions(unittest.TestCase):
    
    def test_get_last_updated(self):
        date = tf.get_last_updated(filename='lastUpdatedTest.txt')
        print(date)
        self.assertEqual(type(date), type(datetime.today()), 'date should be a datetime object')
        
    def test_write_last_updated(self):
        dateStr = '1999-9-9'
        tf.write_last_updated(dateStr, 'lastUpdatedTest.txt')
        writtenDate = tf.get_last_updated(filename='lastUpdatedTest.txt')
        self.assertEqual(writtenDate, datetime.strptime(dateStr, '%Y-%m-%d'))
        print(writtenDate)

    def test_download_todays_file_and_clean_up_space(self):
        tf.download_todays_file(tf.GDAC, tf.ftpPath, tf.globalProfileIndex, tf.globalProfileName)
        tf.download_todays_file(tf.GDAC, tf.ftpPath, tf.mixedProfileIndex, tf.mixedProfileName)
        
        self.assertTrue(os.path.exists(tf.globalProfileIndex), 'global profile file not created')
        self.assertTrue(os.path.exists(tf.mixedProfileIndex), 'mixed profile file not created')        

        tmpDir = os.path.join(os.getcwd(), 'tmp')
        os.mkdir( tmpDir )
        self.assertTrue(os.path.exists(tmpDir), 'tmp dir not created')
        tf.clean_up_space(tf.globalProfileIndex, tf.mixedProfileIndex, tmpDir)
        
        self.assertFalse(os.path.exists(tf.globalProfileIndex), 'global profile file not destroyed')
        self.assertFalse(os.path.exists(tf.mixedProfileIndex), 'mixed profile file not destroyed') 
        self.assertFalse(os.path.exists(tmpDir), 'tmp dir not destroyed')
        

    def test_get_df_from_dated_updated(self):
        minDate = tf.get_last_updated(filename='lastUpdated.txt')
        maxDate = datetime.today()
        #df = tf.get_df_from_dates_updated(minDate, maxDate)

        
    def test_clean_up_space(self):
        return
        

if __name__ == '__main__':
    unittest.main()