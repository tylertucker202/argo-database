# -*- coding: utf-8 -*-
"""
Created on Sat Nov 17 07:15:00 2018

@author: tyler
"""

import os
import sys
import pdb
sys.path.append('..')
sys.path.append('./../add-profiles')
import unittest
from datetime import datetime, timedelta
import warnings
from numpy import warnings as npwarnings
import tmpFunctions as tf
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class testTmpFunctions(unittest.TestCase):

    def setUp(self):
        self.thisWeekPath = os.path.join(os.path.curdir, 'test-ar_index_this_week_prof.txt')
        self.minDate = datetime.strptime('2019-02-09', '%Y-%m-%d')
        self.maxDate = datetime.strptime('2019-02-12', '%Y-%m-%d')
        self.mDate = self.minDate.strftime('%Y-%m-%d')
        self.mxDate = self.maxDate - timedelta(days=1)
        self.mxDate = self.mxDate.strftime('%Y-%m-%d')

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
        if not os.path.exists(tmpDir):
            os.mkdir( tmpDir )
        self.assertTrue(os.path.exists(tmpDir), 'tmp dir not created')
        tf.clean_up_space()

        self.assertFalse(os.path.exists(tf.globalProfileIndex), 'global profile file not destroyed')
        self.assertFalse(os.path.exists(tf.mixedProfileIndex), 'mixed profile file not destroyed') 
        self.assertFalse(os.path.exists(tmpDir), 'tmp dir not destroyed')  

    def test_get_df_from_date_update(self):
        filterColumn = 'date'
        df = tf.get_df_of_files_to_add(self.thisWeekPath, self.minDate, self.maxDate, dateCol=filterColumn)
        print('df shape of dates_updated: {}'.format(df.shape))
        uniqueDates = df[filterColumn].dt.strftime('%Y-%m-%d').unique()
        self.assertEqual(len(uniqueDates), 3, 'dates should be two unique dates')
        self.assertTrue(self.mDate in uniqueDates, 'one date should be minDate')
        self.assertTrue(self.mxDate in uniqueDates, 'one date should be maxDate')

    def test_get_df_from_dates(self):
        filterColumn = 'date'
        df = tf.get_df_of_files_to_add(self.thisWeekPath, self.minDate, self.maxDate, dateCol=filterColumn)
        print('df shape of dates: {}'.format(df.shape))
        uniqueDates = df[filterColumn].dt.strftime('%Y-%m-%d').unique()
        self.assertEqual(len(uniqueDates), 3, 'dates should be two unique dates')
        self.assertTrue(self.mDate in uniqueDates, 'one date should be minDate')
        self.assertTrue(self.mxDate in uniqueDates, 'one date should be maxDate')
        tf.clean_up_space()

    def test_create_dir_of_files(self):
        df = tf.get_df_of_files_to_add(self.thisWeekPath, self.minDate, self.maxDate, dateCol='date')
        tf.create_dir_of_files(df.head(5), tf.GDAC, tf.ftpPath, tf.tmpDir)
        tmpDir = os.path.join(os.getcwd(), 'tmp')
        self.assertTrue(os.path.exists(tmpDir), 'tmp dir not created')
        for file in df.head(5).file:
            filePath = os.path.join(tmpDir, file)
            self.assertTrue(os.path.exists(filePath), 'File was not added')

    def test_timeout_errors(self):
        rsyncStart = datetime.today()
        for idx in range(10):
            self.test_create_dir_of_files()
        rsyncEnd = datetime.today()
        rsyncDuration = rsyncEnd - rsyncStart
        print('rsync took {} seconds'.format(rsyncDuration.total_seconds()))
        tf.clean_up_space()

if __name__ == '__main__':
    unittest.main()
