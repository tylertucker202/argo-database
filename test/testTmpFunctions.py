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
from datetime import datetime, timedelta
import pandas as pd
import warnings
from numpy import warnings as npwarnings
import tmpFunctions as tf
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class testTmpFunctions(unittest.TestCase):
    
    '''
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
   
    def test_get_df_from_date_update(self):
        filename = os.path.join(os.path.curdir, 'ar_index_this_week_prof-test.txt')
        minDate = datetime.strptime('2019-01-18', '%Y-%m-%d')
        maxDate = datetime.strptime('2019-01-19', '%Y-%m-%d')
        mDate = minDate.strftime('%Y-%m-%d')
        mxDate = maxDate - timedelta(days=1)
        mxDate = mxDate.strftime('%Y-%m-%d')
        filterColumn = 'date'
        df = tf.get_df_of_files_to_add(filename, minDate, maxDate, dateCol=filterColumn)
        print('df shape of dates_updated: {}'.format(df.shape))
        uniqueDates = df[filterColumn].dt.strftime('%Y-%m-%d').unique()
        self.assertEqual(len(uniqueDates), 2, 'dates should be two unique dates')
        self.assertTrue(mDate in uniqueDates, 'one date should be minDate')
        self.assertTrue(mxDate in uniqueDates, 'one date should be maxDate')

    def test_get_df_from_dates(self):
        filename = os.path.join(os.path.curdir, 'ar_index_this_week_prof-test.txt')
        minDate = datetime.strptime('2019-01-18', '%Y-%m-%d')
        maxDate = datetime.strptime('2019-01-19', '%Y-%m-%d')
        mDate = minDate.strftime('%Y-%m-%d')
        mxDate = maxDate - timedelta(days=1)
        mxDate = mxDate.strftime('%Y-%m-%d')
        filterColumn = 'date'
        df = tf.get_df_of_files_to_add(filename, minDate, maxDate, dateCol=filterColumn)
        print('df shape of dates: {}'.format(df.shape))
        uniqueDates = df[filterColumn].dt.strftime('%Y-%m-%d').unique()
        self.assertEqual(len(uniqueDates), 2, 'dates should be two unique dates')
        self.assertTrue(mDate in uniqueDates, 'one date should be minDate')
        self.assertTrue(mxDate in uniqueDates, 'one date should be maxDate')
        tf.clean_up_space(tf.globalProfileIndex, tf.mixedProfileIndex, tmpDir)

    '''
    def test_create_dir_of_files(self):
        filename = os.path.join(os.path.curdir, 'ar_index_this_week_prof-test.txt')
        minDate = datetime.strptime('2019-01-20', '%Y-%m-%d')
        maxDate = datetime.strptime('2019-01-21', '%Y-%m-%d')
        df = tf.get_df_of_files_to_add(filename, minDate, maxDate, dateCol='date')
        tf.create_dir_of_files(df.head(5), tf.GDAC, tf.ftpPath)
        tmpDir = os.path.join(os.getcwd(), 'tmp')
        self.assertTrue(os.path.exists(tmpDir), 'tmp dir not created')
        
        for file in df.head(5).file:
            filePath = os.path.join(tmpDir, file)
            self.assertTrue(os.path.exists(filePath), 'File was not added')
        
    def test_wget_create_dir_of_files(self):
        filename = os.path.join(os.path.curdir, 'ar_index_this_week_prof-test.txt')
        minDate = datetime.strptime('2019-01-20', '%Y-%m-%d')
        maxDate = datetime.strptime('2019-01-21', '%Y-%m-%d')
        df = tf.get_df_of_files_to_add(filename, minDate, maxDate, dateCol='date')
        tf.wget_create_dir_of_files(df.head(5), tf.GDAC, tf.ftpPath)
        tmpDir = os.path.join(os.getcwd(), 'tmp')
        self.assertTrue(os.path.exists(tmpDir), 'tmp dir not created')
        
        for file in df.head(5).file:
            filePath = os.path.join(tmpDir, file)
            self.assertTrue(os.path.exists(filePath), 'File was not added')
    
    def test_rsync_create_dir_of_files(self):
        filename = os.path.join(os.path.curdir, 'ar_index_this_week_prof-test.txt')
        minDate = datetime.strptime('2019-01-20', '%Y-%m-%d')
        maxDate = datetime.strptime('2019-01-21', '%Y-%m-%d')
        df = tf.get_df_of_files_to_add(filename, minDate, maxDate, dateCol='date')
        tf.rsync_create_dir_of_files(df.head(5), tf.GDAC, tf.ftpPath)
        tmpDir = os.path.join(os.getcwd(), 'tmp')
        self.assertTrue(os.path.exists(tmpDir), 'tmp dir not created')
        
        for file in df.head(5).file:
            filePath = os.path.join(tmpDir, file)
            self.assertTrue(os.path.exists(filePath), 'File was not added')
    
    
    def test_timeout_errors(self):
        ftpStart = datetime.today()
        for idx in range(10):
            self.test_create_dir_of_files()
        ftpEnd = datetime.today()
        ftpDuration = ftpEnd - ftpStart
        print('ftp took {} seconds'.format(ftpDuration.total_seconds()))
        tf.clean_up_space()
        
        wgetStart = datetime.today()
        for idx in range(10):
            self.test_wget_create_dir_of_files()
        wgetEnd = datetime.today()
        wgetDuration = wgetEnd - wgetStart
        print('wget took {} seconds'.format(wgetDuration.total_seconds()))
        tf.clean_up_space()
        
        rsyncStart = datetime.today()
        for idx in range(10):
            self.test_wget_create_dir_of_files()
        rsyncEnd = datetime.today()
        rsyncDuration = rsyncEnd - rsyncStart
        print('rsync took {} seconds'.format(rsyncDuration.total_seconds()))
        tf.clean_up_space()
    
    

    

if __name__ == '__main__':
    unittest.main()