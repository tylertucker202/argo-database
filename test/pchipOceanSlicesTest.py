

import os
import sys
import pdb
import re
import numpy as np
sys.path.append('..')
sys.path.append('../gridding/')
from pchipOceanSlices import PchipOceanSlices
import unittest
from datetime import datetime
import random
import warnings
from numpy import warnings as npwarnings
import pandas as pd

class PchipOceanSlicesTest(unittest.TestCase):

    def setUp(self):
        pLevelRange = [10, 20]
        self.xintp = 10
        basin = 4
        starttdx = 0
        self.pos = PchipOceanSlices(pLevelRange, basin=basin, exceptBasin={}, starttdx=starttdx, appLocal=True)

    def tearDown(self):
        pass

    def get_profiles(self, longPresRange=None):
        '''helper function used to get profiles'''
        presRange = '[5.0,15.0]'
        reduceMeas = False
        if longPresRange:
            presRange = '[0.0,2000.0]'
            reduceMeas = True

        dateRange = ['2007-01-01', '2007-01-02']
        dates = pd.to_datetime(dateRange)
        intPres = 10
        basin = '1'
        appLocal = True
        profiles = self.pos.get_ocean_slice(dateRange[0], dateRange[1], presRange, intPres, basin, appLocal, reduceMeas)
        return profiles, dates

    def test_reduce_presLevels_and_presRangess(self):
        self.assertEqual(self.pos.presLevels, [10] , 'pres levels not equal')
        for presRange in self.pos.presRanges:
            self.assertFalse(' ' in presRange, 'check presRange for white space')
        self.assertEqual(self.pos.presRanges[0], '[5.0,15.0]', 'presRanges not equal')

    def test_get_ocean_slice(self):

        profiles, dates = self.get_profiles()
        self.assertTrue(len(profiles) > 0, 'should have returned somthing')
        for profile in profiles:
            self.assertTrue(profile['BASIN'] == 1, 'basin should be filtered')
            date = pd.to_datetime(profile['date'])

            self.assertLess(date, dates[1], 'date not within range')
            self.assertGreater(date, dates[0], 'date not within range')


    def test_make_interpolated_df(self):
        profiles, dates = self.get_profiles()
        xintp = 10
        df = self.pos.make_interpolated_df(profiles, xintp)
        self.assertTrue(df.pres.unique() == xintp, 'pres should always be the same')
        self.assertFalse(any(df.pres.isna()), 'there should be no nan')
        self.assertFalse(any(df.temp.isna()), 'there should be no nan')
        self.assertTrue('profile_id' in df.columns, 'column should have been renamed')
        

    def test_format_xy(self):
        x1 = [8.8, 13.9]
        y1 = [19.283, 19.038]
        xout, yout = self.pos.format_xy(x1, y1)
        #no change
        self.assertEqual(xout, x1)
        self.assertEqual(yout, y1)

        # drop none, nan, and -999
        x2 = x1.copy()
        y2 = y1.copy()
        x2.append(15)
        x2.append(16)
        x2.append(17)
        y2.append(-999)
        y2.append(np.NaN)
        y2.append(None)
        xout, yout = self.pos.format_xy(x2, y2)
        self.assertEqual(xout, x1)
        self.assertEqual(yout, y1)

        # drop duplicate
        x3 = x2.copy()
        y3 = y2.copy()
        x3.append(15)
        y3.append(20)
        xout, yout = self.pos.format_xy(x3, y3)
        self.assertEqual(xout, x1)
        self.assertEqual(yout, y1)
        

    def test_sort_list(self):
        x = [1,2,3]
        y = [0, 1, 2]
        xout, yout = self.pos.sort_list(x, y)
        self.assertEqual(x, xout)
        self.assertEqual(y, yout)

        x2 = x.copy()
        y2 = y.copy()
        x2.append(1.5)
        y2.append(3)
        xout, yout = self.pos.sort_list(x2, y2)
        self.assertEqual(xout, [1,1.5,2,3])
        self.assertEqual(yout, [0,3,1,2])

        x3 = x.copy()
        y3 = y.copy()

        x3.append(4)
        y3.append(2)
        xout, yout = self.pos.sort_list(x3, y3)
        self.assertEqual(x3, xout)
        self.assertEqual(y3, yout)

    def test_record_to_array(self):
        xLab = 'pres'
        yLab = 'temp'
        meas = [{'temp': 19.283, 'psal': 35.517, 'pres': 8.8}, {'temp': 19.038, 'psal': 35.489, 'pres': 13.9}]
        x, y = self.pos.record_to_array(meas, xLab, yLab)
        self.assertEqual(x, [8.8, 13.9])
        self.assertEqual(y, [19.283, 19.038])

    def long_profile_int(self, profile, xintp, presRange, xLab, yLab, reduceMeas=True):
        '''
        same as make_interpolated_profile, just filters by pressure range
        before interpolation step.
        '''
        meas = profile['measurements']
        if len(meas) == 0:
            return None
        if not yLab in meas[0].keys():
            return None
        x, y = self.pos.record_to_array(meas, xLab, yLab)
        x, y = self.pos.format_xy(x, y)

        # filter by presRange portion
        if reduceMeas:
            xa = np.array(x)
            filteredIdx = (xa >= presRange[0]) & (xa <= presRange[1])
            ya = np.array(y)
            y = ya[filteredIdx].tolist()
            x = xa[filteredIdx].tolist()

        f = self.pos.make_profile_interpolation_function(x, y)

        rowDict = profile.copy()
        del rowDict['measurements']
        rowDict[xLab] = xintp

        if len(meas) == 1 and meas[xLab][0] == xintp:
            yintp = meas[yLab][0]
        else:
            yintp = f(xintp)
        rowDict[yLab] = yintp
        return x, y, rowDict

    def test_make_interpolated_profile(self):
        #interpolate with narrow pressure range
        profiles, _ = self.get_profiles()
        longProfiles, _ = self.get_profiles(True)

        del longProfiles[19]
        longProfiles = longProfiles[0:-2]
        longPresRange = [5, 15]

        xintp = 10
        xLab = 'pres'
        yLab = 'temp'

        for idx, profile in enumerate(profiles):
            longProfile = longProfiles[idx]
            row = self.pos.make_interpolated_profile(profile, xintp, xLab, yLab)
            if not row: 
                continue
            xl, yl, longRow = self.long_profile_int(longProfile, xintp, longPresRange, xLab, yLab)
            if np.isnan(row['temp']):
                continue
            print( 'row temp: {0} long row temp: {1}'.format( row['temp'],longRow['temp'] ) )
            self.assertTrue(row['temp'] == longRow['temp'], 'iterpolated should equal long interpolated')

    def test_make_profile_interpolation_function(self):
        x = [1,2,3]
        y = [0, 1, 2]
        f = self.pos.make_profile_interpolation_function(x,y)
        for idx, yval in enumerate(y):
            self.assertEqual(f(x[idx]), yval)
        self.assertEqual(f(1.5), .5)
        self.assertEqual(f(2.5), 1.5)
        self.assertTrue(np.isnan(f(5)), 'extrapolation is forbidden!')
    
        x = [1,2,3,4]
        y = [0,1,2,999]
        f = self.pos.make_profile_interpolation_function(x,y)
        for idx, yval in enumerate(y):
            self.assertEqual(f(x[idx]), yval)
        self.assertEqual(f(1.5), .5, 'value at end should not influence this part of the interpolation')
        self.assertNotEqual(f(2.5), 1.5, 'value at end should influence this part of the interpolatoin')

        x = [1,2]
        y = [0,1]
        f = self.pos.make_profile_interpolation_function(x,y)
        self.assertEqual(f(1.5), .5, 'should be linear interpolation')
        self.assertEqual(f(1.25), .25, 'should be linear interpolation')

    def test_reject_profile(self):

        profile = {}
        profile['position_qc'] = 1
        profile['date_qc'] = 1
        profile['measurements'] = [x for x in range(0, 10)]
        profile['BASIN'] = 1

        self.assertFalse(self.pos.reject_profile(profile), 'profile should be accepted')

        posProf = profile.copy()
        posProf['position_qc'] = 3
        self.assertTrue(self.pos.reject_profile(posProf), 'profile should be rejected')

        dateProf = profile.copy()
        dateProf['date_qc'] = 4
        self.assertTrue(self.pos.reject_profile(dateProf), 'profile should be rejected')
        
        measProf = profile.copy()
        measProf['measurements'] = [x for x in range (0,1)]
        print(len(measProf['measurements']))
        self.assertTrue(self.pos.reject_profile(measProf), 'profile should be rejected')

        self.assertTrue(len(self.pos.exceptBasin) == 0, 'there should be not missing basin')
        self.pos.exceptBasin = {1}
        self.assertTrue(self.pos.reject_profile(profile), 'profile should be rejected')

    def test_get_dates_set(self):
        datesSet = self.pos.get_dates_set()
        year = 2007
        for newDateSeg in datesSet[::12]:
            self.assertTrue('-01-01' in newDateSeg[0])
            self.assertTrue(str(year) in newDateSeg[0])
            self.assertTrue(str(year) in newDateSeg[1])
            self.assertTrue('-01-31' in newDateSeg[1])
            year += 1


if __name__ == '__main__':
    unittest.main()