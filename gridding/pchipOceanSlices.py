import pandas as pd
import pdb
import requests
import numpy as np
import os, sys
from datetime import datetime, timedelta
import logging
from scipy.interpolate import PchipInterpolator
import argparse
from collections import OrderedDict, defaultdict

class PchipOceanSlices(object):

    def __init__(self, pLevelRange, basin=None, exceptBasin={None}, starttdx=None, appLocal=False):
        self.appLocal = appLocal
        self.tempCol = 'temp'
        self.psalCol = 'psal'
        self.presCol = 'pres'
        self.keepCols = ['date', 'lat', 'lon', self.presCol, self.tempCol, self.psalCol, 'profile_id', 'position_qc', 'date_qc', 'BASIN']
        self.datesSet = self.get_dates_set()
        self.exceptBasin = exceptBasin
        self.starttdx = starttdx
        self.qcKeep = set([1,2]) # used to filter bad positions and dates
        self.basin = basin # indian ocean only Set to None otherwise
        self.presLevels = [   2.5,   10. ,   20. ,   30. ,   40. ,   50. ,   60. ,   70. ,   80. ,
                        90. ,  100. ,  110. ,  120. ,  130. ,  140. ,  150. ,  160. ,  170. ,
                        182.5,  200. ,  220. ,  240. ,  260. ,  280. ,  300. ,  320. ,  340. ,
                        360. ,  380. ,  400. ,  420. ,  440. ,  462.5,  500. ,  550. ,  600. ,
                        650. ,  700. ,  750. ,  800. ,  850. ,  900. ,  950. , 1000. , 1050. ,
                        1100. , 1150. , 1200. , 1250. , 1300. , 1350. , 1412.5, 1500. , 1600. ,
                        1700. , 1800. , 1900. , 1975. ]

        self.pLevelRange = pLevelRange

        self.presRanges = self.makePresRanges(self.presLevels)
        #self.colNames = ['obsProf', 'profFloatIDAggrSel', 'profJulDayAggrSel', 'profLatAggrSel', 'profLongAggrSel']
        #self.rnCols = {'i' + iCol:colNames[0], 'profile_id': colNames[1], 'date': colNames[2], 'lat': colNames[3], 'lon': colNames[4] }
        pass

    @staticmethod
    def get_dates_set(period=30):
        """
        create a set of dates split into n periods.
        period is in days.
        """
        n_rows = int(np.floor(365/period))
        datesSet = []
        for year in range(2007, 2019):
            yearSet = np.array_split(pd.date_range(str(year)+'-01-01', str(year)+'-12-31'), n_rows)
            datesSet = datesSet + yearSet
        keepEnds = lambda x: [x[0].strftime(format='%Y-%m-%d'), x[-1].strftime(format='%Y-%m-%d')]
        datesSet = list(map(keepEnds, datesSet))
        return datesSet

    @staticmethod
    def get_ocean_slice(startDate, endDate, presRange, intPres, basin=None, appLocal=None):
        '''
        query horizontal slice of ocean for a specified time range
        startDate and endDate should be a string formated like so: 'YYYY-MM-DD'
        presRange should comprise of a string formatted to be: '[lowPres,highPres]'
        Try to make the query small enough so as to not pass the 15 MB limit set by the database.
        '''
        if appLocal:
            baseURL = 'http://localhost:3000/gridding/presSliceForInterpolation/'
        else:
            baseURL = 'https://argovis.colorado.edu/gridding/presSliceForInterpolation/'
        startDateQuery = '?startDate=' + startDate
        endDateQuery = '&endDate=' + endDate
        presRangeQuery = '&presRange=' + presRange
        intPresQuery = '&intPres=' + str(intPres)
        url = baseURL + startDateQuery + endDateQuery + presRangeQuery + intPresQuery
        if basin:
            basinQuery = '&basin=' + basin
            url += basinQuery
        resp = requests.get(url)
        # Consider any status other than 2xx an error
        if not resp.status_code // 100 == 2:
            raise ValueError("Error: Unexpected response {}".format(resp))
        profiles = resp.json()
        return profiles

    def reject_profile(self, profile):
        if not profile['position_qc'] in self.qcKeep:
            reject = True
        elif not profile['date_qc'] in self.qcKeep:
            reject = True
        elif len(profile['measurements']) < 2: #  cannot be interpolated
            reject = True
        elif profile['BASIN'] in self.exceptBasin: #  ignores basins
            reject=True
        else:
            reject = False
        return reject

    @staticmethod
    def make_profile_interpolation_function(x,y, xLab='pres', yLab='temp'):
        '''
        creates interpolation function
        df is a dataframe containing columns xLab and yLab
        xLab: the column name for the interpolation input x
        yLab: the column to be interpolated
        '''
        try:
            f = PchipInterpolator(x, y, axis=1, extrapolate=False)
        except Exception as err:
            pdb.set_trace()
            logging.warning(err)
            raise Exception
        return f  
    
    @staticmethod
    def makePresRanges(presLevels):
        """
        Pressure ranges are based off of depths catagory
        surface: at 2.5 dbar +- 2.5
        shallow: 10 to 182.5 dbar +- 5
        medium: 200 to 462.5 dbar +- 15
        deep: 500 to 1050 dbar +- 30
        abbysal: 1100 to 1975 dbar +- 60
        """
        stringifyArray = lambda x: str(x).replace(' ', '')
        surfaceRange = [[presLevels[0] - 2.5, presLevels[0]+ 2.5]]
        shallowRanges = [ [x - 5, x + 5] for x in presLevels[1:19] ]
        mediumRanges = [ [x - 15, x + 15] for x in presLevels[19:33] ]
        deepRanges = [ [x - 30, x + 30] for x in presLevels[33:45] ]
        abbysalRanges = [ [x - 60, x + 60] for x in presLevels[45:] ]
        presRanges = surfaceRange + shallowRanges + mediumRanges + deepRanges + abbysalRanges
        presRanges = [stringifyArray(x) for x in presRanges]
        return presRanges

    @staticmethod
    def saveIDF(iDf, filename, tdx):
        iDf.date = pd.to_datetime(iDf.date)
        iDf.date = iDf.date.apply(lambda d: d.strftime("%d-%b-%Y %H:%M:%S"))
        if not iDf.empty:
            with open(filename, 'a') as f:
                if tdx==0:
                    iDf.to_csv(f, header=True)
                else:
                    iDf.to_csv(f, header=False)

    @staticmethod
    def record_to_array(measurements, xLab, yLab):
        x = []
        y = []
        for meas in measurements:
            x.append(meas[xLab])
            y.append(meas[yLab])
        return x, y

    @staticmethod
    def sort_list(x, y):
        '''sort list 1 based off of list 2'''
        xy = zip(x, y) 
        ys = [y for _, y in sorted(xy)] 
        xs = sorted(x)
        return xs, ys

    @staticmethod
    def unique_idxs(seq):
        '''gets unique, non nan and non -999 indexes'''
        tally = defaultdict(list)
        for idx,item in enumerate(seq):
            tally[item].append(idx)
        dups = [ (key,locs) for key,locs in tally.items() ]
        dups = [ (key, locs) for key, locs in dups if not np.isnan(key) or key not in {-999, None, np.NaN} ]
        idxs = []
        for dup in sorted(dups):
            idxs.append(dup[1][0])
        return idxs

    def format_xy(self, x, y):
        '''prep for interpolation'''
        x2, y2 = self.sort_list(x, y)
        try:
            x_dup_idx = self.unique_idxs(x2)
            xu = [x2[idx] for idx in x_dup_idx]
            yu = [y2[idx] for idx in x_dup_idx]
            # remove none -999 and none
            y_nan_idx =[idx for idx,key in enumerate(yu) if not key in {-999, None, np.NaN} ]
        except:
            pdb.set_trace()
        xu = [x2[idx] for idx in y_nan_idx]
        yu = [y2[idx] for idx in y_nan_idx]
        return xu, yu

    def make_interpolated_df(self, profiles, xintp, xLab='pres', yLab='temp'):
        '''
        make a dataframe of interpolated values set at xintp for each profile
        xLab: the column name for the interpolation input x
        yLab: the column to be interpolated
        xintp: the values to be interpolated
        '''
        outArray = []
        for profile in profiles:
            if len(profile['measurements']) == 0:
                continue
            if not yLab in profile['measurements'][0].keys():
                continue
            x, y = self.record_to_array(profile['measurements'], xLab, yLab)
            x, y = self.format_xy(x, y)
            if len(x) <= 1:
                continue

            f = self.make_profile_interpolation_function(x, y, yLab)

            rowDict = profile.copy()
            del rowDict['measurements']
            rowDict[xLab] = xintp
            rowDict[yLab] = f(xintp)
            outArray.append(rowDict)
        outDf = pd.DataFrame(outArray)
        outDf = outDf.rename({'_id': 'profile_id'}, axis=1)
        outDf = outDf.dropna(subset=[xLab, yLab], how='any', axis=0)
        logging.debug('number of rows in df: {}'.format(outDf.shape[0]))
        logging.debug('number of profiles interpolated: {}'.format(len(outDf['profile_id'].unique())))
        return outDf

    def intp_pres(self, xintp, presRange):
        if self.basin:
            iTempFileName = 'iTempData_pres_{0}_basin_{1}.csv'.format(xintp, self.basin)
            iPsalFileName = 'iPsalData_pres_{0}_basin_{1}.csv'.format(xintp, self.basin)
        else:
            iTempFileName = 'iTempData_pres_{}.csv'.format(xintp)
            iPsalFileName = 'iPsalData_pres_{}.csv'.format(xintp)
        start = datetime.now()
        
        logging.debug('number of dates:{}'.format(len(self.datesSet)))

        for tdx, dates in enumerate(self.datesSet):

            if tdx < self.starttdx:
                continue
            logging.debug('starting interpolation at time index: {}'.format(tdx))
            startDate, endDate = dates
            try:
                sliceProfiles = self.get_ocean_slice(startDate, endDate, presRange, xintp, self.basin, self.appLocal)
            except Exception as err:
                logging.warning('profiles not recieved: {}'.format(err))
                continue
            logging.debug('xintp: {0} on tdx: {1}'.format(xintp, tdx))
            logging.debug('number of profiles found in interval: {}'.format(len(sliceProfiles)))
            try:
                iTempDf = self.make_interpolated_df(sliceProfiles, xintp, 'pres', 'temp')
            except Exception as err:
                logging.warning('error when interpolating temp')
                logging.warning(err)
                continue

            try:
                iPsalDf = self.make_interpolated_df(sliceProfiles, xintp, 'pres', 'psal')
            except Exception as err:
                pdb.set_trace()
                logging.warning('error when interpolating psal')
                logging.warning(err)
                continue
            
            self.saveIDF(iTempDf, iTempFileName, tdx)
            self.saveIDF(iPsalDf, iPsalFileName, tdx)
            logging.debug('interpolation complete at time index: {}'.format(tdx))
        timeTick = datetime.now()
        logging.debug(timeTick.strftime(format='%Y-%m-%d %H:%M'))
        dt = timeTick-start
        logging.debug('completed run for psal {0} running for: {1}'.format(xintp, dt))

    def main(self):
        logging.debug('inside main loop')
        logging.debug('running pressure level ranges: {}'.format(self.pLevelRange))
        startIdx = self.presLevels.index(self.pLevelRange[0])
        endIdx = self.presLevels.index(self.pLevelRange[1])
        presLevels = self.presLevels[ startIdx:endIdx ]
        presRanges = self.presRanges[ startIdx:endIdx ]
        for idx, presLevel in enumerate(presLevels):
            xintp = presLevel
            presRange = presRanges[idx]
            self.intp_pres(xintp, presRange)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--maxl", help="start on pressure level", type=float, nargs='?', default=20)
    parser.add_argument("--minl", help="end on pressure level", type=float, nargs='?', default=10)
    parser.add_argument("--basin", help="filter this basin", type=str, nargs='?', default=None)
    parser.add_argument("--starttdx", help="start time index", type=int, nargs='?', default=0)
    parser.add_argument("--logFileName", help="name of log file", type=str, nargs='?', default='pchipOceanSlices.log')

    myArgs = parser.parse_args()

    pLevelRange = [myArgs.minl, myArgs.maxl]
    basin = myArgs.basin
    starttdx = myArgs.starttdx
    #idxStr = str(myArgs.minl) + ':' + str(myArgs.maxl)
    #logFileName = 'pchipOceanSlices{}.log'.format(idxStr)

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename=myArgs.logFileName,
                        level=logging.DEBUG)

    logging.debug('Start of log file')
    startTime = datetime.now()
    pos = PchipOceanSlices(pLevelRange, basin=basin, exceptBasin={}, starttdx=starttdx, appLocal=True)
    pos.main()
    endTime = datetime.now()
    dt = endTime - startTime
    logging.debug('end of log file for pressure level ranges: {}'.format(pLevelRange))
    dtStr = 'time to complete: {} seconds'.format(dt.seconds)
    print(dtStr)
    logging.debug(dtStr)