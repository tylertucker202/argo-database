import pandas as pd
import pdb
import requests
import numpy as np
import os, sys
import xarray as xr
from datetime import datetime, timedelta
import logging
from scipy.interpolate import PchipInterpolator
import argparse
from collections import OrderedDict, defaultdict
sys.path.append('..')
from pchipOceanSlices import PchipOceanSlices
import xarray as xr

class MeanOceanSlices(PchipOceanSlices):

    def __init__(self, basin=None, exceptBasin={None}, starttdx=None, appLocal=False):
        PchipOceanSlices.__init__(self, [2.5, 1975], basin, exceptBasin, starttdx, appLocal)
        self.reduceMeas = False
        self.delta = .25 #grid dimension
        self.presRanges = [  [0,7.5],  [7.5,12], [12,20],  [20,30],  [30,50],  [50,75],  [75,100],  [100,125], \
                        [125,150], [150,200], [200,250], [250,300], [300,400], [400,500], [500,600], [600,700], [700,800], \
                        [800,900], [900,1000], [1000,1100], [1100,1200], [1200,1300], [1300,1400], [1400,1500], [1500,1750], [1750,2000]]
        self.presLevels = self.make_pres_levels(self.presRanges)
        self.keepCols = ['date', 'lat', 'lon', 'measurements', '_id', 'position_qc', 'date_qc', 'BASIN']
    @staticmethod
    def make_pres_levels(presRanges):
        presLevels = []
        for pr in presRanges:
            pl = (pr[1] + pr[0])/2.
            presLevels.append(pl)
        return presLevels

    @staticmethod
    def get_dates_set(period=5):
        """
        create a set of dates split into n periods.
        increments fall on the same day, regardless of year (leap years have an extra day)
        """
        n_rows = int(np.floor(365/period))
        datesSet = []
        yearSet = np.array_split(pd.date_range('2001-01-01', '2001-12-31'), n_rows) #using non-leap year as reference
        keepEnds = lambda x: [x[0].strftime(format='-%m-%d'), x[-1].strftime(format='-%m-%d')]
        yearSet = list(map(keepEnds, yearSet))
        for year in range(2004, 2019):
            ys = [[str(year)+x[0], str(year)+x[1]] for x in yearSet]
            datesSet = datesSet + ys

        return datesSet
    
    def main(self):
        logging.debug('inside main loop')
        logging.debug('running pressure level ranges: {}'.format(self.pLevelRange))
        for idx, presLevel in enumerate(self.presLevels):
            if not presLevel == 1875:
                continue
            pdb.set_trace()
            xintp = presLevel
            presRange = self.presRanges[idx]
            self.intp_pres(xintp, presRange)

    def intp_pres(self, xintp, presRange):
        if self.basin:
            iTempFileName = 'iTempData_basin_{0}.csv'.format(self.basin)
            iPsalFileName = 'iPsalData_basin_{0}.csv'.format(self.basin)
            mTempFileName = 'mTempData_basin_{0}.csv'.format(self.basin)
            mPsalFileName = 'mPsalData_basin_{0}.csv'.format(self.basin)
        else:
            iTempFileName = 'iTempData.csv'
            iPsalFileName = 'iPsalData.csv'
            mTempFileName = 'mTempData.csv'
            mPsalFileName = 'mPsalData.csv'
        start = datetime.now()
        
        logging.debug('number of dates:{}'.format(len(self.datesSet)))

        for tdx, dates in enumerate(self.datesSet):

            if tdx < self.starttdx:
                continue
            logging.debug('starting interpolation at time index: {}'.format(tdx))
            startDate, endDate = dates
            try:
                prs = str(presRange).replace(' ', '')
                sliceProfiles = self.get_ocean_slice(startDate, endDate, prs, xintp, self.basin, self.appLocal, self.reduceMeas)
            except Exception as err:
                logging.warning('profiles not recieved: {}'.format(err))
                continue
            logging.debug('xintp: {0} on tdx: {1}'.format(xintp, tdx))
            logging.debug('number of profiles found in interval: {}'.format(len(sliceProfiles)))
            try:
                #iTempDf = self.make_interpolated_df(sliceProfiles, xintp, 'pres', 'temp')
                mTempDf = self.make_mean_df(sliceProfiles, 'temp')
            except Exception as err:
                logging.warning('error with temp df')
                logging.warning(err)
                continue

            # try:
            #     iPsalDf = self.make_interpolated_df(sliceProfiles, xintp, 'pres', 'psal')
            #     mPsalDf = self.make_mean_df(sliceProfiles, 'psal')
            # except Exception as err:
            #     pdb.set_trace()
            #     logging.warning('error with psal df')
            #     logging.warning(err)
                # continue

            #self.save_df(iTempDf, iTempFileName, tdx, dates, xintp)
            #self.save_df(iPsalDf, iPsalFileName, tdx, dates, xintp)
            self.save_df(mTempDf, mTempFileName, tdx, dates, xintp)
            #self.save_df(mPsalDf, mPsalFileName, tdx, dates, xintp)
        logging.debug('interpolation complete at time index: {}'.format(tdx))
        timeTick = datetime.now()
        logging.debug(timeTick.strftime(format='%Y-%m-%d %H:%M'))
        dt = timeTick-start
        logging.debug('completed run for psal {0} running for: {1}'.format(xintp, dt))

    def save_df(self, df, filename, tdx, dates, pres):
        '''save as xarray'''
        dates = pd.to_datetime(dates)
        df['startDate'] = dates[0]
        df['endDate'] = dates[1]
        df['tdx'] = int(tdx)
        df['pres'] = pres
        tailend = '_{0}.csv'.format(pres)
        filename = filename.replace('.csv', tailend)
        filename = '/storage/ShenResults/' + filename
        df.to_csv(filename)

    def calc_mean(self, df, yLab):
        y = []
        for row in df.itertuples():
            for rm in row.measurements:
                if not yLab in rm.keys():
                    continue
                y.append(rm[yLab])
        if len(y) == 0:
            return np.nan, np.nan, 0
        return np.mean(y), np.std(y), len(y)

    def make_mean_df(self, profiles, yLab):
        df = pd.DataFrame(profiles)
        df = df[self.keepCols]
        def to_bin(x, delta):
            if x % (delta) <= delta/2:
                return np.floor(x / delta) * delta
            else:
                return np.ceil(x / delta) * delta
        # group profiles into lat-lon bins
        to_bin_2 = lambda x: to_bin(x, self.delta)
        df['latbin'] = df['lat'].map(to_bin_2)
        df['lonbin'] = df['lon'].map(to_bin_2)
        grouped = df.groupby(['latbin', 'lonbin'], as_index=False)
        lats = []
        longs = []
        means = []
        stds = []
        dofs = []
        
        for (lat, lon), group in grouped: # mean, std, df for each bin
            mean, std, dof = self.calc_mean(group, yLab)
            if dof == 0:
                continue
            lats.append(lat)
            longs.append(lon)
            means.append(mean)
            stds.append(std)
            dofs.append(dof)
        mydict = {'lat': lats, 'long': longs, 'mean': means, 'std': stds, 'dof': dofs}
        meanDf = pd.DataFrame(mydict)
        return meanDf

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--basin", help="filter this basin", type=str, nargs='?', default=None)
    parser.add_argument("--starttdx", help="start time index", type=int, nargs='?', default=0)
    parser.add_argument("--logFileName", help="name of log file", type=str, nargs='?', default='MeanOceanSlices.log')

    myArgs = parser.parse_args()
    basin = myArgs.basin
    starttdx = myArgs.starttdx

    # python meanOceanSlices.py --starttdx 1088

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename=myArgs.logFileName,
                        level=logging.DEBUG)

    logging.debug('Start of log file')
    startTime = datetime.now()
    mos = MeanOceanSlices(basin=basin, exceptBasin={}, starttdx=starttdx, appLocal=True)
    mos.main()
    endTime = datetime.now()
    dt = endTime - startTime
    # logging.debug('end of log file for pressure level ranges: {}'.format(pLevelRange))
    # dtStr = 'time to complete: {} seconds'.format(dt.seconds)
    # print(dtStr)
    # logging.debug(dtStr)