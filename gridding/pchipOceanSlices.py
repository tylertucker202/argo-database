import pandas as pd
import pdb
import requests
import numpy as np
import os, sys
from datetime import datetime
import logging
from scipy.interpolate import PchipInterpolator


def get_dates_set(period=30):
    """
    create a set of dates split into n periods.
    period is in days.
    """
    n_rows = int(np.floor(365/period))
    datesSet = []
    for year in range(2007, 2020):
        yearSet = np.array_split(pd.date_range(str(year)+'-01-01', str(year)+'-12-31'), n_rows)
        datesSet = datesSet + yearSet
    keepEnds = lambda x: [x[0].strftime(format='%Y-%m-%d'), x[-1].strftime(format='%Y-%m-%d')]
    datesSet = list(map(keepEnds, datesSet))
    return datesSet

def get_ocean_slice(startDate, endDate, presRange='[5,15]'):
    '''
    query horizontal slice of ocean for a specified time range
    startDate and endDate should be a string formated like so: 'YYYY-MM-DD'
    presRange should comprise of a string formatted to be: '[lowPres,highPres]'
    Try to make the query small enough so as to not pass the 15 MB limit set by the database.
    '''
    baseURL = 'https://argovis.colorado.edu/gridding/presSlice/'
    startDateQuery = '?startDate=' + startDate
    endDateQuery = '&endDate=' + endDate
    presRangeQuery = '&presRange=' + presRange
    url = baseURL + startDateQuery + endDateQuery + presRangeQuery
    resp = requests.get(url)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    profiles = resp.json()
    return profiles

def parse_into_df(profiles):
    '''
    transforms list of profile dictionaries into one dataframe
    '''
    meas_keys = profiles[0]['measurements'][0].keys()
    df = pd.DataFrame(columns=meas_keys)
    profDicts = []
    for profile in profiles:
        if len(profile['measurements']) < 2: # ignore values that cant interpolate to
            continue
        profileDf = pd.DataFrame(profile['measurements'])
        profileDf['cycle_number'] = profile['cycle_number']
        profileDf['profile_id'] = profile['_id']
        profileDf['lat'] = profile['lat']
        profileDf['lon'] = profile['lon']
        profileDf['date'] = profile['date']
        profDict = profileDf.to_dict(orient='records')
        profDicts += profDict
    df = pd.DataFrame(profDicts)
    return df

def make_profile_interpolation_function(df, xLab='pres', yLab='temp'):
    '''
    creates interpolation function
    df is a dataframe containing columns xLab and yLab
    xLab: the column name for the interpolation input x
    yLab: the column to be interpolated
    '''
    df = df.sort_values([xLab], ascending=True)
    x = df.pres.astype(float).values
    y = df[yLab].astype(float).values
    try:
        f = PchipInterpolator(x, y, axis=1, extrapolate=False)
    except Exception as err:
        pdb.set_trace()
        logging.warning(err)
        raise Exception
    return f  

def make_interpolated_df(df, xintp, xLab='pres', yLab='temp'):
    '''
    make a dataframe of interpolated values set at xintp for each profile
    xLab: the column name for the interpolation input x
    yLab: the column to be interpolated
    xintp: the values to be interpolated
    '''
    outDf = pd.DataFrame()
    for idx, (_id, profDf) in enumerate(df.groupby(['profile_id'])):
        if profDf.empty:
            continue
        profDf = profDf.drop_duplicates(subset=[xLab])
        profDf = profDf[profDf[yLab] != -999]
        if profDf.shape[0] <= 1:
            continue

        f = make_profile_interpolation_function(profDf, xLab, yLab)

        intpDf = profDf.head(1)
        intpDf = pd.concat([intpDf]*len(xintp), ignore_index=True)
        intpDf[xLab] = xintp
        intpDf[yLab] = intpDf.pres.apply(lambda pres: f(pres))

        intpDf = intpDf.dropna(subset=[xLab, yLab], how='any', axis=0)
        outDf = pd.concat([outDf, intpDf], axis=0, ignore_index=True)


    logging.debug('number of rows in df: {}'.format(outDf.shape[0]))
    logging.debug('number of profiles interpolated: {}'.format(len(outDf['profile_id'].unique())))
    return outDf


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='pchipOceanSlices.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')

    presLevels = [   2.5,   10. ,   20. ,   30. ,   40. ,   50. ,   60. ,   70. ,   80. ,
                    90. ,  100. ,  110. ,  120. ,  130. ,  140. ,  150. ,  160. ,  170. ,
                    182.5,  200. ,  220. ,  240. ,  260. ,  280. ,  300. ,  320. ,  340. ,
                    360. ,  380. ,  400. ,  420. ,  440. ,  462.5,  500. ,  550. ,  600. ,
                    650. ,  700. ,  750. ,  800. ,  850. ,  900. ,  950. , 1000. , 1050. ,
                    1100. , 1150. , 1200. , 1250. , 1300. , 1350. , 1412.5, 1500. , 1600. ,
                    1700. , 1800. , 1900. , 1975. ]
    xintp = [20]  # 10 decibar
    presRange = '[15,25]'
    iCol = 'temp'
    jCol = 'psal'
    xLab = 'pres'
    yLab = iCol
    colName = 'i' + iCol
    keepCols = ['date', 'lat', 'lon', xLab, yLab, 'profile_id']

    colNames = ['obsProf', 'profFloatIDAggrSel', 'profJulDayAggrSel', 'profLatAggrSel', 'profLongAggrSel']

    profDfColNames = ['date','lat','lon','pres', 'profile_id', iCol]
    rnCols = {'i' + iCol:colNames[0], 'profile_id': colNames[1], 'date': colNames[2], 'lat': colNames[3], 'lon': colNames[4] }
    start = datetime.now()
    datesSet = get_dates_set()
    
    logging.debug('number of dates:{}'.format(len(datesSet)))

    df = pd.DataFrame()

    for tdx, dates in enumerate(datesSet):
        if tdx < 30:
            continue

        logging.debug('time index: {}'.format(tdx))
        startDate, endDate = dates

        sliceProfiles = get_ocean_slice(startDate, endDate, presRange)
        if len(sliceProfiles) == 0:
            continue
        logging.debug('on tdx: {}'.format(tdx))
        logging.debug('number of profiles found in interval: {}'.format(len(sliceProfiles)))

        sliceDf = parse_into_df(sliceProfiles)
        sliceDf = sliceDf[profDfColNames]
        sliceDf = sliceDf[keepCols]
        try:
            iDf = make_interpolated_df(sliceDf, xintp, xLab, yLab)
        except Exception as err:
            logging.warning(err)
            continue
        
        iDf.date = pd.to_datetime(iDf.date)
        iDf.date = iDf.date.apply(lambda d: d.strftime("%d-%b-%Y %H:%M:%S"))
        if iDf.empty:
            continue
        df = pd.concat([df, iDf], sort=False)
        with open('testData.csv', 'a') as f:
            if tdx==0:
                iDf.to_csv(f, header=True)
            else:
                iDf.to_csv(f, header=False)


    logging.debug('time index: {}'.format(tdx))
    timeTick = datetime.now()
    logging.debug(timeTick.strftime(format='%Y-%m-%d %H:%M'))
    dt = timeTick-start
    logging.debug('running for: {}'.format(dt))