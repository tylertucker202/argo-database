# -*- coding: utf-8 -*-
"""
Created on Tue Nov 27 16:59:48 2018

@author: tyler
"""
import numpy as np
import pandas as pd
import logging
import re
import pdb

class measToDf(object):
    def __init__(self, variables,
                 stationParameters,
                 nProf,
                 profileID,
                 data_mode,
                 idx=0,
                 qcThreshold='1',
                 decodeFormat='utf-8'):
        logging.debug('initializing measToDf')
        self.decodeFormat = decodeFormat
        self.data_mode = data_mode
        self.stationParameters = stationParameters
        self.variables = variables
        self.idx = idx
        self.coreList = ['TEMP',
                         'PRES',
                         'PSAL',
                         'CNDC']
        self.measList = ['TEMP',
                         'PRES',
                         'PSAL',
                         'CNDC', 
                         'DOXY',
                         'CHLA',
                         'CDOM',
                         'NITRATE',
                         'CP',
                         'BBP',
                         'TURBIDITY',
                         'BISULFIDE',
                         'PH_IN_SITU_TOTAL',
                         'DOWN_IRRADIANCE',
                         'UP_RADIANCE',
                         'DOWNWELLING_PAR']

        self.bgcList =   ['DOXY',
                         'CHLA',
                         'CDOM',
                         'NITRATE',
                         'CP',
                         'BBP',
                         'TURBIDITY',
                         'BISULFIDE',
                         'PH_IN_SITU_TOTAL',
                         'DOWN_IRRADIANCE',
                         'UP_RADIANCE',
                         'DOWNWELLING_PAR']

        self.qcDeepPresThreshold = ['1', '2']
        self.qcDeepThreshold = ['1', '2', '3']
        self.qcThreshold = qcThreshold
        self.nProf = nProf
        self.invalidSet = [np.nan, 99999.0]
        self.profileID = profileID
        
    def format_qc_array(self, measQC):
        """ Converts array of QC values (temp, psal, pres, etc) into list"""
        try:
            decodeFormat = self.decodeFormat
            data = [x.decode(decodeFormat) if isinstance(x, bytes) else '4' for x in measQC] # nan are replaced by '4'
        except Exception as err:
            raise Exception('Error while formatting qc: {}'.format(err))
        return data

    def format_adjusted(self, measStr, key, idx):
        adjMeasStr = measStr + '_ADJUSTED'
        measAndQC = {}
        try:
            meas = self.variables[adjMeasStr]['data'][idx]
            if not adjMeasStr in self.variables.keys():
                logging.warning('{} key not found. are you sure it is adjusted?'.format(adjMeasStr))
                meas = [np.NaN]
            elif isinstance(meas, list):
                measAndQC[key] = meas
            else:
                meas = np.nan
            measQC = self.variables[adjMeasStr + '_QC']['data'][idx]
            measAndQC[key+'_qc'] = self.format_qc_array(measQC)
        except Exception as err:
            logging.debug('adjusted value for {0} was not added. {1}'.format(measStr, err))
            measAndQC[key] = np.nan
            measAndQC[key+'_qc'] = np.nan
        return measAndQC

    def format_non_adjusted(self, measStr, key, idx):
        measAndQC = {}
        # get unadjusted value.
        try:
            meas = self.variables[measStr]['data'][idx]
            measAndQC[key] = meas
        except ValueError:
            ValueError('Check data type for measurement {}'.format(measStr))
	        # Sometimes non-adjusted value is invalid.
        try:
            measQC = self.variables[measStr + '_QC']['data'][idx]
            measAndQC[key+'_qc'] = self.format_qc_array(measQC)
        except KeyError:
            raise KeyError('qc not found for {}'.format(measStr))
        return measAndQC

    def format_measurements(self, measStr, idx):
        """
        Combines a measurement's real time and adjusted values into a 1D dataframe.
        An adjusted value replaces each real-time value. 
        Also includes a QC procedure that removes all data that doesn't meet the qc threshhold.

        Delayed mode and Adjusted use adjusted measurements and QC
        """
        key = measStr.lower()
        if (self.data_mode == 'D') or (self.data_mode == 'A'): # both A and D mode use adjusted data only
            #use adjusted data
            measAndQC = self.format_adjusted(measStr, key, idx)
            #  Handles the case when adjusted field is masked. uses adjusted QC and keeps unadjusted data.
            uniqueMeas = np.unique(measAndQC[key])
            if len(uniqueMeas) == 1 and uniqueMeas in self.invalidSet:
                logging.debug('adjusted param is masked for meas: {}. setting to NaN'.format(measStr))
                measAndQC[key] = np.NaN
                missingQC = any( measAndQC[key + '_qc'].astype(str).isin({'1', '2'}) )
                if missingQC:
                    raise ValueError('adjusted param qc is not 3, 4, or masked for masked meas {}. notify dac.'.format(measStr))
        else:
            measAndQC = self.format_non_adjusted(measStr, key, idx)
        measAndQC[key] = [ np.NaN if x > 9999 else x for x in measAndQC[key] ]

        measAndQC[key] = np.array(measAndQC[key])
        measAndQC[key + '_qc'] = np.array(measAndQC[key + '_qc'])
        return measAndQC

    def do_qc_on_meas(self, measAndQC, key):
        """
        QC procedure replaces any row whos qc value does not equal '1' with nan
        """
        try:
            notQcKeep = measAndQC[key + '_qc'] != self.qcThreshold
            measAndQC[key + '_qc'][notQcKeep] = np.NaN
            measAndQC[key][notQcKeep] = np.NaN
        except KeyError:
            raise KeyError('measurement: {0} has no qc.'.format(key))
        return measAndQC

    def drop_nan_from_df(self, df):
        #pressure is the critical feature. If it has a bad qc value, drop the whole row
        if not 'pres_qc' in df.columns:
            raise ValueError('bad pressure qc.')
        df = df[df['pres_qc'] != np.NaN]
        df.dropna(axis=0, how='all', inplace=True)
        # Drops the values where pressure isn't reported
        df.dropna(axis=0, subset=['pres'], inplace=True)
        # Drops the values where both temp and psal aren't reported
        if 'temp' in df.columns and 'psal' in df.columns:
            df.dropna(subset=['temp', 'psal'], how='all', inplace=True)
        elif 'temp' in df.columns: 
            df.dropna(subset=['temp'], how='all', inplace=True)
        elif 'psal' in df.columns:
            df.dropna(subset=['psal'], how='all', inplace=True)
        else:
            raise ValueError('df has neither temp nor psal.')
            
        # profile must include both temperature and pressure
        if not 'pres' in df.columns:
            raise ValueError('df has no pres')
        if not 'temp' in df.columns:
            raise ValueError('df has no temp')
        return df

    @staticmethod
    def merge_dfs(df1, df2):
        '''combins BGC dfs. df1 into df2 for nan in df2'''
        try:
            df1 = df1.set_index('pres')
            df2 = df2.set_index('pres')
        except KeyError:
            raise KeyError('missing pressure axis when merging bgc measurements. notify dacs')
        df = pd.concat( [df1, df2], axis=0, join='outer', sort='true')
        df = df.reset_index()
        
        checkNanColumns = [x for x in df.columns if not x.endswith('_qc') and x != 'pres']
        df.dropna(axis=0, how='all', subset=checkNanColumns, inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        return df

    def create_BGC_DF(self):
        '''
        BGC measurements are found in several indexes. Here we loop through
        each N_PROF and merge using the merge_dfs method.
        '''
        df = self.make_profile_df(self.idx, self.measList, includeQC=False) # note we add pres temp and psal
        if self.nProf == 1:
            df = df.dropna(axis=1, how='all')
        else:
            for idx in range(1, self.nProf):
                profDf = self.make_profile_df(idx, self.measList, includeQC=False)
                if not 'pres' in profDf.columns: # Ignore parameters with no pressure axis.
                    continue
                if set(profDf.columns) == {'pres', 'pres_qc'}:  #  Ignores items not in bgcList (core parameters)
                    continue
                if df.empty:
                    df = profDf
                else:
                    df = self.merge_dfs(df, profDf)
            df = df.dropna(axis=1, how='all')
        return df
    def do_qc_on_deep_meas(self, df, key):
        """
        QC procedure drops any row whos qc value does not equal '1'
        """
        try:
            dfShallow = df[ df['pres'] <= 2000]
            dfShallow = dfShallow[dfShallow[key+'_qc'] == self.qcThreshold]
            dfDeep = df[ df['pres'] > 2000]
            if key == 'pres' or key == 'temp':
                dfDeep = dfDeep[ dfDeep[key+'_qc'].isin(self.qcDeepPresThreshold) ]
            else:
                dfDeep = dfDeep[ dfDeep[key+'_qc'].isin(self.qcDeepThreshold) ]
            df = pd.concat([dfShallow, dfDeep], axis=0 )
        except KeyError:
            raise KeyError('measurement: {0} has no qc.'.format(key))
        return df

    def make_deep_profile_df(self, idx, measList, includeQC=True):
        ''' Deep profiles use pressure in their qc process'''
        df = self.make_profile_df(idx, measList, False)
        cols = [col for col in df.columns.tolist() if not '_qc' in col]  
        for key in cols:
            df = self.do_qc_on_deep_meas(df, key)

        qcColNames = [k for k in df.columns.tolist() if '_qc' in k]  
        if includeQC:
            df = self.drop_nan_from_df(df)
        else:
            df = self.drop_nan_from_df(df)
            df = df.drop(qcColNames, axis=1) # qc values are no longer needed.
        return df

    def make_profile_df(self, idx, measList, includeQC=True):
        '''Profile measurements are gathered in a dataframe'''
        measStrings = self.stationParameters
        profileDict = {}
        arrayLen = len(self.variables['PRES']['data'][self.idx])
        for measStr in measStrings:
            key = measStr.lower()
            keyBase =  re.sub(r'\d+', '', measStr)
            if not keyBase in measList: # sometimes meas has digits
                continue
            measAndQC = self.format_measurements(measStr, idx)
            if includeQC:
                measAndQC = self.do_qc_on_meas(measAndQC, key)
            if np.isnan(measAndQC[key]).all(): # don't include a column of nan
                continue
            # append with index conserved
            if measAndQC[key].size == arrayLen:
                profileDict[key] = measAndQC[key]
                profileDict[key + '_qc'] = measAndQC[key + '_qc']
            else:
                raise ValueError('array dim mismatch')
        qcColNames = [k for k in profileDict.keys() if '_qc' in k]
        df = pd.DataFrame(profileDict)
        if includeQC:
            df = self.drop_nan_from_df(df)
            df.drop(qcColNames, axis = 1, inplace = True) # qc values are no longer needed.
        return df