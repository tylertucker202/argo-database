#!/usr/bin/env python3
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
                 idx=0,
                 qcThreshold='1'):
        logging.debug('initializing measToDf')
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
        
    @staticmethod
    def format_qc_array(array):
        """ Converts array of QC values (temp, psal, pres, etc) into list"""
        if isinstance(array, np.ndarray):
            data = [x.astype(str) for x in array]
        elif type(array) == np.ma.core.MaskedArray:
            data = array.data
            try:
                data = np.array([x.astype(str) for x in data])
            except NotImplementedError:
                raise NotImplementedError('NotImplemented Error while formatting qc')
        return data    

    def format_adjusted(self, measStr, doc_key, idx):
        df = pd.DataFrame()
        try:
            if isinstance(self.variables[measStr + '_ADJUSTED'][idx, :], np.ndarray):
                df[doc_key] = self.variables[measStr + '_ADJUSTED'][idx, :]
        except KeyError:
            logging.debug('adjusted value for {} does not exist'.format(measStr))
            df[doc_key] = np.nan
        except RuntimeWarning as err:
            raise RuntimeWarning('measStr: {1} runtime warning when getting adjusted value. Reason: {2}'.format(measStr, err.args))
        else:  # sometimes a masked array is used
            try:
                df[doc_key] = self.variables[measStr + '_ADJUSTED'][idx, :].data
            except ValueError:
                raise ValueError('Value error while formatting measurement {}: check data type'.format(measStr))
        try:
            df[doc_key+'_qc'] = self.format_qc_array(self.variables[measStr + '_ADJUSTED_QC'][idx, :])
        except KeyError:
            raise KeyError('qc not found for {}'.format(measStr))
        return df

    def format_non_adjusted(self, measStr, doc_key, idx):
        df = pd.DataFrame()
        # get unadjusted value. Types vary from arrays to masked arrays.
        if isinstance(self.variables[measStr][idx, :], np.ndarray):
            df[doc_key] = self.variables[measStr][idx, :]
        else:  # sometimes a masked array is used
            try:
                df[doc_key] = self.variables[measStr][idx, :].data
            except ValueError:
                ValueError('Check data type for measurement {}'.format(measStr))
	        # Sometimes non-adjusted value is invalid.
        try:
            df[doc_key+'_qc'] = self.format_qc_array(self.variables[measStr + '_QC'][idx, :])
        except KeyError:
            raise KeyError('qc not found for {}'.format(measStr))
            return pd.DataFrame()
        return df

    def format_measurments(self, measStr, idx):
        """
        Combines a measurement's real time and adjusted values into a 1D dataframe.
        An adjusted value replaces each real-time value. 
        Also includes a QC procedure that removes all data that doesn't meet the qc threshhold.
        """
        adj = measStr.lower()
        doc_key = measStr.lower()
        if (self.profileDoc['DATA_MODE'] == 'D') or (self.profileDoc['DATA_MODE'] == 'A'):
            #use adjusted data
            df = self.format_adjusted(measStr, doc_key, idx)
            #  Handles the case when adjusted field is masked.
            if len(df[doc_key].unique()) == 1 and df[doc_key].unique() in self.invalidSet:
                logging.debug('adjusted param is masked for meas: {}'.format(measStr))
                df = self.format_non_adjusted(measStr, doc_key, idx)
        else:
            df = self.format_non_adjusted(measStr, doc_key, idx)
        df.loc[df[doc_key] > 9999, adj] = np.NaN
        return df

    def do_qc_on_meas(self, df, measStr):
        """
        QC procedure drops any row whos qc value does not equal '1'
        """
        try:
            df = df[df[measStr+'_qc'] == self.qcThreshold]
        except KeyError:
            raise KeyError('measurement: {0} has no qc.'
                      ' returning empty dataframe'.format(measStr))
            return pd.DataFrame()
        if df.empty:
            return pd.DataFrame()
        return df

    def do_qc_on_deep_meas(self, df, measStr):
        """
        QC procedure drops any row whos qc value does not equal '1'
        """
        try:
            dfShallow = df[ df['pres'] <= 2000]
            dfShallow = dfShallow[dfShallow[measStr+'_qc'] == self.qcThreshold]
            dfDeep = df[ df['pres'] > 2000]
            if measStr == 'pres' or measStr == 'temp':
                dfDeep = dfDeep[ dfDeep[measStr+'_qc'].isin(self.qcDeepPresThreshold) ]
            else:
                dfDeep = dfDeep[ dfDeep[measStr+'_qc'].isin(self.qcDeepThreshold) ]
            df = pd.concat([dfShallow, dfDeep], axis=0 )
        except KeyError:
            raise KeyError('measurement: {0} has no qc.'
                      ' returning empty dataframe'.format(measStr))
            return pd.DataFrame()
        return df

    def drop_nan_from_df(self, df):
        #pressure is the critical feature. If it has a bad qc value, drop the whole row
        if not 'pres_qc' in df.columns:
            raise ValueError('bad pressure qc.'.format(self.platformNumber))
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
        '''combins df1 into df2 for nan in df2'''
        df1 = df1.astype(float).replace(-999, np.NaN)
        df2 = df2.astype(float).replace(-999, np.NaN)
        try:
            df1 = df1.set_index('pres')
            df2 = df2.set_index('pres')
        except KeyError as err:
            raise KeyError('missing pressure axis when merging bgc measurements')
        df = pd.concat( [df1, df2], axis=0, join='outer', sort='true')
        df = df.reset_index()
        
        checkNanColumns = [x for x in df.columns if not x.endswith('_qc') and x != 'pres']
        df.dropna(axis=0, how='all', subset=checkNanColumns, inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        #reformat
        df.fillna(-999, inplace=True)
        qcCol = [x for x in df.columns if x.endswith('_qc')]
        df[qcCol] = df[qcCol].astype(int).astype(str)
        return df

    @staticmethod
    def format_bgc_df(df):
        try:
            df = df.astype(float).replace(-999, np.NaN)
        except ValueError as err:
            raise ValueError('invalid values in bgc measurements {}'.format(err))
        df.dropna(axis=0, how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        df.fillna(-999, inplace=True)
        qcCol = [x for x in df.columns if x.endswith('_qc')]
        df[qcCol] = df[qcCol].astype(int).astype(str)
        return df
        
    
    def create_BGC(self):
        ''' BGC measurements are found in several indexes. Here we loop through
        each N_PROF and merge using the merge_dfs method.'''
        df = self.make_profile_df(self.idx, self.measList, includeQC=False) # note we add pres temp and psal
        if self.nProf == 1:
            df = self.format_bgc_df(df)
            return df.astype(np.float64).to_dict(orient='records')
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
            df = self.format_bgc_df(df)
            return df.astype(np.float64).to_dict(orient='records')
            
    def make_deep_profile_df(self, idx, measList, includeQC=True):
        ''' Deep profiles use pressure in their qc process'''
        df = pd.DataFrame()
        keys = self.stationParameters[idx]
        pres = self.format_measurments('PRES', idx)
        for key in keys:
            if re.sub(r'\d+', '', key) in measList: # sometimes meas has digits
                meas_df = self.format_measurments(key, idx)
            else:
                continue
            if includeQC and key != 'PRES':
                meas_df['pres'] = pres['pres']
                meas_df['pres_qc'] = pres['pres_qc']
                meas_df = self.do_qc_on_deep_meas(meas_df, key.lower())
                meas_df.drop(['pres', 'pres_qc'], axis=1, inplace=True)
                df = pd.concat([df, meas_df], axis=1)
        pres = self.do_qc_on_deep_meas(pres, 'pres')
        df = pd.concat([pres, df], axis=1)  # join pressure axis
        qcColNames = [k for k in df.columns.tolist() if '_qc' in k]  
        if includeQC:
            df = self.drop_nan_from_df(df)
        else:
            df = self.drop_nan_from_df(df)
            df.drop(qcColNames, axis = 1, inplace = True) # qc values are no longer needed.
        df.fillna(-999, inplace=True) # API needs all measurements to be a number
        return df

    def make_profile_df(self, idx, measList, includeQC=True):
        df = pd.DataFrame()
        keys = self.stationParameters[idx]
        #  Profile measurements are gathered in a dataframe
        for key in keys:
            if re.sub(r'\d+', '', key) in measList: # sometimes meas has digits
                meas_df = self.format_measurments(key, idx)
                if includeQC:
                    meas_df = self.do_qc_on_meas(meas_df, key.lower())
                # append with index conserved
                df = pd.concat([df, meas_df], axis=1)
        qcColNames = [k for k in df.columns.tolist() if '_qc' in k]  
        if includeQC:
            df = self.drop_nan_from_df(df)
            df.drop(qcColNames, axis = 1, inplace = True) # qc values are no longer needed.
        df.fillna(-999, inplace=True) # API needs all measurements to be a number
        return df
