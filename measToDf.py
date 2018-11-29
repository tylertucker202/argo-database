#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 27 16:59:48 2018

@author: tyler
"""
import numpy as np
import pandas as pd
import logging
import pdb

class measToDf(object):
    def __init__(self, variables,
                 platformNumber,
                 idx=0,
                 qcThreshold='1',
                 nProf=1):
        logging.debug('initializing measToDf')
        self.platformNumber = platformNumber
        self.variables = variables
        self.idx = idx
        self.cycleNumber = int(self.variables['CYCLE_NUMBER'][idx].astype(str))
        self.profileId = self.platformNumber + '_' + str(self.cycleNumber)
        self.measList = ['TEMP', 'PRES', 'PSAL', 'CNDC', 'DOXY', 'CHLA', 'CDOM', 'NITRATE']
        self.bgcKeys = ['DOXY', 'CHLA', 'CDOM', 'NITRATE']
        self.qcDeepThreshold = ['1', '2', '3']
        self.qcThreshold = qcThreshold
        self.nProf = nProf
        
    @staticmethod
    def format_qc_array(array, idx):
        """ Converts array of QC values (temp, psal, pres, etc) into list"""
        if isinstance(array, np.ndarray):
            data = [x.astype(str) for x in array]
        elif type(array) == np.ma.core.MaskedArray:
            data = array.data
            try:
                data = np.array([x.astype(str) for x in data])
            except NotImplementedError:
                raise NotImplementedError('NotImplemented Error for idx: {1}'.format(idx))
        return data    

    def format_measurments(self, measStr, idx):
        """
        Combines a measurement's real time and adjusted values into a 1D dataframe.
        An adjusted value replaces each real-time value. 
        Also includes a QC procedure that removes all data that doesn't meet the qc threshhold.
        """

        df = pd.DataFrame()
        adj = measStr.lower()
        
        doc_key = measStr.lower()

        if (self.profileDoc['DATA_MODE'] == 'D') or (self.profileDoc['DATA_MODE'] == 'A'):
            #use adjusted data
            try:
                if isinstance(self.variables[measStr + '_ADJUSTED'][idx, :], np.ndarray):
                    df[doc_key] = self.variables[measStr + '_ADJUSTED'][idx, :]
            except KeyError:
                logging.debug('adjusted value for {} does not exist'.format(measStr))
                df[doc_key] = np.nan
            except RuntimeWarning as err:
                raise RuntimeWarning('Profile:{0} measStr: {1} runtime warning when getting adjusted value. Reason: {2}'.format(self.profileId, measStr, err.args))

                
            else:  # sometimes a masked array is used
                try:
                    df[doc_key] = self.variables[measStr + '_ADJUSTED'][idx, :].data
                except ValueError:
                    raise ValueError('Value error while formatting measurement {}: check data type'.format(measStr))
            try:
                df.loc[df[doc_key] >= 99999, adj] = np.NaN
            except KeyError:
                raise KeyError('key not found...')
            try:
                df[doc_key+'_qc'] = self.format_qc_array(self.variables[measStr + '_ADJUSTED_QC'][idx, :], idx)
            except KeyError:
                raise KeyError('qc not found for {}'.format(measStr))
        else:
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
                df[doc_key+'_qc'] = self.format_qc_array(self.variables[measStr + '_QC'][idx, :], idx)
            except KeyError:
                raise KeyError('qc not found for {}'.format(measStr))
                return pd.DataFrame()

        return df
    
    def do_qc_on_meas(self, df, measStr):
        """
        QC procedure drops any row whos qc value does not equal '1'
        """
        try:
            if self.deepFloat:
                dfShallow = df[ df['pres'] <= 2000]
                dfDeep = df[ df['pres'] > 2000]
                df = pd.concat([dfShallow, dfDeep[dfDeep[measStr+'_qc'] in self.qcDeepThreshold]], axis=0 )
            else:
                df = df[df[measStr+'_qc'] == self.qcThreshold]
        except KeyError:
            raise KeyError('measurement: {0} has no qc.'
                      ' returning empty dataframe'.format(measStr))
            return pd.DataFrame()
        return df

    def drop_nan_from_df(self, df):
        #pressure is the critical feature. If it has a bad qc value, drop the whole row
        if not 'pres_qc' in df.columns:
            raise ValueError('Float: {0} has bad pressure qc.'
                          ' Not going to add'.format(self.platformNumber))
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
            raise ValueError('Profile:{0} has neither temp nor psal.'
                          ' Not going to add'.format(self.profileId))
        df.fillna(-999, inplace=True) # API needs all measurements to be a number
        return df
    
    def createBGC(self):
        df = pd.DataFrame()
        if self.nProf > 1:
            pdb.set_trace()
        for idx in range(self.nProf):
            profDf = self.make_profile_df(idx, includeQC=False)
            #  TODO: perform a merge on parameters that exist on both
            df = pd.concat([df, profDf], axis = 0)
        return df.astype(np.float64).to_dict(orient='records')

    def make_profile_df(self, idx, includeQC=True):
        df = pd.DataFrame()
        keys = self.variables.keys()
        #  Profile measurements are gathered in a dataframe
        for measStr in self.measList:
            if measStr in keys:
                meas_df = self.format_measurments(measStr, idx)
                if includeQC:
                    meas_df = self.do_qc_on_meas(meas_df, measStr.lower())
                # append with index conserved
                df = pd.concat([df, meas_df], axis=1)
        qcColNames = [k for k in df.columns.tolist() if '_qc' in k]  
        if includeQC:
            df = self.drop_nan_from_df(df)
            df.drop(qcColNames, axis = 1, inplace = True) # qc values are no longer needed.
        else:
            df.fillna(-999, inplace=True)
        return df