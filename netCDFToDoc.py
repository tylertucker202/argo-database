#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  4 15:46:14 2018
@author: tyler
"""
import logging
import pandas as pd
import numpy as np
from datetime import timedelta
import warnings
import pdb

warnings.simplefilter('error', RuntimeWarning)


class netCDFToDoc(object):

    def __init__(self, variables,
                 dacName,
                 refDate,
                 remotePath,
                 stationParameters,
                 platformNumber,
                 idx=0,
                 qcThreshold='1'):
        logging.debug('initializing netCDFToDoc')
        self.platformNumber = platformNumber
        self.variables = variables
        self.idx = idx
        self.cycleNumber = int(self.variables['CYCLE_NUMBER'][self.idx].astype(str))
        self.profileId = self.platformNumber + '_' + str(self.cycleNumber)
        self.profileDoc = dict()
        self.deepFloatWMO = ['838' ,'849','862','874','864']  # Deep floats don't have QC
        self.measList = ['TEMP', 'PRES', 'PSAL', 'CNDC', 'DOXY', 'CHLA', 'CDOM', 'NITRATE']
        self.qcDeepThreshold = ['1', '2', '3']
        self.qcThreshold = qcThreshold
        
        # populate profileDoc
        self.make_profile_dict(dacName, refDate, remotePath, stationParameters)
    
    def get_profile_doc(self):
        return self.profileDoc
    
    def format_qc_array(self, array):
        """ Converts array of QC values (temp, psal, pres, etc) into list"""
        if isinstance(array, np.ndarray):
            data = [x.astype(str) for x in array]
        elif type(array) == np.ma.core.MaskedArray:
            data = array.data
            try:
                data = np.array([x.astype(str) for x in data])
            except NotImplementedError:
                raise NotImplementedError('NotImplemented Error for idx: {1}'.format(self.idx))
        return data    

    def format_measurments(self, measStr):
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
                if isinstance(self.variables[measStr + '_ADJUSTED'][self.idx, :], np.ndarray):
                    df[doc_key] = self.variables[measStr + '_ADJUSTED'][self.idx, :]
            except KeyError:
                logging.debug('adjusted value for {} does not exist'.format(measStr))
                df[doc_key] = np.nan
            except RuntimeWarning as err:
                raise RuntimeWarning('Profile:{0} runtime warning when getting adjusted value. Reason: {1}'.format(self.profileId, err.args))

                
            else:  # sometimes a masked array is used
                try:
                    df[doc_key] = self.variables[measStr + '_ADJUSTED'][self.idx, :].data
                except ValueError:
                    raise ValueError('Value error while formatting measurement {}: check data type'.format(measStr))
            try:
                df.loc[df[doc_key] >= 99999, adj] = np.NaN
            except KeyError:
                raise KeyError('key not found...')
            try:
                df[doc_key+'_qc'] = self.format_qc_array(self.variables[measStr + '_ADJUSTED_QC'][self.idx, :])
            except KeyError:
                raise KeyError('qc not found for {}'.format(measStr))
        else:
            # get unadjusted value. Types vary from arrays to masked arrays.
            if isinstance(self.variables[measStr][self.idx, :], np.ndarray):
                df[doc_key] = self.variables[measStr][self.idx, :]
            else:  # sometimes a masked array is used
                try:
                    df[doc_key] = self.variables[measStr][self.idx, :].data
                except ValueError:
                    ValueError('Check data type for measurement {}'.format(measStr))
	        # Sometimes non-adjusted value is invalid.
            try:
                df[doc_key+'_qc'] = self.format_qc_array(self.variables[measStr + '_QC'][self.idx, :])
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
        qcColNames = [k for k in df.columns.tolist() if '_qc' in k]  # qc values are no longer needed.
        df.drop(qcColNames, axis = 1, inplace = True)    
        return df

    def make_profile_df(self, includeQC=True):
        df = pd.DataFrame()
        keys = self.variables.keys()
        #  Profile measurements are gathered in a dataframe
        for measStr in self.measList:
            if measStr in keys:
                meas_df = self.format_measurments(measStr)
                if includeQC:
                    meas_df = self.do_qc_on_meas(meas_df, measStr.lower())
                # append with index conserved
                df = pd.concat([df, meas_df], axis=1)
        if includeQC:
            df = self.drop_nan_from_df(df)
        else:
            df.fillna(-999, inplace=True)
        return df

    def add_string_values(self, valueName):
        """
        Used to add POSITIONING_SYSTEM PLATFORM_TYPE DATA_MODE and PI_NAME fields.
        if missing or is masked, values will not be added to the document.
        """
        try:
            if isinstance(self.variables[valueName][self.idx], np.ma.core.MaskedConstant):
                value = self.variables[valueName][self.idx].astype(str).item()
                logging.debug('Profile:{0} has unknown {1}.'
                          ' Not going to add item to document'.format(self.profileId, valueName))
            else:
                if valueName == 'DATA_MODE':
                    value = self.variables[valueName][self.idx].astype(str).item()
                else:
                    value = ''.join([(x.astype(str).item()) for x in self.variables[valueName][self.idx].data])
                    value = value.strip(' ')

            if valueName == 'INST_REFERENCE': # renames 'INST_REFERENCE' to 'PLATFORM_TYPE'
                self.profileDoc['PLATFORM_TYPE'] = value
            else:
                self.profileDoc[valueName] = value

        except KeyError:
            if valueName == 'PLATFORM_TYPE':
                instRefExists = 'INST_REFERENCE' in self.variables.keys()
                logging.debug('PLATFORM_TYPE not found.'
                              'INST_REFERENCE exists? {}'.format(instRefExists))
                raise KeyError    
            logging.debug('unknown key {0}.'
                          ' Not going to add item to document'.format(valueName))
        except:
            logging.debug('error when adding {0} to document.'
                          ' Not going to add item to document'.format(valueName))
    
    def add_max_min_pres(self, df, param, maxBoolean):
        if not param in df.columns:
            return
        try:
            reducedDf = df[ df[param] != -999 ]['pres']
            if reducedDf.empty:
                return
            if maxBoolean:
                presValue = reducedDf.max()
                presValue = presValue.astype(np.float64)
                maxMin = 'max'
            else:
                presValue = reducedDf.min()
                presValue = presValue.astype(np.float64)
                maxMin = 'min'

            paramName = 'pres_' + maxMin + '_for_' + param.upper()
            self.profileDoc[paramName] = presValue
        except:
            logging.warning('Profile {}: unable to get presmax/min, unknown exception.'.format(self.profileId))

    def add_bgc_flag(self):
        bgcKeys = ['CNDC', 'DOXY', 'CHLA', 'CDOM', 'NITRATE']
        if any (k in bgcKeys for k in self.variables.keys()):
            self.profileDoc['containsBGC'] = 1
            df = self.make_profile_df(includeQC=False)
            self.profileDoc['bgcMeas'] = df.astype(np.float64).to_dict(orient='records')
    
    def check_if_deep_profile(self):
        try:
            if self.profileDoc['WMO_INST_TYPE'] in self.deepFloatWMO:
                deepFloat = True
            else:
                deepFloat = False
        except NameError:
            deepFloat = False
        return deepFloat

    def make_profile_dict(self, dacName, refDate, remotePath, stationParameters):
        """
        Takes a profile measurement and formats it into a dictionary object.
        """
        self.add_string_values('POSITIONING_SYSTEM')
        # sometimes INST_REFERENCE is used instead of PLATFORM_TYPE
        try:
            self.add_string_values('PLATFORM_TYPE')
        except KeyError:
            self.add_string_values('INST_REFERENCE')
        self.add_string_values('DATA_MODE')
        self.add_string_values('PI_NAME')
        self.add_string_values('WMO_INST_TYPE')
        self.deepFloat = self.check_if_deep_profile()
        try:
            profileDf = self.make_profile_df(includeQC=True)
        except ValueError as err:
            raise ValueError('Profile:{0} has ValueError:{1} profileDf not created.'
                          ' Not going to add.'.format(self.profileId, err.args))
        except KeyError as err:
            raise ValueError('Profile:{0} has KeyError:{1} profileDf not created.'
                          ' Not going to add.'.format(self.profileId, err.args))
        except UnboundLocalError as err:
            raise UnboundLocalError('Profile:{0} has UnboundLocalError:{1} profileDf not created.'
                          ' Not going to add'.format(self.profileId, err.args))
        except AttributeError as err:
            raise AttributeError('Profile:{0} has AttributeError:{1} profileDf not created.'
                          ' Not going to add.'.format(self.profileId, err.args))
        except Exception as err:
            raise UnboundLocalError('Profile:{0} has unknown error {1}. profileDf not created.'
                          ' Not going to add'.format(self.profileId, err.args))
        self.profileDoc['measurements'] = profileDf.astype(np.float64).to_dict(orient='records')
        
        
        
        self.add_max_min_pres(profileDf, 'temp', maxBoolean=True)
        self.add_max_min_pres(profileDf, 'temp', maxBoolean=False)
        self.add_max_min_pres(profileDf, 'psal', maxBoolean=True)
        self.add_max_min_pres(profileDf, 'psal', maxBoolean=False)
        
        maxPres = profileDf.pres.max()
        self.profileDoc['max_pres'] = np.float64(maxPres)
        if isinstance(self.variables['JULD'][self.idx], np.ma.core.MaskedConstant):
            raise AttributeError('Profile:{0} has unknown date.'
                          ' Not going to add'.format(self.profileId))

        date = refDate + timedelta(self.variables['JULD'][self.idx].item())
        self.profileDoc['date'] = date
        
        try:
            dateQC = self.variables['JULD_QC'][self.idx].astype(np.float64).item()
            self.profileDoc['date_qc'] = dateQC
        except AttributeError:
            if isinstance(self.variables['JULD_QC'][self.idx], np.ma.core.MaskedConstant):
                dateQC = np.float64(self.variables['JULD_QC'][self.idx].item())
                self.profileDoc['date_qc'] = dateQC
            else:
                raise AttributeError('error with date_qc. Not going to add.')
        
        phi = self.variables['LATITUDE'][self.idx].item()
        lam = self.variables['LONGITUDE'][self.idx].item()
        if isinstance(phi, np.ma.core.MaskedConstant) or isinstance(lam, np.ma.core.MaskedConstant):
            raise AttributeError('Profile:{0} has unknown lat-lon.'
                          ' Not going to add'.format(self.profileId))

        try:
            positionQC = self.variables['POSITION_QC'][self.idx].astype(np.float64).item()
        except AttributeError:
            if type(self.variables['POSITION_QC'][self.idx] == np.ma.core.MaskedConstant):
                positionQC = self.variables['POSITION_QC'][self.idx].data.astype(np.float64).item()
            else:
                raise AttributeError('error with position_qc. Not going to add.')
        except ValueError as err:
            raise ValueError('Profile:{0} not created. Error {1}'
                          ' Not going to add'.format(self.profileId, err))
        except Exception as err:
            raise Exception('Profile:{0} not created. Error {1}'
                          ' Not going to add'.format(self.profileId, err))
        if positionQC == 4:
            raise ValueError('position_qc is a 4. Not going to add.')

        self.profileDoc['position_qc'] = positionQC
        self.profileDoc['cycle_number'] = self.cycleNumber
        self.profileDoc['lat'] = phi
        self.profileDoc['lon'] = lam
        self.profileDoc['dac'] = dacName
        self.profileDoc['geoLocation'] = {'type': 'Point', 'coordinates': [lam, phi]}
        self.profileDoc['platform_number'] = self.platformNumber
        self.profileDoc['station_parameters'] = stationParameters
        profile_id = self.platformNumber + '_' + str(self.cycleNumber)
        url = remotePath
        self.profileDoc['nc_url'] = url
        
        self.add_bgc_flag()

        """
        Normally, the floats take measurements on the ascent. 
        In the event that the float takes measurements on the descent, the
        cycle number doesn't change. So, to have a unique identifer, this 
        the _id field has a 'D' appended
        """
        if isinstance(self.variables['DIRECTION'][self.idx], np.ma.core.MaskedConstant):
            logging.debug('direction unknown')
        else:
            direction = self.variables['DIRECTION'][self.idx].astype(str).item()
            if direction == 'D':
                profile_id += 'D'
            self.profileDoc['DIRECTION'] = 'D'
            self.profileDoc['_id'] = profile_id