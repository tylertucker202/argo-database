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
import pdb
import warnings

warnings.simplefilter('error', RuntimeWarning)


class netCDFToDoc(object):
    def __init__(self,
                 variables,
                 dacName,
                 refDate,
                 remotePath,
                 stationParameters,
                 platformNumber,
                 idx=0,
                 qcThreshold='1'):
        logging.debug('initializing netCDFToDoc')
        self.platformNumber = platformNumber
        self.qcThreshold = qcThreshold
        self.variables = variables
        self.idx = idx
        self.cycleNumber = int(self.variables['CYCLE_NUMBER'][self.idx].astype(str))
        self.profileDoc = dict()
        
        # populate profileDoc
        self.make_profile_dict(dacName, refDate, remotePath, stationParameters)
    
    def get_profile_doc(self):
        return self.profileDoc

    def format_measurments(self, measStr):
        """
        Combines a measurement's real time and adjusted values into a 1D dataframe.
        An adjusted value replaces each real-time value. 
        Also includes a QC procedure that removes all data that doesn't meet the qc threshhold.
        """
        def format_qc_array(array):
            """ Converts array of QC values (temp, psal, pres, etc) into list"""
            if type(array) == np.ndarray:
                data = [x.astype(str) for x in array]
            elif type(array) == np.ma.core.MaskedArray:
                data = array.data
                try:
                    data = np.array([x.astype(str) for x in data])
                except NotImplementedError:
                    logging.warning('NotImplemented Error for idx: {1}'.format(self.idx))
            return data

        df = pd.DataFrame()
        
        
        not_adj = measStr.lower()+'_not_adj'
        adj = measStr.lower()
        
        doc_key = measStr.lower()

        if (self.profileDoc['DATA_MODE'] == 'D') or (self.profileDoc['DATA_MODE'] == 'A'):
            #use adjusted data
            try:
                if type(self.variables[measStr + '_ADJUSTED'][self.idx, :]) == np.ndarray:
                    df[doc_key] = self.variables[measStr + '_ADJUSTED'][self.idx, :]
            except KeyError:
                logging.debug('adjusted value for {} does not exist'.format(measStr))
                df[doc_key] = np.nan
            except RuntimeWarning as err:
                logging.warning('runtime warning when getting adjusted value. Reason: {}'.format(err.args))
                logging.warning(('Float: {0} cycle: {1} may be missing an adjusted value.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber)))
                
            else:  # sometimes a masked array is used
                try:
                    df[doc_key] = self.variables[measStr + '_ADJUSTED'][self.idx, :].data
                except ValueError:
                    logging.warning('Value error while formatting measurement {}: check data type'.format(measStr))
            try:
                df.loc[df[doc_key] >= 99999, adj] = np.NaN
            except KeyError:
                logging.warning('key not found...')
            try:
                df[doc_key+'_qc'] = format_qc_array(self.variables[measStr + '_ADJUSTED_QC'][self.idx, :])
            except KeyError:
                logging.warning('qc not found for {}'.format(measStr))
                logging.warning('returning empty dataframe')
                return pd.DataFrame()
        else:
            # get unadjusted value. Types vary from arrays to masked arrays.
            if type(self.variables[measStr][self.idx, :]) == np.ndarray:
                df[doc_key] = self.variables[measStr][self.idx, :]
            else:  # sometimes a masked array is used
                try:
                    df[doc_key] = self.variables[measStr][self.idx, :].data
                except ValueError:
                    logging.warning('Value error while formatting measurement {}: check data type'.format(measStr))
	    # Sometimes non-adjusted value is invalid.
            # df.ix[df[doc_key] >= 99999, not_adj] = np.NaN # not sure this is needed anymore
            try:
                df[doc_key+'_qc'] = format_qc_array(self.variables[measStr + '_QC'][self.idx, :])
            except KeyError:
                logging.warning('qc not found for {}'.format(measStr))
                logging.warning('returning empty dataframe')
                return pd.DataFrame()
            
        """
        QC procedure drops any row whos qc value does not equal '1'
        """
        try:
            df = df[df[doc_key+'_qc'] == self.qcThreshold]
        except KeyError:
            logging.warning('measurement: {0} has no qc.'
                      ' returning empty dataframe'.format(doc_key))
            return pd.DataFrame()
            df.shape
        return df

    def makeProfileDf(self):    
        profileDf = pd.DataFrame()
        keys = self.variables.keys()
        #  Profile measurements are gathered in a dataframe
        measList = ['TEMP', 'PRES', 'PSAL', 'CNDC', 'DOXY', 'CHLA', 'CDOM', 'NITRATE']
        for measStr in measList:
            if measStr in keys:
                meas_df = self.format_measurments(measStr)
                # append with index conserved
                profileDf = pd.concat([profileDf, meas_df], axis=1)

        #pressure is the critical feature. If it has a bad qc value, drop the whole row
        if not 'pres_qc' in profileDf.columns:
            raise ValueError('Float: {0} has bad pressure qc.'
                          ' Not going to add'.format(self.platformNumber))
        profileDf = profileDf[profileDf['pres_qc'] != np.NaN]
        profileDf.dropna(axis=0, how='all', inplace=True)
        # Drops the values where pressure isn't reported
        profileDf.dropna(axis=0, subset=['pres'], inplace=True)
        # Drops the values where both temp and psal aren't reported
        if 'temp' in profileDf.columns and 'psal' in profileDf.columns:
            profileDf.dropna(subset=['temp', 'psal'], how='all', inplace=True)
        elif 'temp' in profileDf.columns: 
            profileDf.dropna(subset=['temp'], how='all', inplace=True)
        elif 'psal' in profileDf.columns:
            profileDf.dropna(subset=['psal'], how='all', inplace=True)
        else:
            raise ValueError('Float: {0} cycle: {1} has neither temp nor psal.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
        profileDf.fillna(-999, inplace=True) # API needs all measurements to be a number
        qcColNames = [k for k in profileDf.columns.tolist() if '_qc' in k]  # qc values are no longer needed.
        profileDf.drop(qcColNames, axis = 1, inplace = True)
        return profileDf

    def add_string_values(self, valueName):
        """
        Used to add POSITIONING_SYSTEM PLATFORM_TYPE DATA_MODE and PI_NAME fields.
        if missing or is masked, values will not be added to the document.
        """
        try:
            if type(self.variables[valueName][self.idx]) == np.ma.core.MaskedConstant:
                value = self.variables[valueName][self.idx].astype(str)
                logging.debug('Float: {0} cycle: {1} has unknown {2}.'
                          ' Not going to add item to document'.format(self.cycleNumber, valueName))
            else:
                if valueName == 'DATA_MODE':
                    value = self.variables[valueName][self.idx].astype(str)
                else:
                    value = ''.join([(x.astype(str)) for x in self.variables[valueName][self.idx].data])
                    value = value.strip(' ')
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
        try:
            profileDf = self.makeProfileDf()
            if profileDf.shape[0] == 0:
                raise AttributeError('Float: {0} cycle: {1} has no valid measurements.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
        except ValueError as err:
            logging.warning('Float: {0} cycle: {1} profileDf not created.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
            logging.warning('Reason: {}'.format(err.args))
            raise ValueError('Reason: {}'.format(err.args))
        except KeyError as err:
            logging.warning('Key error. Float: {0} cycle: {1} profileDf not created.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
            logging.warning('Reason: {}'.format(err.args))            
        except UnboundLocalError as err:
            logging.warning('Float: {0} cycle: {1} profileDf not created.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
            logging.warning('Reason: {}'.format(err.args))
            raise UnboundLocalError('Reason: {}'.format(err.args))
        except AttributeError as err:
            logging.debug('Float: {0} cycle: {1} has no valid measurements.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
            raise AttributeError('Float: {0} cycle: {1} has no valid measurements.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
        except:
            logging.warning('Float: {0} cycle: {1} profileDf not created.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
            logging.warning('Reason: unknown')
            pdb.set_trace()
            raise UnboundLocalError('Reason: unknown')            

        maxPres = profileDf.pres.max()
        self.profileDoc['max_pres'] = int(maxPres)
        self.profileDoc['measurements'] = profileDf.astype(np.float64).to_dict(orient='records')
        if type(self.variables['JULD'][self.idx]) == np.ma.core.MaskedConstant:
            raise AttributeError('Float: {0} cycle: {1} has unknown date.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))

        date = refDate + timedelta(self.variables['JULD'][self.idx])
        self.profileDoc['date'] = date
        phi = self.variables['LATITUDE'][self.idx]
        lam = self.variables['LONGITUDE'][self.idx]
        if type(phi) == np.ma.core.MaskedConstant or type(lam) == np.ma.core.MaskedConstant:
            raise AttributeError('Float: {0} cycle: {1} has unknown lat-lon.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))

        try:
            positionQC = str(self.variables['POSITION_QC'][self.idx].astype(int))
        except AttributeError:
            if type(self.variables['POSITION_QC'][self.idx] == np.ma.core.MaskedConstant):
                positionQC = str(self.variables['POSITION_QC'][self.idx].data.astype(int))
            else:
                raise AttributeError('error with position_qc. Not going to add.')
        except:
            logging.warning('Float: {0} cycle: {1} profileDf not created.'
                          ' Not going to add'.format(self.platformNumber, self.cycleNumber))
            logging.warning('check position qc')
        #  currently does not add do qc on position
        if positionQC == 4:
            raise ValueError('position_qc is a 4. Not going to add.')
        self.profileDoc['position_qc'] = positionQC
        self.profileDoc['cycle_number'] = self.cycleNumber
        self.profileDoc['lat'] = phi
        self.profileDoc['lon'] = lam
        self.profileDoc['geoLocation'] = {'type': 'Point', 'coordinates': [lam, phi]}
        self.profileDoc['geo2DLocation'] = [lam, phi]
        self.profileDoc['dac'] = dacName
        self.profileDoc['platform_number'] = self.platformNumber
        self.profileDoc['station_parameters'] = stationParameters
        profile_id = self.platformNumber + '_' + str(self.cycleNumber)
        url = remotePath
        self.profileDoc['nc_url'] = url
        """
        Normally, the floats take measurements on the ascent. 
        In the event that the float takes measurements on the descent, the
        cycle number doesn't change. So, to have a unique identifer, this 
        the _id field has a 'D' appended
        """
        if type(self.variables['DIRECTION'][self.idx]) == np.ma.core.MaskedConstant:
            logging.debug('direction unknown')
        else:
            direction = self.variables['DIRECTION'][self.idx].astype(str)
            if direction == 'D':
                profile_id += 'D'
            self.profileDoc['_id'] = profile_id
