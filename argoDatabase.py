# -*- coding: utf-8 -*-
import pymongo
import os
import re
import glob
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset
import bson.errors
import sys
from bson.decimal128 import Decimal128
import pdb

class argoDatabase(object):
    def __init__(self,
                 db_name,
                 collection_name, 
                 replace_profile=False,
                 qcThreshold='1', 
                 dbDumpThreshold=10000):
        logging.debug('initializing ArgoDatabase')
        self.init_database(db_name)
        self.init_profiles_collection(collection_name)
        self.local_platform_dir = 'float_files'
        self.home_dir = os.getcwd()
        self.replace_profile = replace_profile
        self.url = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
        self.qcThreshold = qcThreshold
        self.dbDumpThreshold = dbDumpThreshold

    def init_database(self, db_name):
        logging.debug('initializing init_database')
        dbUrl = 'mongodb://localhost:27017/'
        client = pymongo.MongoClient(dbUrl)
        # create database
        self.db = client[db_name]

    def init_profiles_collection(self, collection_name):
        try:
            self.profiles_coll = self.db[collection_name]
            #compound index is used when order matters
            '''
            self.profiles_coll.create_index([('geoLocation', pymongo.GEOSPHERE),
                                             ('date', pymongo.DESCENDING),
                                             ('measurements.pres', pymongo.DESCENDING)])
            '''
            #Indexes can be combined using multiple index intersections, but this does not replace compound indexes.
            self.profiles_coll.create_index([('date', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('platform_number', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('cycle_number', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('dac', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('geoLocation', pymongo.GEOSPHERE)])
            #self.profiles_coll.create_index([('measurements.pres', pymongo.DESCENDING)]) #doesn't speed up selection

        except:
            logging.warning('not able to get collections or set indexes')

    def add_locally(self, local_dir, how_to_add='all', files=[], dacs=[]):
        os.chdir(local_dir)
        reBR = re.compile(r'^(?!.*BR\d{1,})') # ignore characters starting with BR followed by a digit
        if how_to_add=='by_dac_profiles':
            files = []
            logging.debug('adding dac profiles from path: {0}'.format(local_dir))
            for dac in dacs:
                logging.debug('On dac: {0}'.format(dac))
                files = files+glob.glob(os.path.join(local_dir, dac, '**', 'profiles', '*.nc'))     
        elif how_to_add=='profile_list':
            logging.debug('adding profiles in provided list')
        elif how_to_add=='profiles':
            logging.debug('adding profiles individually')
            files = files+glob.glob(os.path.join(local_dir, '**', '**', 'profiles', '*.nc'))
            files = list(filter(reBR.search, files))
        else:
            logging.warning('how_to_add not recognized. not going to do anything.')
            return
        
        documents = []
        for fileName in files:
            logging.info('on file: {0}'.format(fileName))
            dac_name = fileName.split('/')[-4]
            root_grp = Dataset(fileName, "r", format="NETCDF4")
            remote_path = self.url + os.path.relpath(fileName,local_dir)
            variables = root_grp.variables
            doc = self.make_profile_doc(variables, dac_name, remote_path)
            if isinstance(doc, dict):
                documents.append(doc)
            if len(documents) >= self.dbDumpThreshold:
                logging.debug('dumping data to database')
                self.add_many_profiles(documents, fileName)
                documents = []
        logging.debug('all files have been read. dumping remaining documents to database')
        if len(documents) == 1:
            self.add_single_profile(documents[0], fileName)
        elif len(documents) > 1:
            self.add_many_profiles(documents, fileName)

    def make_profile_dict(self,
                          variables,
                          idx,
                          platform_number,
                          ref_date,
                          dac_name,
                          station_parameters,
                          remote_path):
        """ Takes a profile measurement and formats it into a dictionary object.
        Currently, only temperature, pressure, salinity, and conductivity are included.
        There are other methods. """

        def format_qc_array(array):
            """ Converts array of QC values (temp, psal, pres, etc) into list"""
            if type(array) == np.ndarray:
                data = [x.astype(str) for x in array]
            elif type(array) == np.ma.core.MaskedArray:
                data = array.data
                try:
                    data = np.array([x.astype(str) for x in data])
                except NotImplementedError:
                    logging.warning('NotImplemented Error for platform:'
                                    ' {0}, idx: {1}'.format(platform_number, idx))
            return data

        def format_measurments(variables, measStr):
            """ Converts array of measurements and adjusted measurements into arrays"""
            df = pd.DataFrame()
            not_adj = measStr.lower()+'_not_adj'
            adj = measStr.lower()
            # get unadjusted value. Types vary from arrays to masked arrays.
            if type(variables[measStr][idx, :]) == np.ndarray:
                df[not_adj] = variables[measStr][idx, :]
            else:  # sometimes a masked array is used
                try:
                    df[not_adj] = variables[measStr][idx, :].data
                except ValueError:
                    logging.warning('Value error while formatting measurement {}: check data type'.format(measStr))
            # get adjusted value.
            try:
                if type(variables[measStr + '_ADJUSTED'][idx, :]) == np.ndarray:
                    df[adj] = variables[measStr + '_ADJUSTED'][idx, :]
            except KeyError:
                logging.debug('adjusted value for {} does not exist'.format(measStr))
                df[adj] = np.nan
            else:  # sometimes a masked array is used
                try:
                    df[adj] = variables[measStr + '_ADJUSTED'][idx, :].data
                except ValueError:
                    logging.warning('Value error while formatting measurement {}: check data type'.format(measStr))
            try:
                df.loc[df[adj] >= 99999, adj] = np.NaN
            except KeyError:
                logging.warning('key not found...')
            df.ix[df[not_adj] >= 99999, not_adj] = np.NaN
            try:
                df[adj+'_qc'] = format_qc_array(variables[measStr + '_QC'][idx, :])
            except KeyError:
                logging.warning('qc not found for {}'.format(measStr))
                logging.warning('returning empty dataframe')
                return pd.DataFrame()
            
            """Adjusted column NaN are filled with unadjusted values.
            unadjusted column is then dropped.
            """
            df[adj].fillna(df[not_adj], inplace=True)
            df.drop([not_adj], axis=1, inplace=True)
            """QC procedure drops any row whos qc value does not equal '1'
            """
            try:
                df = df[df[adj+'_qc'] == self.qcThreshold]
            except KeyError:
                logging.warning('measurement: {0} has no qc.'
                          ' returning empty dataframe'.format(adj))
                return pd.DataFrame()
                df.shape
            return df

        profileDf = pd.DataFrame()
        keys = variables.keys()
        #  Profile measurements are gathered in a dataframe
        measList = ['TEMP', 'PRES', 'PSAL', 'CNDC', 'DOXY', 'CHLA', 'CDOM', 'NITRATE']
        for measStr in measList:
            if measStr in keys:
                meas_df = format_measurments(variables, measStr)
                # append with index conserved
                profileDf = pd.concat([profileDf, meas_df], axis=1)
                

        #pressure is the critical feature. If it has a bad qc value, drop the whole row
        if not 'pres_qc' in profileDf.columns:
            logging.warning('Float: {0} has bad pressure qc.'
                          ' Not going to add'.format(platform_number))
            return
        profileDf = profileDf[profileDf['pres_qc'] != np.NaN]

        if type(variables['JULD'][idx]) == np.ma.core.MaskedConstant:
            cycle_number = variables['CYCLE_NUMBER'][idx].astype(str)
            logging.debug('Float: {0} cycle: {1} has unknown date.'
                          ' Not going to add'.format(platform_number, cycle_number))
            return
        else:
            date = ref_date + timedelta(variables['JULD'][idx])

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
            cycle_number = variables['CYCLE_NUMBER'][idx].astype(str)
            logging.debug('Float: {0} cycle: {1} has neither temp nor psal.'
                          ' Not going to add'.format(platform_number, cycle_number))
            return


        def make_meas_docs(xName, yName, df, profile_doc):
            """
            create a list of dict objects x, y from profile. Optional to add in order to make
            plotting simplar.
            """
            dfOut = df[[yName, xName]].copy()
            
            #dfOut Decimal128(row[key])
            dfOut.dropna(axis=0, how='any', inplace=True)
            # pymongo will give a BSON error sometimes when trying to save this as a float
            dfOut[[yName, xName]] = dfOut[[yName, xName]].astype(str)
            # dictOut = dfOut.to_dict(orient='list')
            dictOut = dfOut.to_dict(orient='records')
    
            if not len(dictOut) == 0:
                measName = yName+'_vs_'+xName
                profile_doc[measName] = dictOut
            return profile_doc


        profile_doc = dict()
        # Not being used at the moment. Sorting and filtering is done on the front end.
        """
        profile_doc = make_meas_docs('pres','temp', profileDf, profile_doc)
        profile_doc = make_meas_docs('pres','psal', profileDf, profile_doc)
        profile_doc = make_meas_docs('psal','temp', profileDf, profile_doc)
        """
        profileDf.fillna(-999, inplace=True) # API needs all measurements to be a number
        if profileDf.shape[0] == 0:
            logging.warning('Float: {0} cycle: {1} has no valid measurements.'
                          ' Not going to add'.format(platform_number, idx))
            return
        maxPres = profileDf.pres.max()

        profile_doc['max_pres'] = int(maxPres)
        qcColNames = [k for k in profileDf.columns.tolist() if '_qc' in k]  # qc values are no longer needed.
        profileDf.drop(qcColNames, axis = 1, inplace = True)

        profile_doc['measurements'] = profileDf.astype(np.float64).to_dict(orient='records')  # orient='list' will store these as single arrays
        profile_doc['date'] = date
        phi = variables['LATITUDE'][idx]
        lam = variables['LONGITUDE'][idx]

        def add_string_values(profile_doc, valueName, idx):
            """Used to add POSITIONING_SYSTEM PLATFORM_TYPE DATA_MODE and PI_NAME fields.
            if missing or 
            """
            try:
                #if type(variables[valueName][idx] == np.bytes_):
                #    value = variables[valueName][idx]
                #    profile_doc[valueName] = value
                #    return profile_doc
                if type(variables[valueName][idx]) == np.ma.core.MaskedConstant:
                    value = variables[valueName][idx].astype(str)
                    cycle_number = variables['CYCLE_NUMBER'][idx]
                    logging.debug('Float: {0} cycle: {1} has unknown {2}.'
                              ' Not going to add item to document'.format(platform_number, cycle_number, valueName))
                    return profile_doc
                else:
                    if valueName == 'DATA_MODE':
                        value = variables[valueName][idx].astype(str)
                    else:
                        value = ''.join([(x.astype(str)) for x in variables[valueName][idx].data])
                        value = value.strip(' ')
                    profile_doc[valueName] = value
                    return profile_doc
            except KeyError:
                logging.debug('unknown key {0}.'
                              ' Not going to add item to document'.format(valueName))
                return profile_doc
            except:
                logging.debug('error when adding {0} to document.'
                              ' Not going to add item to document'.format(valueName))
                return profile_doc

        profile_doc = add_string_values(profile_doc, 'POSITIONING_SYSTEM', idx)
        profile_doc = add_string_values(profile_doc, 'PLATFORM_TYPE', idx)
        #pdb.set_trace()
        profile_doc = add_string_values(profile_doc, 'DATA_MODE', idx) # oftentimes fails...
        profile_doc = add_string_values(profile_doc, 'PI_NAME', idx)

        try:
            profile_doc['position_qc'] = int(variables['POSITION_QC'][idx].astype(int))
        except AttributeError:
            if type(variables['POSITION_QC'][idx] == np.ma.core.MaskedConstant):
                profile_doc['position_qc'] = str(variables['POSITION_QC'][idx].data.astype(int))
            else:
                logging.warning('error with position_qc. not going to add.')
                return

        cycle_number = int(variables['CYCLE_NUMBER'][idx].astype(str))
        if type(phi) == np.ma.core.MaskedConstant:
            logging.warning('Float: {0} cycle: {1} has unknown lat-lon.'
                          ' Not going to add'.format(platform_number, cycle_number))
            return
            
        profile_doc['cycle_number'] = cycle_number
        profile_doc['lat'] = phi
        profile_doc['lon'] = lam
        profile_doc['geoLocation'] = {'type': 'Point', 'coordinates': [lam, phi]}
        profile_doc['dac'] = dac_name
        profile_doc['platform_number'] = platform_number
        profile_doc['station_parameters'] = station_parameters
        profile_id = platform_number + '_' + str(cycle_number)
        url = remote_path
        profile_doc['nc_url'] = url
        """Normally, the floats take measurements on the ascent. 
        In the event that the float takes measurements on the descent, the
        cycle number doesn't change. So, to have a unique identifer, this 
        the _id field has a 'D' appended"""
        if type(variables['DIRECTION'][idx]) == np.ma.core.MaskedConstant:
            logging.debug('direction unknown')
        else:
            direction = variables['DIRECTION'][idx].astype(str)
            if direction == 'D':
                profile_id += 'D'
            profile_doc['_id'] = profile_id
        return profile_doc

    def make_profile_doc(self, variables, dac_name, remote_path):

        def format_param(param):
            if type(param) == np.ndarray:
                formatted_param = ''.join([(x.astype(str)) for x in param])
            elif type(param) == np.ma.core.MaskedArray:
                try:
                    formatted_param = ''.join([(x.astype(str)) for x in param.data])
                except NotImplementedError:
                    logging.debug('NotImplementedError param is not expected type')
                except AttributeError:  # sometimes platform_num is an array...
                    logging.debug('attribute error: param is not expected type')
                    logging.debug('type: {}'.format(type(param)))
                except:
                    logging.debug('type: {}'.format(type(param)))
            else:
                logging.error(' check type: {}'.format(type(param)))
                pass
            return formatted_param.strip(' ')


        cycles = variables['CYCLE_NUMBER'][:][:]
        list_of_dup_inds = [np.where(a == cycles)[0] for a in np.unique(cycles)]
        for array in list_of_dup_inds:
            if len(array) > 2:
                logging.info('duplicate cycle numbers found...')

        platform_number = format_param(variables['PLATFORM_NUMBER'][0])
        station_parameters = list(map(lambda param: format_param(param), variables['STATION_PARAMETERS'][0]))
        numOfProfiles = variables['JULD'][:].shape[0]
        logging.info('number of profiles inside file: {}'.format(numOfProfiles))

        ref_date_array = variables['REFERENCE_DATE_TIME'][:]
        ref_str = ''.join([x.astype(str) for x in ref_date_array])
        ref_date = datetime.strptime(ref_str, '%Y%m%d%H%M%S')
        idx = 0 #stometimes there are two profiles. The second profile is ignored.
        doc = self.make_profile_dict(variables, idx, platform_number, ref_date, dac_name, station_parameters, remote_path)
        return doc

    def add_single_profile(self, doc, file_name, attempt=0):
        if self.replace_profile == True:
            try:
                self.profiles_coll.replace_one({'_id': doc['_id']}, doc, upsert=True)
            except pymongo.errors.WriteError:
                logging.warning('check the following id '
                                'for filename : {0}'.format(doc['_id'], file_name))
            except bson.errors.InvalidDocument:
                pdb.set_trace()
                logging.warning('bson error')
                logging.warning('check the following document: {0}'.format(doc['_id']))
            except TypeError:
                logging.warning('Type error while inserting one document.')
        else:
            try:
                self.profiles_coll.insert_one(doc)
            except pymongo.errors.DuplicateKeyError:
                logging.error('duplicate key: {0}'.format(doc['_id']))
                logging.error('not going to add')
    
            except pymongo.errors.WriteError:
                logging.warning('check the following id '
                                'for filename : {0}'.format(doc['_id'], file_name))
            except bson.errors.InvalidDocument:
                logging.warning('bson error')
                logging.warning('check the following document: {0}'.format(doc['_id']))
            except TypeError:
                logging.warning('Type error while inserting one document.')

    def add_many_profiles(self, documents, file_name):
        try:
            self.profiles_coll.insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            writeErrors = bwe.details['writeErrors']
            problem_idx = []
            for we in writeErrors:
                problem_idx.append(we['index'])
            trouble_list = [documents[i] for i in problem_idx]
            logging.warning('bulk write failed for: {0}'.format(file_name))
            logging.warning('adding the failed documents one at a time.')
            for doc in trouble_list:
                self.add_single_profile(doc, file_name)
        except bson.errors.InvalidDocument:
            logging.warning('bson error')
            for doc in documents:
                self.add_single_profile(doc, file_name)
        except TypeError:
            nonDictDocs = [doc for doc in documents if not isinstance(doc, dict)]
            logging.warning('Type error when during insert_many method. Check documents.')
            logging.warning('number of non dictionary items in documents: {}'.format(len(nonDictDocs)))

def getOutput():
    try:
        mySystem = sys.argv[1]
    except:
        mySystem = 'carbyTrouble'

    if mySystem == 'carby':
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    if mySystem == 'carbyTrouble':
        OUTPUT_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'troublesomeFiles')
    elif mySystem == 'kadavu':
        OUTPUT_DIR = os.path.join('/home', 'tylertucker', 'ifremer')
    elif mySystem == 'ciLab':
        OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    else:
        print('pc not found. assuming default')
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    return OUTPUT_DIR

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='argoTroublesomeProfiles.log',
                        level=logging.INFO)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = getOutput()
    # init database
    DB_NAME = 'argoTrouble'
    COLLECTION_NAME = 'profiles'
    ad = argoDatabase(DB_NAME, COLLECTION_NAME, True)
    ad.add_locally(OUTPUT_DIR, how_to_add='profiles')
    logging.debug('End of log file')
