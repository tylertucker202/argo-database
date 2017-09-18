# -*- coding: utf-8 -*-
import pymongo
import os
import glob
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset
import bson.errors

class ArgoDatabase(object):
    def __init__(self,
                 db_name,
                 collection_name):
        logging.debug('initializing ArgoDatabase')
        self.init_database(db_name)
        self.init_float_collection(collection_name)
        self.local_platform_dir = 'float_files'
        self.home_dir = os.getcwd()
        self.url = 'usgodae.org'
        self.path = '/pub/outgoing/argo/dac/'

    def init_database(self, db_name):
        logging.debug('initializing init_database')
        dbUrl = 'mongodb://localhost:27017/'
        client = pymongo.MongoClient(dbUrl)
        # create database
        self.db = client[db_name]

    def init_float_collection(self, collection_name):
        try:
            self.float_coll = self.db[collection_name]
            self.float_coll.create_index([("geoLocation", pymongo.GEOSPHERE),
                                          ('date', pymongo.DESCENDING)])
        except:
            logging.warning('not able to get collections')

    def add_locally(self, local_dir, how_to_add='all', files=[]):
        os.chdir(local_dir)

        if how_to_add=='all':
            logging.debug('adding all files ending in _prof.nc:')
            files = glob.glob(os.path.join(local_dir, '**', '*_prof.nc'), recursive=True)
        elif how_to_add=='profList':
            logging.debug('adding _prof.nc in provided list')
        elif how_to_add=='profiles':
            logging.debug('adding profiles individually')
        else:
            logging.warning('how_to_add not recognized. not going to do anything.')
            return
        for file in files:
            logging.debug('on file: {0}'.format(file))
            dac_name = file.split('/')[-3]
            root_grp = Dataset(file, "r", format="NETCDF4")
            variables = root_grp.variables
            documents = self.make_prof_documents(variables, dac_name)
            if len(documents) == 1:
                self.add_single_profile(documents[0], file)
            else:
                self.add_many_profiles(documents, file)


    def make_prof_dict(self, variables, idx, platform_number, ref_date, dac_name, station_parameters):
        """ Takes a profile measurement and formats it into a dictionary object.
        Currently, only temperature, pressure, salinity, and conductivity are included.
        There are other methods. """

        def format_qc_array(array):
            """ Converts array of QC values (temp, psal, pres, etc) into list"""

            # sometimes array comes in as a different type
            if type(array) == np.ndarray:
                data = [x.astype(str) for x in array]
            elif type(array) == np.ma.core.MaskedArray:  # otherwise type is a masked array
                data = array.data
                try:
                    data = np.array([x.astype(str) for x in data])  # Convert to ints
                except NotImplementedError:
                    logging.warning('NotImplemented Error for platform:'
                                    ' {0}, idx: {1}'.format(platform_number, idx))
            return data

        def format_measurments(meas_str):
            """ Converts array of measurements and adjusted measurements into arrays"""
            df = pd.DataFrame()
            not_adj = meas_str.lower()+'_not_adj'
            adj = meas_str.lower()

            if type(variables[meas_str][idx, :]) == np.ndarray:
                df[not_adj] = variables[meas_str][idx, :]
                df[adj] = variables[meas_str + '_ADJUSTED'][idx, :]
            else:  # sometimes a masked array is used
                try:
                    df[not_adj] = variables[meas_str][idx, :].data
                    df[adj] = variables[meas_str + '_ADJUSTED'][idx, :].data
                except ValueError:
                    logging.debug('check data type')
            df[adj].loc[df[adj] >= 99999] = np.NaN
            df[not_adj].loc[df[not_adj] >= 99999] = np.NaN
            df[adj+'_qc'] = format_qc_array(variables[meas_str + '_QC'][idx, :])
            df[adj].fillna(df[not_adj], inplace=True)
            df.drop([not_adj], axis=1, inplace=True)
            return df
        
        profile_df = pd.DataFrame()
        keys = variables.keys()
        #  Profile measurements are gathered in a dataframe
        if 'TEMP' in keys:
            temp_df = format_measurments('TEMP')
            profile_df = pd.concat([profile_df, temp_df], axis=1)
        if 'PRES' in keys:
            pres_df = format_measurments('PRES')
            profile_df = pd.concat([profile_df, pres_df], axis=1)
        if 'PSAL' in keys:
            psal_df = format_measurments('PSAL')
            profile_df = pd.concat([profile_df, psal_df], axis=1)
        if 'CNDC' in keys:
            cndc_df = format_measurments('CNDC')
            profile_df = pd.concat([profile_df, cndc_df], axis=1)
        date = ref_date + timedelta(variables['JULD'][idx])
        profile_df.replace([99999.0, 99999.99999], value=np.NaN, inplace=True)
        profile_df.dropna(axis=0, how='all', inplace=True)
        profile_df.dropna(axis=0, subset=['pres'], inplace=True)  # Drops the values where pressure isn't reported
        profile_doc = dict()
        profile_doc['measurements'] = profile_df.to_dict(orient='list')
        profile_doc['date'] = date
        phi = variables['LATITUDE'][idx]
        lam = variables['LONGITUDE'][idx]
        try:
            profile_doc['position_qc'] = variables['POSITION_QC'][idx].astype(str)
        except AttributeError:
            if type(variables['POSITION_QC'][idx] == np.ma.core.MaskedConstant):
                profile_doc['position_qc'] = str(variables['POSITION_QC'][idx].data.astype(str))
            else:
                logging.warning('error with position_qc. not going to add.')
                return

        cycle_number = variables['CYCLE_NUMBER'][idx].astype(str)
        if type(phi) == np.ma.core.MaskedConstant:
            logging.debug('Float: {0} cycle: {1} has unknown lat-lon.'
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
        url = 'ftp://' + self.url + self.path \
              + dac_name \
              + '/' + platform_number \
              + '/' + platform_number + '_prof.nc'
        profile_doc['nc_url'] = url
        profile_doc['_id'] = profile_id
        return profile_doc

    def make_prof_documents(self, variables, dac_name):

        def format_param(param):
            if type(param) == np.ndarray:
                formatted_param = ''.join([(x.astype(str)) for x in param])
            elif type(param) == np.ma.core.MaskedArray:
                try:
                    formatted_param = ''.join([(x.astype(str)) for x in param.data])
                except NotImplementedError:
                    logging.debug('platform number is not expected type')
                except AttributeError:  # sometimes platform_num is an array...
                    logging.debug('param is not expected type')
                    logging.debug('type: {}'.format(type(param)))
                except:
                    logging.debug('type: {}'.format(type(param)))
            else:
                logging.error(' check type: {}'.format(type(param)))
                pass
            return formatted_param.strip(' ')

        platform_number = format_param(variables['PLATFORM_NUMBER'][0])
        station_parameters = list(map(lambda param: format_param(param), variables['STATION_PARAMETERS'][0]))
        numOfProfiles = variables['JULD'][:].shape[0]
        documents = []
        ref_date_array = variables['REFERENCE_DATE_TIME'][:]
        ref_str = ''.join([x.astype(str) for x in ref_date_array])
        ref_date = datetime.strptime(ref_str, '%Y%m%d%H%M%S')
        for idx in range(numOfProfiles):
            doc = self.make_prof_dict(variables, idx, platform_number, ref_date, dac_name, station_parameters)
            if doc is not None:
                documents.append(doc)
        return documents

    def add_single_profile(self, doc, file_name):
        try:
            self.float_coll.insert(doc)
        except pymongo.errors.WriteError:
            logging.warning('check the following id '
                            'for filename : {0}'.format(doc['_id'], file_name))
        except pymongo.errors.DuplicateKeyError:
            logging.debug('duplicate key: {0}'.format(doc['_id']))
            logging.debug('moving on: {0}'.format(doc['_id']))
        except bson.errors.InvalidDocument:
            logging.warning('bson error')
            logging.warning('check the following document: {0}'.format(doc['_id']))
        except TypeError:
            logging.warning('Type error')

    def add_many_profiles(self, documents, file_name):
        try:
            self.float_coll.insert_many(documents)
        except pymongo.errors.BulkWriteError:
            logging.warning('bulk write failed for: {0}'.format(file_name))
            logging.warning('going to add one at a time')
            for doc in documents:
                self.add_single_profile(doc, file_name)
        except bson.errors.InvalidDocument:
            logging.warning('bson error')
            for doc in documents:
                self.add_single_profile(doc, file_name)
        except TypeError:
            logging.warning('Type error')

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='argoTestDatabase.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo', 'argoBackend', 'FTP-mirror')
    # init database
    DB_NAME = 'argo_test'
    COLLECTION_NAME = 'profiles'
    DATA_DIR = os.path.join(HOME_DIR, 'data')

    ad = ArgoDatabase(DB_NAME, COLLECTION_NAME)
    ad.add_locally(OUTPUT_DIR, how_to_add='all')
