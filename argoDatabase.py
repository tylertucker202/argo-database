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
import pdb

class argoDatabase(object):
    def __init__(self,
                 db_name,
                 collection_name, 
                 replace_profile=False):
        logging.debug('initializing ArgoDatabase')
        self.init_database(db_name)
        self.init_profiles_collection(collection_name)
        self.local_platform_dir = 'float_files'
        self.home_dir = os.getcwd()
        self.replace_profile = replace_profile
        self.url = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'

    def init_database(self, db_name):
        logging.debug('initializing init_database')
        dbUrl = 'mongodb://localhost:27017/'
        client = pymongo.MongoClient(dbUrl)
        # create database
        self.db = client[db_name]

    def init_profiles_collection(self, collection_name):
        try:
            self.profiles_coll = self.db[collection_name]
            self.profiles_coll.create_index([('geoLocation', pymongo.GEOSPHERE),
                                          ('date', pymongo.DESCENDING),
                                          ('platform_number', pymongo.DESCENDING)])

        except:
            logging.warning('not able to get collections or set indexes')

    def add_locally(self, local_dir, how_to_add='all', files=[], dacs=[]):
        os.chdir(local_dir)
        reBR = re.compile(r'^(?!.*BR\d{1,})') # ignore characters starting with BR followed by a digit
        if how_to_add=='all_prof':
            logging.debug('adding all files ending in _prof.nc:')
            files = glob.glob(os.path.join(local_dir, '**', '*_prof.nc'), recursive=True)
        elif how_to_add=='by_dac_prof': #to be phased out eventaully. prof.nc files are formatted differently.
            files = []
            for dac in dacs:
                dac_files = glob.glob(os.path.join(local_dir, dac, '**', '*_prof.nc'))
                dac_files = list(filter(reBR.search, dac_files))
                files += dac_files
        elif how_to_add=='by_dac_profiles':
            files = []
            for dac in dacs:
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
            if len(documents) >= 10000:
                logging.debug('dumping data to database')
                self.add_many_profiles(documents, fileName)
                documents = []
        logging.debug('all files have been read. dumping remaining documents to database')
        if len(documents) == 1:
            self.add_single_profile(documents[0], fileName)
        elif len(documents) > 1:
            self.add_many_profiles(documents, fileName)


    def make_profile_dict(self, variables, idx, platform_number, ref_date, dac_name, station_parameters, remote_path):
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

        def format_measurments(variables, meas_str):
            """ Converts array of measurements and adjusted measurements into arrays"""
            df = pd.DataFrame()
            not_adj = meas_str.lower()+'_not_adj'
            adj = meas_str.lower()

            # get unadjusted value. Sometimes adjusted and unadj fields aren't the same type.
            if type(variables[meas_str][idx, :]) == np.ndarray:
                df[not_adj] = variables[meas_str][idx, :]
            else:  # sometimes a masked array is used
                try:
                    df[not_adj] = variables[meas_str][idx, :].data
                except ValueError:
                    logging.warning('Value error while formatting measurement {}: check data type'.format(meas_str))
            # get adjusted value.
            try:
                if type(variables[meas_str + '_ADJUSTED'][idx, :]) == np.ndarray:
                    df[adj] = variables[meas_str + '_ADJUSTED'][idx, :]
            except KeyError:
                logging.debug('adjusted value for {} does not exist'.format(meas_str))
                df[adj] = np.nan
            else:  # sometimes a masked array is used
                try:
                    df[adj] = variables[meas_str + '_ADJUSTED'][idx, :].data
                except ValueError:
                    logging.warning('Value error while formatting measurement {}: check data type'.format(meas_str))
            try:
                df.ix[df[adj] >= 99999, adj] = np.NaN
            except KeyError:
                logging.warning('key not found...')
            df.ix[df[not_adj] >= 99999, not_adj] = np.NaN
            try:
                df[adj+'_qc'] = format_qc_array(variables[meas_str + '_QC'][idx, :])
            except KeyError:
                logging.warning('qc not found for {}'.format(meas_str))
                logging.warning('not going to add') 
            df[adj].fillna(df[not_adj], inplace=True)
            df.drop([not_adj], axis=1, inplace=True)
            return df

        def compare_with_neighbors(idx):
            '''sometimes you want to compare with neighbors when debugging'''
            keys = variables.keys()
            for key in keys:
                print(key)
                first = variables[key][idx]
                second = variables[key][idx + 1]
                print('first value')
                print(first)
                print('second value')
                print(second)

        profile_df = pd.DataFrame()
        keys = variables.keys()
        #  Profile measurements are gathered in a dataframe
        if 'TEMP' in keys:
            temp_df = format_measurments(variables, 'TEMP')
            profile_df = pd.concat([profile_df, temp_df], axis=1)
        if 'PRES' in keys:
            pres_df = format_measurments(variables, 'PRES')
            profile_df = pd.concat([profile_df, pres_df], axis=1)
        if 'PSAL' in keys:
            psal_df = format_measurments(variables, 'PSAL')
            profile_df = pd.concat([profile_df, psal_df], axis=1)
        if 'CNDC' in keys:
            cndc_df = format_measurments(variables, 'CNDC')
            profile_df = pd.concat([profile_df, cndc_df], axis=1)
        if 'DOXY' in keys:
            cndc_df = format_measurments(variables, 'DOXY')
            profile_df = pd.concat([profile_df, cndc_df], axis=1)
        if 'CHLA' in keys:
            cndc_df = format_measurments(variables, 'CHLA')
            profile_df = pd.concat([profile_df, cndc_df], axis=1)
        if 'CDOM' in keys:
            cndc_df = format_measurments(variables, 'CDOM')
            profile_df = pd.concat([profile_df, cndc_df], axis=1)
        if 'NITRATE' in keys:
            cndc_df = format_measurments(variables, 'NITRATE')
            profile_df = pd.concat([profile_df, cndc_df], axis=1)
            

        if type(variables['JULD'][idx]) == np.ma.core.MaskedConstant:
            cycle_number = variables['CYCLE_NUMBER'][idx].astype(str)
            logging.debug('Float: {0} cycle: {1} has unknown date.'
                          ' Not going to add'.format(platform_number, cycle_number))
            return
        else:
            date = ref_date + timedelta(variables['JULD'][idx])
            
        profile_df.replace([99999.0, 99999.99999], value=np.NaN, inplace=True)
        profile_df.dropna(axis=0, how='all', inplace=True)
        profile_df.dropna(axis=0, subset=['pres'], inplace=True)  # Drops the values where pressure isn't reported
        profile_doc = dict()
        profile_doc['measurements'] = profile_df.to_dict(orient='records' )  # orient='list' will store these as single arrays
        profile_doc['date'] = date
        phi = variables['LATITUDE'][idx]
        lam = variables['LONGITUDE'][idx]
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
        if profile_df['pres'].shape[0] ==0:
            logging.warning('Float: {0} cycle: {1} has no pressure data. not going to add'.format(platform_number, cycle_number))
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
        pdb.set_trace()
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

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='argoTroublesomeProfiles.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    #OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    #OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'troublesome_files')
    OUTPUT_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo', 'argo-database', 'troublesomeFiles', 'mixedMode')
    # init database
    DB_NAME = 'mixedMode'
    COLLECTION_NAME = 'profiles'
    DATA_DIR = os.path.join(HOME_DIR, 'data')
    ad = argoDatabase(DB_NAME, COLLECTION_NAME)
    ad.add_locally(OUTPUT_DIR, how_to_add='profiles')
    logging.debug('End of log file')
    
    #have added 'nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois' 'jma', 'csiro'
    #have not added: 'aoml'  'coriolis'
