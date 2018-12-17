# -*- coding: utf-8 -*-
import pymongo
import os
import re
import glob
import logging
import numpy as np
from scipy.interpolate import griddata
from datetime import datetime
from netCDF4 import Dataset
import bson.errors
import pdb
from netCDFToDoc import netCDFToDoc
import pandas as pd

class argoDatabase(object):
    def __init__(self,
                 dbName,
                 collectionName='profiles', 
                 replaceProfile=False,
                 qcThreshold='1', 
                 dbDumpThreshold=10000,
                 removeExisting=True, 
                 testMode=False,
                 basinFilename='../basinmask_01.nc', 
                 addToDb=True):
        logging.debug('initializing ArgoDatabase')
        self.collectionName = collectionName
        self.dbName = dbName
        self.home_dir = os.getcwd()
        self.replaceProfile = replaceProfile
        self.url = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
        self.qcThreshold = qcThreshold
        self.dbDumpThreshold = dbDumpThreshold
        self.removeExisting = removeExisting
        self.testMode = testMode # used for testing documents outside database
        self.addToDb = addToDb
        self.documents = []
        self.init_basin(basinFilename)
        
    def init_basin(self, basinFilename):
        nc = Dataset(basinFilename, 'r')
    
        assert nc.variables['LONGITUDE'].mask == True
        assert nc.variables['LATITUDE'].mask == True
    
        idx = np.nonzero(~nc.variables['BASIN_TAG'][:].mask)
        self.basin = nc.variables['BASIN_TAG'][:][idx].astype('i')
        self.coords = np.stack([nc.variables['LATITUDE'][idx[0]],
                           nc.variables['LONGITUDE'][idx[1]]]).T

    def get_basin(self, lat, lon):
        """Returns the basin code for a given lat lon coordinates
           Ex.:
           basin = get_basin(15, -38, '/path/to/basinmask_01.nc')
        """   
        return int(griddata(self.coords, self.basin, (lon, lat), method='nearest'))
    
    def add_basin(self, doc, fileName):
        try:
            doc['BASIN'] = self.get_basin(doc['lat'], doc['lon'])
        except:
            logging.warning('not able to retireve basin flag for filename: {}'.format(fileName))
            doc['BASIN'] = int(-999)
        return doc

    def create_collection(self):
        dbUrl = 'mongodb://localhost:27017/'
        client = pymongo.MongoClient(dbUrl)
        db = client[self.dbName]
        coll = db[self.collectionName]
        coll = self.init_profiles_collection(coll)
        return coll    

    @staticmethod
    def init_profiles_collection(coll):
        try:
            coll.create_index([('date', pymongo.DESCENDING)])
            coll.create_index([('platform_number', pymongo.DESCENDING)])
            coll.create_index([('cycle_number', pymongo.DESCENDING)])
            coll.create_index([('dac', pymongo.DESCENDING)])
            coll.create_index([('geoLocation', pymongo.GEOSPHERE)])
            coll.create_index([('containsBGC', pymongo.DESCENDING)])
            coll.create_index([('isDeep', pymongo.DESCENDING)])
            #coll.create_index([('BASIN', pymongo.DESCENDING)])
        except:
            logging.warning('not able to get collections or set indexes')
        return coll
    
    @staticmethod
    def create_df_of_files(files):
        df = pd.DataFrame()
        df['file'] = files
        df['filename'] = df['file'].apply(lambda x: x.split('/')[-1])
        df['profile'] = df['filename'].apply(lambda x: re.sub('[MDAR(.nc)]', '', x))
        df['prefix'] = df['filename'].apply(lambda x: re.sub(r'[0-9_(.nc)]', '', x))
        df['platform'] = df['profile'].apply(lambda x: re.sub(r'(_\d{3})', '', x))
        return df        
    
    def remove_duplicate_if_mixed(self, files):
        '''remove platforms from core that exist in mixed df'''
        df = self.create_df_of_files(files)
        dfMixed = df[ df['prefix'].str.contains('M')]
        dfCore = df[ ~df['prefix'].str.contains('M')]
        rmPlat = dfMixed['platform'].unique().tolist()
        dfTruncCore = dfCore[ ~dfCore['platform'].isin(rmPlat)]
        outDf = pd.concat([dfMixed, dfTruncCore], axis=0, sort=False)
        outFiles = outDf.file.tolist()
        return outFiles

    def get_file_names_to_add(self, localDir, dacs=[]):
        files = []
        reBR = re.compile(r'^(?!.*BR\d{1,})') # ignore characters starting with BR followed by a digit
        if len(dacs) != 0:
            for dac in dacs:
                logging.debug('On dac: {0}'.format(dac))
                files = files+glob.glob(os.path.join(localDir, dac, '**', 'profiles', '*.nc'))
        else:
            logging.debug('adding profiles individually')
            files = files+glob.glob(os.path.join(localDir, '**', '**', 'profiles', '*.nc'))

        files = list(filter(reBR.search, files))
        files = self.remove_duplicate_if_mixed(files)
        return files        

    def add_locally(self, localDir, files):
        
        if self.addToDb:
            coll = self.create_collection()

        if self.removeExisting and self.addToDb: # Removes profiles on list before adding list
            self.remove_profiles(files, coll)

        logging.warning('Attempting to add: {}'.format(len(files)))
        documents = []
        for fileName in files:
            logging.info('on file: {0}'.format(fileName))
            dacName = fileName.split('/')[-4]
            
            try:
                root_grp = Dataset(fileName, "r", format="NETCDF4")
            except OSError as err:
                logging.warning('File not read: {}'.format(err))
                continue
            remotePath = self.url + os.path.relpath(fileName, localDir)
            variables = root_grp.variables
            
            #current method of creating dac
            nProf = root_grp.dimensions['N_PROF'].size
            doc = self.make_profile_doc(variables, dacName, remotePath, fileName, nProf)
            if isinstance(doc, dict):
                doc = self.add_basin(doc, fileName)
                documents.append(doc)
                if self.testMode:
                    self.documents.append(doc)
            if len(documents) >= self.dbDumpThreshold and self.addToDb:
                logging.debug('dumping data to database')
                self.add_many_profiles(documents, fileName, coll)
                documents = []
        logging.debug('all files have been read. dumping remaining documents to database')
        if len(documents) == 1 and self.addToDb:
            self.add_single_profile(documents[0], fileName, coll)
        elif len(documents) > 1 and self.addToDb:
            self.add_many_profiles(documents, fileName, coll)
        
    def remove_profiles(self, files, coll):
        #get profile ids
        idList = []
        for fileName in files:
            profileName = fileName.split('/')[-1]
            profileName = profileName[1:-3]
            profileCycle = profileName.split('_')
            cycle = profileName.split('_')[1].lstrip('0')
            profileName = profileCycle[0] + '_' + cycle
            idList.append(profileName)
        #remove all profiles at once
        logging.debug('removing profiles before reintroducing')
        logging.debug('number of profiles to be deleted: {}'.format(len(idList)))
        countBefore = coll.find({}).count()
        coll.delete_many({'_id': {'$in': idList}})
        countAfter = coll.find({}).count()
        delta = countBefore - countAfter
        coll.find({}).count()
        logging.debug('number of profiles removed: {}'.format(delta))

    @staticmethod
    def format_param(param):
        """
        Param is an fixed array of characters. format_param attempts to convert this array to
        a string.
        """
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
    
    def getStationParameters(self, statParam):
        stationParameters = []
        for idx in range(0, statParam.shape[0]):
            params = list(map(lambda param: self.format_param(param), statParam[idx]))
            dimStatParam = [x for x in params if x]
            stationParameters.append(dimStatParam)
        return stationParameters
        
        
        

    def make_profile_doc(self, variables, dacName, remotePath, fileName, nProf):
        """
        Retrieves some meta information and counts number of profile measurements.
        Sometimes a profile will contain more than one measurement in variables field.
        Only the first is added.
        """

        cycles = variables['CYCLE_NUMBER'][:][:]
        list_of_dup_inds = [np.where(a == cycles)[0] for a in np.unique(cycles)]
        for array in list_of_dup_inds:
            if len(array) > 2:
                logging.info('duplicate cycle numbers found...')

        platformNumber = self.format_param(variables['PLATFORM_NUMBER'][0])
        
        stationParameters = self.getStationParameters(variables['STATION_PARAMETERS'])
        refDateArray = variables['REFERENCE_DATE_TIME'][:]
        refStr = ''.join([x.astype(str) for x in refDateArray])
        refDate = datetime.strptime(refStr, '%Y%m%d%H%M%S')
        idx = 0 #stometimes there are two profiles. The second profile is ignored.
        qcThreshold='1'
        try:
            p2D = netCDFToDoc(variables, dacName, refDate, remotePath, stationParameters, platformNumber, idx, qcThreshold, nProf)
            doc = p2D.get_profile_doc()
            return doc
        except AttributeError as err:
            logging.warning('Profile: {0} enountered AttributeError: {1}'.format(fileName.split('/')[-1], err.args))
        except TypeError as err:
            logging.warning('Profile: {0} enountered TypeError: {1}'.format(fileName.split('/')[-1], err.args))
        except ValueError as err:
            logging.warning('Profile: {0} enountered ValueError: {1}'.format(fileName.split('/')[-1], err.args))
        except UnboundLocalError as err:
            if 'no valid measurements.' in err.args[0]:
                logging.warning('Profile: {0} has no valid measurements. not going to add'.format(fileName.split('/')[-1]))
            else:
                logging.warning('Profile: {0} encountered UnboundLocalError: {1}'.format(fileName.split('/')[-1], err.args))

    def add_single_profile(self, doc, file_name, coll, attempt=0):
        if self.replaceProfile:
            try:
                coll.replace_one({'_id': doc['_id']}, doc, upsert=True)
            except pymongo.errors.WriteError:
                logging.warning('check the following id '
                                'for filename : {0}'.format(doc['_id'], file_name))
            except bson.errors.InvalidDocument as err:
                logging.warning('bson error {1} for: {0}'.format(doc['_id'], err))
            except TypeError:
                logging.warning('Type error while inserting one document.')
        else:
            try:
                coll.insert_one(doc)
            except pymongo.errors.DuplicateKeyError:
                logging.error('duplicate key: {0}'.format(doc['_id']))
            except pymongo.errors.WriteError:
                logging.warning('check the following id '
                                'for filename : {0}'.format(doc['_id'], file_name))
            except bson.errors.InvalidDocument as err:
                logging.warning('bson error {1} for: {0}'.format(doc['_id'], err))
            except TypeError:
                logging.warning('Type error while inserting one document.')

    def add_many_profiles(self, documents, file_name, coll):
        try:
            coll.insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            writeErrors = bwe.details['writeErrors']
            problem_idx = []
            for we in writeErrors:
                problem_idx.append(we['index'])
            trouble_list = [documents[i] for i in problem_idx]
            logging.warning('bulk write failed for: {0}. adding failed documents on at a time'.format(file_name))
            for doc in trouble_list:
                self.add_single_profile(doc, file_name, coll)
        except bson.errors.InvalidDocument:
            logging.warning('bson error')
            for doc in documents:
                self.add_single_profile(doc, file_name, coll)
        except TypeError:
            nonDictDocs = [doc for doc in documents if not isinstance(doc, dict)]
            logging.warning('Type error during insert_many method. Check documents.')
            logging.warning('number of non dictionary items in documents: {}'.format(len(nonDictDocs)))
            
def getOutput():
    mySystem = os.uname().nodename
    if mySystem == 'carby':
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    elif mySystem == 'kadavu.ucsd.edu':
        OUTPUT_DIR = os.path.join('/home', 'tylertucker', 'ifremer')
    elif mySystem == 'ciLab':
        OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    else:
        print('pc not found. assuming default')
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    return OUTPUT_DIR
