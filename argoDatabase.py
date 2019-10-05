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
                 qcThreshold='1', 
                 dbDumpThreshold=1000,
                 removeExisting=True,
                 basinFilename='../basinmask_01.nc', 
                 addToDb=True, 
                 removeAddedFileNames=False, 
                 adjustedOnly=False):
        logging.debug('initializing ArgoDatabase')
        self.collectionName = collectionName
        self.dbName = dbName
        self.home_dir = os.getcwd()
        self.url = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
        self.qcThreshold = qcThreshold
        self.dbDumpThreshold = dbDumpThreshold
        self.removeExisting = removeExisting
        self.addToDb = addToDb # used for testing
        self.documents = []
        self.removeAddedFileNames=removeAddedFileNames
        self.adjustedOnly = adjustedOnly
        
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
        return int(griddata(self.coords, self.basin, (lat, lon), method='nearest'))
    
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
            #coll.create_index([('cycle_number', pymongo.DESCENDING)])
            coll.create_index([('dac', pymongo.DESCENDING)])
            coll.create_index([('geoLocation', pymongo.GEOSPHERE)])

            # these are needed for db overview
            coll.create_index([('containsBGC', pymongo.DESCENDING)])
            coll.create_index([('isDeep', pymongo.DESCENDING)])
            #coll.create_index([('BASIN', pymongo.DESCENDING)])
        except:
            logging.warning('not able to get collections or set indexes')
        return coll

    @staticmethod
    def delete_list_of_files(files):
        for file in files:
            try:
                os.remove(file)
            except Exception as err:
                logging.warning('error when removing files: {}'.format(err))
                pass

    @staticmethod
    def core_data_mode(data_modes):
        '''Extracts core data mode from array of data_modes from a synthetic profile'''
        core_data_modes = np.unique(data_modes[0:3])
        if not len(core_data_modes) == 1:
            logging.warning('core data mode has multiple warnings ')
        data_mode = core_data_modes[0]
        return data_mode

    def add_locally(self, localDir, files, threadN=1):
        nFiles = len(files)
        if self.addToDb:
            coll = self.create_collection()

        if self.removeExisting and self.addToDb: # Removes profiles on list before adding list
            logging.warning('removing existing profiles before adding files')
            self.remove_profiles(files, coll)

        logging.warning('Attempting to add: {}'.format(nFiles))
        completedFileNames = []
        for idx, fileName in enumerate(files):
            logging.info('on file: {0}'.format(fileName))
            dacName = fileName.split('/')[-4]
            
            if idx % 3000 == 0:
                percDone = 100 * idx / nFiles
                logging.warning('{0} percent through files for thread: {1}'.format(percDone, threadN ))
            try:
                root_grp = Dataset(fileName, "r", format="NETCDF4")
                
            except OSError as err:
                logging.warning('File not read: {}'.format(err))
                continue
            remotePath = self.url + os.path.relpath(fileName, localDir)
            variables = root_grp.variables
            if 'DATA_MODE' in variables.keys():
                data_mode = variables['DATA_MODE'][0].astype(str).item()
            elif 'PARAMETER_DATA_MODE' in variables.keys():
                data_modes = variables['PARAMETER_DATA_MODE'][0].data.astype(str).tolist()
                data_mode = self.core_data_mode(data_modes)
            else:
                pdb.set_trace()
                logging.warning('filename {0} could not retrieve data_mode. not going to add. notify dacs. {1}'.format(fileName, err))
            if self.adjustedOnly & (data_mode == 'A'):
                continue
            
            #current method of creating dac
            nProf = root_grp.dimensions['N_PROF'].size
            doc = self.make_profile_doc(variables, dacName, remotePath, fileName, nProf, data_mode)
            if isinstance(doc, dict):
                doc = self.add_basin(doc, fileName)
                completedFileNames.append(fileName)
                self.documents.append(doc)
            if len(self.documents) >= self.dbDumpThreshold and self.addToDb:
                logging.warning( 'adding {} profiles to database'.format( len(self.documents) ) )
                self.add_many_profiles(self.documents, coll)
                self.documents = []
                if self.removeAddedFileNames:
                    self.delete_list_of_files(completedFileNames)
        percDone = 100 * idx / nFiles
        logging.warning( '{0} percent through files for thread: {1}'.format(percDone, threadN ) )
        logging.warning('all files have been read. dumping remaining documents to database')
        if len(self.documents) == 1 and self.addToDb:
            self.add_single_profile(self.documents[0], coll)
        elif len(self.documents) > 1 and self.addToDb:
            self.add_many_profiles(self.documents, coll)
        
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
        coll.delete_many({'_id': {'$in': idList}})

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
            except Exception as err:
                logging.debug('exception: param is not expected type {}'.format(err))
        else:
            logging.error(' check type: {}'.format(type(param)))
            pass
        return formatted_param.strip(' ')
    
    def format_list(self, paramList):
        outList = []
        for idx in range(0, paramList.shape[0]):
            params = list(map(lambda param: self.format_param(param), paramList[idx]))
            dimStatParam = [x for x in params if x]
            outList.append(dimStatParam)
        return outList
        
    def make_profile_doc(self, variables, dacName, remotePath, fileName, nProf, data_mode):
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
        
        stationParameters = self.format_list(variables['STATION_PARAMETERS'])
        try:
            p2D = netCDFToDoc(variables, dacName, remotePath, stationParameters, platformNumber, nProf, data_mode)
            doc = p2D.get_profile_doc()
            return doc
        except Exception as err:
            if 'no valid measurements.' in err.args[0]:
                logging.warning('Profile: {0} has no valid measurements. not going to add'.format(fileName.split('/')[-1]))
            else:
                logging.warning('Profile: {0} encountered error: {1}'.format(fileName.split('/')[-1], err.args))

    def add_single_profile(self, doc, coll, attempt=0):
        try:
            coll.replace_one({'_id': doc['_id']}, doc, upsert=True)
        except pymongo.errors.WriteError:
            logging.warning('check the following id '
                            'for _id : {0}'.format(doc['_id']))
        except bson.errors.InvalidDocument as err:
            logging.warning('bson error {1} for: {0}'.format(doc['_id'], err))
        except TypeError:
            logging.warning('Type error while inserting one document.')

    def add_many_profiles(self, documents, coll):
        try:
            coll.insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            writeErrors = bwe.details['writeErrors']
            problem_idx = []
            for we in writeErrors:
                problem_idx.append(we['index'])
            trouble_list = [documents[i] for i in problem_idx]
            logging.warning('bulk write failed for. adding failed documents on at a time')
            for doc in trouble_list:
                self.add_single_profile(doc, coll)
        except bson.errors.InvalidDocument:
            logging.warning('bson error')
            for doc in documents:
                self.add_single_profile(doc, coll)
        except TypeError:
            nonDictDocs = [doc for doc in documents if not isinstance(doc, dict)]
            logging.warning('Type error during insert_many method. Check documents.')
            logging.warning('number of non dictionary items in documents: {}'.format(len(nonDictDocs)))
