# -*- coding: utf-8 -*-
import pymongo
import os
import re
import glob
import logging
import numpy as np
from datetime import datetime
from netCDF4 import Dataset
import bson.errors
from bson.decimal128 import Decimal128
import pdb
from netCDFToDoc import netCDFToDoc

class argoDatabase(object):
    def __init__(self,
                 dbName,
                 collectionName, 
                 replaceProfile=False,
                 qcThreshold='1', 
                 dbDumpThreshold=10000,
                 removeExisting=True):
        logging.debug('initializing ArgoDatabase')
        self.init_database(dbName)
        self.init_profiles_collection(collectionName)
        self.home_dir = os.getcwd()
        self.replaceProfile = replaceProfile
        self.url = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
        self.qcThreshold = qcThreshold
        self.dbDumpThreshold = dbDumpThreshold
        self.removeExisting = removeExisting

    def init_database(self, dbName):
        logging.debug('initializing init_database')
        dbUrl = 'mongodb://localhost:27017/'
        client = pymongo.MongoClient(dbUrl)
        self.db = client[dbName]

    def init_profiles_collection(self, collectionName):
        try:
            self.profiles_coll = self.db[collectionName]
            self.profiles_coll.create_index([('date', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('platform_number', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('cycle_number', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('dac', pymongo.DESCENDING)])
            self.profiles_coll.create_index([('geoLocation', pymongo.GEOSPHERE)])
            self.profiles_coll.create_index([('geo2DLocation', pymongo.GEO2D)])
        except:
            logging.warning('not able to get collections or set indexes')

    def add_locally(self, localDir, howToAdd='all', files=[], dacs=[]):
        os.chdir(localDir)
        reBR = re.compile(r'^(?!.*BR\d{1,})') # ignore characters starting with BR followed by a digit
        if howToAdd=='by_dac_profiles':
            files = []
            logging.debug('adding dac profiles from path: {0}'.format(localDir))
            for dac in dacs:
                logging.debug('On dac: {0}'.format(dac))
                files = files+glob.glob(os.path.join(localDir, dac, '**', 'profiles', '*.nc'))     
        elif howToAdd=='profile_list':
            logging.debug('adding profiles in provided list')
        elif howToAdd=='profiles':
            logging.debug('adding profiles individually')
            files = files+glob.glob(os.path.join(localDir, '**', '**', 'profiles', '*.nc'))
            files = list(filter(reBR.search, files))
        else:
            logging.warning('howToAdd not recognized. not going to do anything.')
            return

        if self.removeExisting == True: # Removes profiles on list before adding list (redundant...but requested)
            #reExist = re.compile(r'.*D\d{1,}')
            #delayModeFiles = list(filter(reDelay.match,files))
            self.remove_profiles(files)
    

        documents = []
        for fileName in files:
            logging.info('on file: {0}'.format(fileName))
            dacName = fileName.split('/')[-4]
            root_grp = Dataset(fileName, "r", format="NETCDF4")
            remotePath = self.url + os.path.relpath(fileName,localDir)
            variables = root_grp.variables
            doc = self.make_profile_doc(variables, dacName, remotePath, fileName)
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

    def remove_profiles(self, files):
        #get profile ids
        idList = []
        for fileName in files:
            profileName = fileName.split('/')[-1]
            profileName = profileName[1:-3]
            idList.append(profileName)
            #self.profiles_coll.remove({'_id':profileName})
        #remove all profiles at once
        logging.debug('removing delayed profiles before reintroducing')
        logging.debug('number of profiles deleted: {}'.format(len(idList)))
        countBefore = self.profiles_coll.find({}).count()
        self.profiles_coll.delete_many({'_id': {'$in': idList}})
        countAfter = self.profiles_coll.find({}).count()
        delta = countBefore - countAfter
        self.profiles_coll.find({}).count()
        logging.debug('number of delayed profiles: {}'.format(delta))

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

    def make_profile_doc(self, variables, dacName, remotePath, fileName):
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
        stationParameters = list(map(lambda param: self.format_param(param), variables['STATION_PARAMETERS'][0]))
        numOfProfiles = variables['JULD'][:].shape[0]
        logging.info('number of profiles inside file: {}'.format(numOfProfiles))

        refDateArray = variables['REFERENCE_DATE_TIME'][:]
        refStr = ''.join([x.astype(str) for x in refDateArray])
        refDate = datetime.strptime(refStr, '%Y%m%d%H%M%S')
        idx = 0 #stometimes there are two profiles. The second profile is ignored.
        qcThreshold='1'
        try:
            p2D = netCDFToDoc(variables, dacName, refDate, remotePath, stationParameters, platformNumber, idx, qcThreshold)
            doc = p2D.get_profile_doc()
            return doc
        except AttributeError as err:
            logging.debug(err.args)
            return
        except TypeError as err:
            logging.warning('Type error encountered for profile: {}'.format(fileName))
            logging.warning('Reason: {}'.format(err.args))
        except ValueError as err:
            logging.warning('Value Error encountered for profile: {}'.format(fileName))
            logging.warning('Reason: {}'.format(err.args))
        except UnboundLocalError as err:
            logging.warning('Unbound Local Error encountered for profile: {}.'
                          ' Not going to add'.format(fileName))
            logging.warning('Reason: {}'.format(err.args))
        except: 
            pdb.set_trace()
            logging.warning('Error encountered for profile: {}'.format(fileName))
            logging.warning('Reason: unknown')

    def add_single_profile(self, doc, file_name, attempt=0):
        if self.replaceProfile == True:
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

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='argoTroublesomeProfiles.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = getOutput()
    # init database
    dbName = 'argoTrouble'
    collectionName = 'profiles'
    ad = argoDatabase(dbName, collectionName, True)
    ad.add_locally(OUTPUT_DIR, howToAdd='profiles')
    logging.debug('End of log file')
