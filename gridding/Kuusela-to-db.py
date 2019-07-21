import numpy as np
import pandas as pd
import pymongo
import pdb
from datetime import datetime, timedelta
from scipy.io import loadmat
import os
import glob
import logging

def create_collection(dbName='argo2', collectionName='kuusela'):
    dbUrl = 'mongodb://localhost:27017/'
    client = pymongo.MongoClient(dbUrl)
    db = client[dbName]
    coll = db[collectionName]
    coll = init_profiles_collection(coll)
    return coll

def init_profiles_collection(coll):
    try:
        coll.create_index([('date', pymongo.DESCENDING)])
        coll.create_index([('pres', pymongo.DESCENDING)])
        coll.create_index([('data.LATITUDE', pymongo.DESCENDING)])
        coll.create_index([('data.LONGITUDE', pymongo.ASCENDING)])
        
        #may want to store as geojson feature collection one day
        #coll.create_index([('data.geometries', pymongo.GEOSPHERE)])

    except:
        logging.warning('not able to get collections or set indexes')
    return coll

def transform_lon(lon):
    '''
    Transforms longitude from absolute to -180 to 180 deg
    '''
    if lon >= 180:
        lon -= 360
    return lon

def make_docs(files, dataVal='predGrid'):
    docs = []
    for file in files:
        doc = {}
        anomData = loadmat(file)
        fa = file.split('/')[-1].split('_')
        year = fa[-1].replace('.mat', '')
        month = fa[-2]
        year_month = year + month

        date = datetime.strptime(year_month, '%Y%m')
        presLevel = float(fa[-6].replace('at', '').replace('dbar', ''))
        latGrid = anomData['latGrid'].flatten()
        lonGrid = anomData['longGrid'].flatten()
        values = anomData[dataVal].flatten()
        df = pd.DataFrame()
        df['LATITUDE'] = latGrid
        df['LONGITUDE'] = lonGrid
        df['LONGITUDE'] = df['LONGITUDE'].apply(lambda lon: transform_lon(lon))
        df['value'] = values
        df = df.fillna(float(-9999))
        dataDict = df.to_dict(orient='records')
        doc['dataVal'] = dataVal
        doc['data'] = dataDict
        doc['date'] = date
        doc['pres'] = float(presLevel)
        doc['cellsize'] = 1  #  Degree
        doc['NODATA_value'] = -9999
        docs.append(doc)
    return docs

if __name__ == '__main__':
    path = os.path.join('KuuselaResults', 'anom*')

    anomMats = glob.glob(path)
    print(len(anomMats))
    coll = create_collection()
    coll.drop()

    for fileChunk in np.array_split(anomMats, 10):
        docs = make_docs(fileChunk)
        print(len(docs))
        coll.insert_many(docs)
    
    # make for express testing
    testColl = create_collection(dbName='argo-express-test', collectionName='kuusela')
    testColl.drop()
    testColl.insert_many(docs)
