import numpy as np
import pandas as pd
import xarray as xr
import pymongo
import pdb
from datetime import datetime, timedelta
from scipy.io import loadmat
import os
import glob
import logging

def transform_lon(lon):
    '''
    Transforms longitude from absolute to -180 to 180 deg
    '''
    if lon >= 180:
        lon -= 360
    return lon

def make_doc(df, date, presLevel, dataVal):
    '''
    Takes df and converts it into a document for mongodb
    '''
    doc = {}
    df = df.rename(index=str, columns={dataVal: 'value'})
    df = df.fillna(float(-9999))
    dataDict = df.to_dict(orient='records')
    doc['data'] = dataDict
    doc['dataVal'] = dataVal
    doc['date'] = date
    doc['pres'] = float(presLevel)
    doc['cellsize'] = 1  #  Degree
    doc['NODATA_value'] = -9999
    return doc

def insert_pres_time_grid(tempAnom, coll, dataVal='ARGO_TEMPERATURE_ANOMALY', insertOne=False):
    for tdx, chunk in tempAnom.groupby('TIME'):
        month = int(tdx % 12 + 1)
        year = int(2004 + tdx // 12)
        date = datetime.strptime('{0}-{1}'.format(year, month), '%Y-%m')
        if not year == 2010: # only add 2010
            continue
        print(date)
        df = chunk.to_dataframe()
        df = df.reset_index()
        df['LONGITUDE'] = df['LONGITUDE'] 
        df['LONGITUDE'] = df['LONGITUDE'].apply(lambda lon: transform_lon(lon))

        for pdx, presDf in df.groupby('PRESSURE'):
            if not pdx in [5, 10, 200]:
                continue
            presDf = presDf.drop(['TIME', 'PRESSURE'], axis=1)
            doc = make_doc(presDf, date, pdx, dataVal)
            coll.insert_one(doc)
            if insertOne: # Use for testing
                return

def create_collection(dbName='argo', collectionName='rgTempAnom'):
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

if __name__ == '__main__':
    rgFilename = './RG_ArgoClim_Temp.nc'
    rg = xr.open_dataset(rgFilename, decode_times=False)

    coll = create_collection()
    coll.drop()

    dataVal='ARGO_TEMPERATURE_ANOMALY'
    tempAnom = rg[dataVal]
    insert_pres_time_grid(tempAnom, coll, dataVal)

    # make for express testing
    testColl = create_collection(dbName='argo-express-test', collectionName='rgTempAnom')
    testColl.drop()
    insert_pres_time_grid(tempAnom, testColl, dataVal='ARGO_TEMPERATURE_ANOMALY', insertOne=True)