# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import json
import pdb
import os
import glob
import pymongo
import numpy as np

def get_feature_collection(file):
    with open(file) as dataFile:
        data = dataFile.read()
        featureColl = data.strip('var dataset = ')
        featureColl = json.loads(featureColl)
    return featureColl

def create_collection(collName, dbName):
    dbUrl = 'mongodb://localhost:27017/'
    client = pymongo.MongoClient(dbUrl)
    db = client[dbName]
    coll = db[collName]
    return coll

def format_features(features, reverseCoordinates=None):
    formattedFeatures = []
    for feature in features:
        doc = feature
        coords = feature['geometry']['coordinates']
        if reverseCoordinates:
            doc['geometry']['coordinates'] = [coords[1], coords[0]]
        else:
            doc['geometry']['coordinates'] = [coords[0], coords[1]]
        formattedFeatures.append(doc)
    return formattedFeatures

def create_covar_docs(files, forcastDays, dLat, dLong, forTest, reverseCoords):
    docs = []
    if forTest:
        files = files[0:10]
    for file in files:
        doc = {}

        lat = float(file.strip('.js').split('_')[-3])
        lng = float(file.strip('.js').split('_')[-1])
        _id = str(lng)+'_'+ str(lat) + '_' + str(forcastDays)
        doc['_id'] = _id
        doc['dLat'] = dLat
        doc['dLong'] = dLong
        doc['forcastDays'] = float(forcastDays)
        doc['geoLocation'] = {'type': 'Point', 'coordinates': [lng, lat]}
        featureColl = get_feature_collection(file)
        features = format_features(featureColl['features'], reverseCoords)
        doc['features'] = features
        docs.append(doc)
    return docs

def add_docs_to_database(docs, coll):
    try:
        coll.insert_many(docs)
    except Exception as err:
        print(err)
        pdb.set_trace()
    coll.create_index([('geoLocation', pymongo.GEOSPHERE)])

def main_add(localDir, coll, forcastDays, dLat=2, dLong=2, forTest=False, reverseCoords=None):
    files = np.array(glob.glob(os.path.join(localDir, '*.js')))
    print('number of files: {}'.format(len(files)))
    fileArrays = np.array_split(files, 50)
    for filesSubset in fileArrays:
        docs = create_covar_docs(filesSubset, forcastDays, dLat, dLong, forTest, reverseCoords)

        if not forTest:
            add_docs_to_database(docs, coll)
        else:
            print('not adding docs for test')

if __name__ == '__main__':

    dbName = 'argo2'
    covarBase = '/usr/src/covarMatricies'
    sLocalDir = os.path.join(covarBase,'60_day')
    collName = 'covars'
    pdb.set_trace()
    lLocalDir = os.path.join(covarBase,'140_day')
    coll = create_collection(collName, dbName)
    coll.drop()
    main_add(sLocalDir, coll, 60, 2, 2, forTest=False, reverseCoords=True)
    main_add(lLocalDir, coll, 140, 2, 2, forTest=False, reverseCoords=True)

    for idx in range(0, 16):
        forcastDays = (idx+1)*120
        print('on folder: {0} forcastDays: {1}'.format(idx, forcastDays))
        covarPath = os.path.join(covarBase,str(idx))
        main_add(covarPath, coll, forcastDays, 2, 3, forTest=False, reverseCoords=False)

    # add for testing purposes
    # testDbName = 'argo-express-test'
    # testColl = create_collection(collName, testDbName)
    # testColl.drop()
    # main_add(sLocalDir, testColl, 60, forTest=True)
    # main_add(lLocalDir, testColl, 140, forTest=True)

        
