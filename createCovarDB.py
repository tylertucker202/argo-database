# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Created on Mon Nov 19 15:08:59 2018

@author: tyler
"""

import json
import pdb
import os
import glob
import pymongo
import pandas as pd

def get_feature_collection(file):
    with open(file) as dataFile:
        data = dataFile.read()
        featureColl = data.strip('var dataset = ')
        featureColl = json.loads(featureColl)
    return featureColl

def create_collection(collName):
    dbUrl = 'mongodb://localhost:27017/'
    client = pymongo.MongoClient(dbUrl)
    db = client['argo2']
    coll = db[collName]
    return coll

def format_features(features):
    formattedFeatures = []
    for feature in features:
        doc = feature
        coords = feature['geometry']['coordinates']
        doc['geometry']['coordinates'] = [coords[1], coords[0]]
        formattedFeatures.append(doc)
    return formattedFeatures

def create_covar_docs(localDir):
    files = glob.glob(os.path.join(localDir, '*.js'))
    docs = []
    for file in files:
        doc = {}
        lat = float(file.strip('.js').split('_')[-3])
        lng = float(file.strip('.js').split('_')[-1])
        _id = str(lng)+'_'+ str(lat)
        doc['_id'] = _id
        doc['geoLocation'] = {'type': 'Point', 'coordinates': [lng, lat]}
        featureColl = get_feature_collection(file)
        features = format_features(featureColl['features'])
        doc['features'] = features
        docs.append(doc)
    return docs

def add_docs_to_database(collName, docs):
    coll = create_collection(collName)
    coll.drop()    
    for doc in docs:
        try:
            coll.insert(doc)
        except:
            pdb.set_trace()
            doc    
    coll.create_index([('geoLocation', pymongo.GEOSPHERE)])

localDir = './60_day'
collName = 'shortCovars'
docs = create_covar_docs(localDir)
add_docs_to_database(collName, docs)

localDir = './140_day'
collName = 'longCovars'
docs = create_covar_docs(localDir)
add_docs_to_database(collName, docs)
        
