import pymongo
from netCDF4 import Dataset
import numpy as np
from scipy.interpolate import griddata
import logging
import pdb
import os

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT,
                    filename='fix-basin.log',
                    level=logging.DEBUG)  
logging.debug('starting script')
nc = Dataset('basinmask_01.nc', 'r')
assert nc.variables['LONGITUDE'].mask == True
assert nc.variables['LATITUDE'].mask == True

bdx = np.nonzero(~nc.variables['BASIN_TAG'][:].mask)
basinVal = nc.variables['BASIN_TAG'][:][bdx].astype('i')
coords = np.stack([nc.variables['LATITUDE'][bdx[0]],
                   nc.variables['LONGITUDE'][bdx[1]]]).T

dbUrl = 'mongodb://localhost:27017/'
client = pymongo.MongoClient(dbUrl)
db = client['argo2']
coll = db['profiles']

cursor = coll.find()
clen = coll.find().count()
try:
    for idx, doc in enumerate(cursor):
        if idx % 2000 == 0:
            logging.debug('on idx: {}'.format(idx))
        try:
            basin = int(griddata(coords, basinVal, (doc['lat'], doc['lon']), method='nearest'))
        except Exception as err:
            logging.warning('Exception occured: {0} for idx: {}'.format(idx, err))
            basin = int(-999)
            pass
        coll.update({'_id': doc['_id']},{'$set': {'BASIN': basin} }, upsert=False, multi=False)
    logging.debug('exited loop: idx {0} out of {1}'.format(idx, clen))

finally:
    logging.debug('exiting script: idx {0} out of {1}'.format(idx, clen))
