import pymongo
from netCDF4 import Dataset
import numpy as np
from scipy.interpolate import griddata
import pdb

nc = Dataset('basinmask_01.nc', 'r')
assert nc.variables['LONGITUDE'].mask == True
assert nc.variables['LATITUDE'].mask == True

idx = np.nonzero(~nc.variables['BASIN_TAG'][:].mask)
basinVal = nc.variables['BASIN_TAG'][:][idx].astype('i')
coords = np.stack([nc.variables['LATITUDE'][idx[0]],
                   nc.variables['LONGITUDE'][idx[1]]]).T


dbUrl = 'mongodb://localhost:27017/'
client = pymongo.MongoClient(dbUrl)
db = client['argo2']
coll = db['profiles']

cursor = coll.find()

for idx, doc in enumerate(cursor):
    try:
        basin = int(griddata(coords, basinVal, (doc['lat'], doc['lon']), method='nearest'))
    except Exception:
        basin = int(-999)
        pass
    coll.update({'_id': doc['_id']},{'$set': {'BASIN': basin} }, upsert=False, multi=False)
