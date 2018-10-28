

"""Procedures to load Argo profiles into ArgoVis MongoDB


   The first time run like:
   python argo4MongoDB.py --mode=create --mongodb-host localhost --npes 4 --basinMask ./basinmask_01.nc  ftp://ftp.ifremer.fr/ifremer/argo/dac/

   After that, run on append mode, which will only add new profiles into DB.
   python argo4MongoDB.py --mode=append --mongodb-host localhost --npes 4 --basinMask ./basinmask_01.nc  ftp://ftp.ifremer.fr/ifremer/argo/dac/
"""

import os
import re
import multiprocessing as mp
import time
from ftplib import FTP
import logging

import numpy as np
import netCDF4
from scipy.interpolate import griddata
import pymongo
from pymongo import MongoClient
import pymongo.errors
import bson.errors

import argo
from ftp import find_ftp_files, profiles_from_ftp


######### DEFINE VARS
NaN_MongoDB = -999
NaN_MongoDB_char = '-'
min_PRES_DEEP_ARGO = 2500
#########

logger = logging.getLogger('argo4MongoDB')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def find_nc_files(rootdir, pattern='^[DR]\d+_\d+D?.nc'):
    """Find files with pattern inside rootdir (generator)

       This can be used like
       for f in find_nc_files('my/dir/with/data'):
           print(f)
    """
    for root, dirnames, filenames in os.walk(rootdir):
        for f in [f for f in filenames if re.match(pattern, f)]:
            yield os.path.join(root, f)


def get_basin(lat, lon, basin_filename='basinmask_01.nc'):
    """Returns the basin code for a given lat lon coordinates

       Ex.:
       basin = get_basin(15, -38, '/path/to/basinmask_01.nc')
    """
    nc = netCDF4.Dataset(basin_filename, 'r')

    assert nc.variables['LONGITUDE'].mask == True
    assert nc.variables['LATITUDE'].mask == True

    idx = np.nonzero(~nc.variables['BASIN_TAG'][:].mask)
    basin = nc.variables['BASIN_TAG'][:][idx].astype('i')
    coords = np.stack([nc.variables['LATITUDE'][idx[0]],
                       nc.variables['LONGITUDE'][idx[1]]]).T

    return int(griddata(coords, basin, (lon, lat), method='nearest'))


def apply_qc(ds, min_PRES_DEEP_ARGO, inplace=False):
    """Mask data that failed QC criterion
    """
    assert 'PRES_QC' in ds, "Missing PRES_QC"
    assert 'TEMP_QC' in ds, "Missing TEMP_QC"

    if not inplace:
        ds = ds.copy(deep=True)

    valid_pres_idx = ds.PRES_QC.isin((b'1', '1')) | \
            (ds.PRES_QC.isin((b'2', '2')) & (ds.PRES > min_PRES_DEEP_ARGO))
    ds['PRES'] = ds.PRES.where(valid_pres_idx, np.nan)

    valid_temp_idx = ds.TEMP_QC.isin((b'1', '1')) \
            | (ds.TEMP_QC.isin((b'2', '2')) & (ds.PRES > min_PRES_DEEP_ARGO))
    valid_temp_idx = valid_temp_idx & valid_pres_idx
    ds['TEMP'] = ds.TEMP.where(valid_temp_idx, np.nan)

    if ('PSAL' in ds) and ('PSAL_QC' in ds):
        valid_psal_idx = ds.PSAL_QC.isin((b'1', '1')) | \
                (ds.PSAL_QC.isin((b'2', '2')) & (ds.PRES > min_PRES_DEEP_ARGO))
        valid_psal_idx = valid_psal_idx & valid_temp_idx
        ds['PSAL'] = ds.PSAL.where(valid_psal_idx, np.nan)

    ds = ds.dropna(dim='N_LEVELS', how='all', subset=['TEMP'])

    if not inplace:
        return ds


def load_profile(f, min_PRES_DEEP_ARGO, basin_filename):
    """

       FTPServer = 'usgodae.org'
       FTPServer = 'ftp.ifremer.fr'
    """
    try:
        if type(f) == str:
            profile = argo.get_profiles_from_nc(f)[0]
        else:
            with FTP(f['host']) as conn:
                conn.login()
                filepath = os.path.join(f['path'], f['filename'])
                profile = profiles_from_ftp(conn, filepath)[0]
                logger.debug('Read profile from: %s %s' %
                        (f['host'], filepath))
    except:
        logger.critical('Failed to read file: %s' % f)
        return

    try:
        profile = profile4mongodb(profile, min_PRES_DEEP_ARGO, basin_filename)
    except:
        logger.critical('Failed to digest file: %s' % f)
        return

    return profile


def profile4mongodb(profile, min_PRES_DEEP_ARGO, basin_filename):
    # define the variables of interest
    vars_N_PROF = ['_id','PLATFORM_NUMBER', 'PI_NAME', 'STATION_PARAMETERS', 'CYCLE_NUMBER', 'DIRECTION', 'DATA_CENTRE', 'DATA_MODE', 'WMO_INST_TYPE', 'JULD', 'JULD_QC', 'LATITUDE', 'LONGITUDE', 'POSITION_QC', 'POSITIONING_SYSTEM', 'PRES', 'PRES_QC', 'PRES_ADJUSTED', 'PRES_ADJUSTED_QC', 'TEMP', 'TEMP_QC', 'TEMP_ADJUSTED', 'TEMP_ADJUSTED_QC', 'PSAL', 'PSAL_QC', 'PSAL_ADJUSTED', 'PSAL_ADJUSTED_QC', 'VERTICAL_SAMPLING_SCHEME', 'PLATFORM_TYPE']

    profile['_id'] = "{0}_{1}".format(profile.PLATFORM_NUMBER.values,
                                  profile.CYCLE_NUMBER.values)
    if profile['DIRECTION'] == 'D':
        profile['_id'] += 'D'
    x_id = profile['_id'].values

    profile = profile[[v for v in vars_N_PROF if v in profile]]

    if ('POSITION_QC' in profile) and profile.POSITION_QC.isin(('3', '4')):
        logger.error('POSITION_QC flagged %s: %s' %
                (profile.POSITION_QC.values, x_id))
        return
    if ('JULD_QC' in profile) and profile.JULD_QC.isin(('3', '4')):
        logger.error('JULD_QC flagged %s: %s' %
                (profile.JULD_QC.values, x_id))
        return

    apply_qc(profile, min_PRES_DEEP_ARGO, inplace=True)
    if profile.N_LEVELS.size == 0:
        logger.error('None valid data on profile: %s' % x_id)
        return
    else:
        logger.debug('There are valid data on profile: %s' % x_id)

    if ('PSAL' not in profile):
        logger.info('Missing PSAL on: %s' % x_id)
    elif profile.PSAL.isnull().all():
        profile = profile.drop(['PSAL', 'PSAL_QC'])
        logger.warning('None valid PSAL on profile: %s' % x_id)

    for v in [v for v in profile.variables if v[-3:]=='_QC']:
        profile[v] = profile[v].where(~profile[v].isnull(), 0).astype('i')
    if profile.PRES.max() < min_PRES_DEEP_ARGO:
        for v in ['PRES_QC', 'TEMP_QC', 'PSAL_QC']:
            if v in profile:
                profile.drop(v)
    for v in ['PRES', 'TEMP', 'PSAL']:
        if (v in profile) and \
                (not profile[v].to_masked_array().compressed().any()):
                    profile.drop(v)
                    if v+'_QC' in profile:
                        profile.drop('%s_QC' % v)

    for v in [v for v in profile.variables if hasattr(profile[v], 'isnull')]:
        if profile[v].dtype.kind in ['i', 'f']:
            profile[v].where(~profile[v].isnull(), NaN_MongoDB)
        elif profile[v].dtype.kind in ['U', 'S', 'O']:
            profile[v].where(~profile[v].isnull(), NaN_MongoDB_char)

    STATION_PARAMETERS_in_MongoDB = ['PRES', 'TEMP']

    profile['PRES_max_for_TEMP'] = profile.PRES.max()
    profile['PRES_min_for_TEMP'] = profile.PRES.min()
    if 'PSAL' in profile:
        profile['PRES_max_for_PSAL'] = profile.dropna(dim='N_LEVELS',
                                                      subset=['PSAL']
                                                      )['PRES'].max()
        profile['PRES_min_for_PSAL'] = profile.dropna(dim='N_LEVELS',
                                                      subset=['PSAL']
                                                      )['PRES'].min()
        STATION_PARAMETERS_in_MongoDB.append('PSAL')

    output = profile.to_dict()['data_vars']
    for v in output:
        output[v] = output[v]['data']

    output['STATION_PARAMETERS_in_MongoDB'] = STATION_PARAMETERS_in_MongoDB

    try:
        output['BASIN'] = get_basin(float(profile['LATITUDE']),
                                    float(profile['LONGITUDE']),
                                    basin_filename)
    except:
        logger.warning('Using basin mask = NaN on profile: %s' % x_id)
        output['BASIN'] = np.nan

    return output


def find_files(src):
    """Find files to process inside src (local or FTP)

       Ex:
       Local: src='/data/argo/sio/'

       FTP: src='ftp://ftp.ifremer.fr/ifremer/argo/dac/'
    """
    if os.path.isfile(src):
        return [src]

    rule = 'ftp://(.*?)(/.*)'
    if re.match(rule, src):
        ftp_server, src = re.match(rule, src).groups()
        logger.info('Getting data from ftp://%s' % ftp_server)
        conn = FTP(ftp_server)
        conn.login()
        return find_ftp_files(conn, src)

    assert os.path.exists(src), "Path doesn't exist: %s" % src
    return find_nc_files(src)


class Argo4MongoDB(object):
    """Read and digest Argo files in parallel

       This is a generator that reads several files in parallel and return
         one processed profile at a time as a dictionary.

      Arguments:
       - src [str]: Source of files to insert. Can be a local absolute path or
                      an ftp server with path. All child directories inside
                      src will be considered. Two examples are:
                      src = '/data/argo/profiles'
                      or
                      src = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
       - min_PRES_DEEP_ARGO [int]: Max pressure for a regular argo.
       - basin_filename [str]: Path to basin mask file.
       - npes [int]: Number of parallel process

      It can be used like:

       >>> src = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
       >>> for d in Argo4MongoDB(src, 2500, './data/basinmask_01.nc', npes=8):
       >>>     print(d['_id'], len(d['TEMP']), d['PRES_max_for_TEMP'])
       >>>     my_func_to_insert_single_profile_into_MongoDB(d)

       ATENTION, at this point I'm not doing anything with failures. I'll
         improve that, but this should allow to load a bunch of datafiles
         into DB to allow the rest of the project to advance.
    """
    def __init__(self, src, min_PRES_DEEP_ARGO, basin_filename, npes=None):
        logger.info('Initializing Argo4MongoDB')

        if npes is None:
            npes = mp.cpu_count()
        self.queue = mp.Queue(int(3 * npes))
        self.p = mp.Process(target=self.reader,
                args=(src, min_PRES_DEEP_ARGO, basin_filename, npes))
        self.p.start()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        output = self.queue.get()
        if isinstance(output, str) and (output == 'END'):
            raise StopIteration
        return output


    def reader(self, src, min_PRES_DEEP_ARGO, basin_filename, npes):
        with mp.Pool(processes=npes) as pool:
            results = []
            for f in find_files(src):
                if len(results) >= npes:
                    while not results[0].ready():
                        time.sleep(1)
                    r = results.pop(0).get()
                    if r != None:
                        self.queue.put(r)
                results.append(pool.apply_async(
                    load_profile,
                    (f, min_PRES_DEEP_ARGO, basin_filename)))
            for r in results:
                r = r.get()
                if r != None:
                    self.queue.put(r)
        self.queue.put('END')


def insert_one(db, p):
    try:
        db.insert_one(p)
    except bson.errors.InvalidBSON:
        logger.error('Invalid BSON, profile: {}'.format(p['_id']))
    except bson.errors.InvalidDocument:
        logger.error('Invalid Document, profile: {}'.format(p['_id']))
    except pymongo.errors.DuplicateKeyError:
        logger.error('Trying to insert a duplicate, profile: {}'.format(
            p['_id']))
    except pymongo.errors.WriteError:
        logger.error('Fail to record profile: {}'.format(p['_id']))

    logger.debug('Inserted profile: {}'.format(p['_id']))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Insert Argo into MongoDB')
    parser.add_argument('--mode', dest='mode', type=str, metavar='MODE',
                        help='Running on [create|append|update] mode.')
    parser.add_argument('--mongodb-host', dest='dbhost', type=str,
                        metavar='HOST', help='MongoDB host.')
    parser.add_argument('--logfile', dest='logfile', type=str, default=None,
                        metavar='LOG', help='Filename to save logs.')
    parser.add_argument('--npes', dest='npes', type=int,
                        help='Number of parallel proceses.')
    parser.add_argument('--basinMask', metavar='FILE', type=str,
                        help='NetCDF with basin mask')
    parser.add_argument('src', metavar='Path to Argo files', type=str,
                        #inonargs='+',
                        help='sat files to process')

    args = parser.parse_args()
    assert os.path.isfile(args.basinMask), "basinMask is not valid"

    if args.logfile is not None:
        fh = logging.FileHandler(args.logfile)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    Profiles = Argo4MongoDB(args.src,
                            2500,
                            basin_filename=args.basinMask,
                            npes=args.npes)

    dbName = 'argo'
    collectionName = 'profiles'

    client = MongoClient('mongodb://%s:27017/' % args.dbhost)
    conn = client[dbName]
    db = conn[collectionName]

    if args.mode == 'create':
        print('Running on create mode')

        db.create_index([('date', pymongo.DESCENDING)])
        db.create_index([('platform_number', pymongo.DESCENDING)])
        db.create_index([('cycle_number', pymongo.DESCENDING)])
        db.create_index([('dac', pymongo.DESCENDING)])
        db.create_index([('geoLocation', pymongo.GEOSPHERE)])
        db.create_index([('geo2DLocation', pymongo.GEO2D)])

        for p in Profiles:
            insert_one(db, p)

    elif args.mode == 'append':
        for p in Profiles:
            if db.find({'_id': p['_id']}).count() == 0:
                insert_one(db, p)

    elif args.mode == 'update':
        print("I'm sorry, I'm not ready to update the database")
