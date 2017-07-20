# -*- coding: utf-8 -*-
import pymongo
import os
import logging
import pdb
from ftplib import FTP, error_perm
from urllib.parse import urlparse
from netCDF4 import Dataset

class ArgoDatabase(object):
    def __init__(self,
                 db_name,
                 collection_name):
        logging.debug('initializing make_database')
        self.init_database(db_name)
        self.init_collection(collection_name)
        self.float_dir = 'float_files'
        self.home_dir = os.getcwd()

    def init_database(self, db_name):
        logging.debug('initializing init_database')
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        # create database
        self.db = client[db_name]

    def init_collection(self, collection_name):
        try:
            self.coll = self.db[collection_name]
            # keeps values unique
            """
            if self.dates.count() == 0:
                self.dates.create_index(
                            [('date', pymongo.ASCENDING)], unique=True)"""
        except:
            logging.warning('not able to get collections')

    def get_float_folder_names(self):
        US_GODAE_URL = 'ftp://usgodae.org'
        AOML_PATH = '/pub/outgoing/argo/dac/aoml/'
        url = urlparse(US_GODAE_URL)

        with FTP(url.netloc) as ftp:  # connect to host, default port
            ftp.login()  # user anonymous, passwd anonymous@
            ftp.cwd(AOML_PATH)
            float_dirs = ftp.nlst()
            os.chdir(self.float_dir)
            for float_name in float_dirs:
                ftp.cwd(float_name)
                self.save_float_files(ftp, float_name)
                self.add_to_collection()
                ftp.cwd('..')
        os.chdir(self.home_dir)

    def save_float_files(self, ftp, float_name):
            file_names = ftp.nlst()
            file_names = list(filter(lambda x: x.endswith('.nc'), file_names))
            for file_name in file_names:
                try:
                    with open(file_name, 'wb') as fobj:
                        ftp.retrbinary('RETR ' + file_name, fobj.write)
                except TypeError:
                    print('Error in writing file: {}'.format(file_name))


    def add_to_collection(self):

        file_names = os.listdir(os.getcwd())
        for file_name in file_names:
            if not file_name.endswith('traj.nc'):
                continue
            rootgrp = Dataset(file_name, "r", format="NETCDF4")
            variables = rootgrp.variables
            KEYS = variables.keys
            FLOAT = variables['FLOAT_SERIAL_NO'][:]
            PSAL = variables['PSAL_ADJUSTED']
            TEMP = variables['TEMP_ADJUSTED']
            PRES = variables['PRES_ADJUSTED']
            LA
            LON = variables['LONGITUDE']
            DATE = variables['JULD'] # list of dates of measurements taken since float started


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='argoDatabase.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = os.path.join(HOME_DIR, 'output')
    # init database
    DB_NAME = 'argo'
    COLLECTION_NAME = 'test'
    DATA_DIR = os.path.join(HOME_DIR, 'data')
    NC_NAME = 'R20170624_prof_10.nc'

    ad = ArgoDatabase(DB_NAME, COLLECTION_NAME)
    ad.get_float_folder_names()
    ad.add_to_collection(DATA_DIR, NC_NAME)
