# -*- coding: utf-8 -*-
import logging
import os
from numpy import array_split
import multiprocessing as mp
import pandas as pd
from numpy import array_split
import csv
import tmpFunctions as tf
import glob
from datetime import datetime

import re
import argparse
import pdb

def format_logger(filename, level=logging.INFO):
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if os.path.exists(os.path.join(os.getcwd(), filename)):
        os.remove(filename)
    logging.basicConfig(format=FORMAT,
                        filename=filename,
                        level=level)    

def tmp_clean_up():
    logging.warning('setting date updated to: {}'.format(tf.todayDate))
    tf.write_last_updated(tf.todayDate)
    logging.warning('Cleaning up space')
    tf.clean_up_space()

def create_df_of_files(files):
    df = pd.DataFrame()
    df['file'] = files
    df['filename'] = df['file'].apply(lambda x: x.split('/')[-1])
    df['profile'] = df['filename'].apply(lambda x: re.sub('[MDARS(.nc)]', '', x))
    df['platform'] = df['profile'].apply(lambda x: re.sub(r'(_\d{3})', '', x))
    df['profile'] = df['profile'].apply(lambda x: re.sub(r'(_0{1,2})', '_', x))
    df['prefix'] = df['filename'].apply(lambda x: re.sub(r'[0-9_(.nc)]', '', x))
    df['dac'] = df['file'].apply(lambda x: x.split('/')[-4])
    return df   

def remove_duplicate_if_mixed_or_synthetic(files):
    '''remove platforms from core that exist in mixed or synthetic df'''
    df = create_df_of_files(files)
    def cat_prefix(x):
        if 'S' in x:
            P  = 'S'
        elif 'M' in x:
            P = 'M'
        else:
            P= 'C'
        return P
    df['cat'] = df.prefix.apply(cat_prefix).astype('category')
    df['cat'] = df['cat'].cat.set_categories(['S', 'M', 'C'], ordered=True)
    df = df.sort_values(['profile', 'cat'])
    df = df.drop_duplicates(subset=['profile'], keep='first')
    return df

def get_df_to_add(localDir, dacs=[]):
    '''
    gathers a list of files that will be added to mongodb. 
    Removes core if there is a mixed file
    Removes mixed and core if there is a synthetic file.
    '''
    files = []
    reBR = re.compile(r'^(?!.*BR\d{1,})') # ignore characters starting with BR followed by a digit
    if len(dacs) != 0:
        for dac in dacs:
            logging.debug('On dac: {0}'.format(dac))
            files = files+glob.glob(os.path.join(localDir, dac, '**', 'profiles', '*.nc'))
    else:
        logging.debug('adding profiles individually')
        files = files+glob.glob(os.path.join(localDir, '**', '**', 'profiles', '*.nc'))

    files = list(filter(reBR.search, files))
    df = remove_duplicate_if_mixed_or_synthetic(files)
    return df

def dir_path(dirString):
    if os.path.isdir(dirString):
        return dirString
    else:
        raise NotADirectoryError(dirString)

def get_mirror_dir(args):
    mySystem = os.uname().nodename
    if mySystem == 'carby':
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    elif mySystem == 'kadavu.ucsd.edu':
        OUTPUT_DIR = os.path.join('/home', 'tylertucker', 'ifremer')
    elif mySystem == 'ciLab':
        OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    else:
        print('pc not found. assuming default')
        OUTPUT_DIR = os.path.join('/data/argovis/storage', 'ifremer')

    if args.subset == 'trouble':
        OUTPUT_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'troublesome-files')

    if (args.subset == 'tmp') or (args.subset == 'dateRange'):
        if args.minDate and args.maxDate:
            minDate = datetime.strptime(args.minDate, '%Y-%m-%d')
            maxDate = datetime.strptime(args.maxDate, '%Y-%m-%d')
        else:
            minDate = tf.get_last_updated(filename='lastUpdated.txt')
            maxDate = datetime.today()
        df = tf.get_df_from_dates_updated(minDate, maxDate)
        logging.warning('Num of files downloading to tmp: {}'.format(df.shape[0]))
        tf.create_dir_of_files(df, tf.GDAC, tf.FTP, tf.tmpDir)
        logging.warning('Download complete. Now going to add to db: {}'.format(args.dbName))
        OUTPUT_DIR = './tmp'
    
    if (args.mirrorDir):
        pdb.set_trace()
        OUTPUT_DIR = dir_path(args.mirrorDir)

    return OUTPUT_DIR

def run_parallel_process(ad, files, ncFileDir, npes=1):
    if npes == 1:
        ad.add_locally(ncFileDir, files)
    else:
        fileArray = array_split(files, npes)
        processes = [mp.Process( target=ad.add_locally, args=(ncFileDir, fileChunk, threadN) ) for threadN, fileChunk in enumerate(fileArray)]
        for p in processes:
            p.start()
        for p in processes:
            p.join()    


def cut_perc(df, nPerc, nArrays):
    '''
    cuts the top n percent rows of each element thats to be split. and sent through in parallel
    Example: cut_perc(ad, files, 40, 4) # cut top 40 percent of 4 lists
    '''
    outDf = pd.DataFrame()
    fileArray = array_split(df.file.tolist(), nArrays)
    for someFiles in fileArray:
        portion_df = create_df_of_files(someFiles)
        length = portion_df.shape[0]
        nRows = int((1 - nPerc/100.0) * length)
        portion_df.tail(nRows).file.tolist()
        outDf = pd.concat([outDf, df], sort=False, axis=0)
    return outDf

def single_out_threads(ad, files, kArrays, nArrays):
    '''
    splits into nArrays and keeps kArrays specified
    Example: single_out_threads(ad, files, [0], 4) # keep first of 4 arrays made up of files.
    '''
    outFiles = []
    fileArray = array_split(files, nArrays)
    for thread, someFiles in enumerate(fileArray):
        if not thread in kArrays:
            continue
        df = ad.create_df_of_files(someFiles)
        truncFiles = df.file.tolist()
        outFiles += truncFiles
    return outFiles

def get_nc_files_from_rsync_output(file, ncFileDir):
    '''
    parses through rsync output to collect a list of nc files.
    '''
    logging.warning('processing: {}:'.format(file))
    content = []
    with open(file, 'r') as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    content = [x for x in content if x.startswith('>')]  # New files start with '>'
    content = [x for x in content if x.endswith('.nc' )]
    content = [x.split(' ')[1] for x in content]
    content = [x for x in content if re.search(r'\d+.nc', x)]
    content = [os.path.join(ncFileDir, profile) for profile in content]
    return content

def format_sysparams():
    defaultNpes = mp.cpu_count()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--logName", help="log file name", type=str, nargs='?', default='default.log')
    parser.add_argument("--subset", help="which dacs to use (all minor, coriolis, aoml, trouble, deep, bgc, delayed, adjusted, adjAndDelay, tmp, dateRange)", type=str, nargs='?', default='all')
    parser.add_argument("--npes", help="number of processors", type=int, nargs='?', default=defaultNpes)

    parser.add_argument("--dbName", help='name of database', type=str, nargs='?', default='argo')
    parser.add_argument("--dbDumpThreshold", help="number of profiles to add at a time", type=int, nargs='?', default=1000)
    parser.add_argument("--qcThreshold", help="qc tolerance for", type=str, nargs='?', default='1')
    parser.add_argument("--basinFilename", help="number of processors", type=str, nargs='?', default='../basinmask_01.nc')
    parser.add_argument("--addToDb", help="number of processors", type=bool, nargs='?', default=True)
    parser.add_argument("--removeExisting", help="", type=bool, nargs='?', default=True)
    parser.add_argument("--removeAddedFileNames", help="delete files after adding. Used for testing only!", type=bool, nargs='?', default=False)
    parser.add_argument("--adjustedOnly", help="add adjusted profiles only", type=bool, nargs='?', default=False)
    parser.add_argument("--minDate", help="min date used for tmp subset", type=str, nargs='?')
    parser.add_argument("--maxDate", help="max date used for tmp subset only", type=str, nargs='?')
    parser.add_argument("--mirrorDir", help="dir used for data", type=str, nargs='?')
    args = parser.parse_args()
    return args

def get_dacs(dacStr, customDacList=None):
    if dacStr == 'minor':
        dacs = ['nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois', 'jma', 'csiro']
    elif dacStr == 'coriolis':
        dacs = ['coriolis']
    elif dacStr == 'aoml':
        dacs= ['aoml']
    else:
        dacs = ['aoml', 'coriolis', 'nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois', 'jma', 'csiro']
    if isinstance(customDacList, list):
        dacs = customDacList
    return dacs

def reduce_files(args, df):

    if args.subset == 'adjAndDelay':
        df = df[df.prefix.isin({'SD', 'SDD', 'MD', 'MDD', 'D', 'DD' })]

    if args.subset == 'bgc':
        df = df[ (df.prefix.str.startswith('S')) | (df.prefix.str.startswith('M')) ]

    if args.subset == 'synthetic':
        df = df[ (df.prefix.str.startswith('S')) ]

    if args.subset == 'adjusted':
        '''python add_profiles.py --logName adjusted.log --adjustedOnly 1 --subset adjusted'''
        adf = pd.read_csv('AdjustedProfs.txt')
        adf['platform'] = adf['_id'].apply(lambda x: x.split('_')[0])
        platforms = adf.platform.unique().tolist()
        df = df[df['platform'].isin(platforms)]

    if args.subset == 'deep':
        with open('deepPlatforms.csv', 'r') as f:
            reader = csv.reader(f)
            deepProfList = list(reader)
            deepProfList = [x[0] for x in deepProfList]
        df = df[ df.platform.isin(deepProfList) ]

    if args.subset == 'missingDataMode':
        df_filter = pd.read_json('missing_data_mode.json', dtype=str)
        df_filter = df_filter.rename(columns={'_id':'profile'})
        dfComb = pd.merge(df, df_filter, how='inner', on='profile')
        df = dfComb
    return df