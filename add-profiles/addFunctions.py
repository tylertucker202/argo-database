# -*- coding: utf-8 -*-
import logging
import os
from numpy import array_split
import multiprocessing as mp
from numpy import array_split
import argparse

def format_logger(filename, level=logging.INFO):
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if os.path.exists(os.path.join(os.getcwd(), filename)):
        os.remove(filename)
    logging.basicConfig(format=FORMAT,
                        filename=filename,
                        level=level)    

def getMirrorDir():
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


def cut_perc(ad, files, nPerc, nArrays):
    '''
    cuts the top n percent rows of each element thats to be split. and sent through in parallel
    Example: cut_perc(ad, files, 40, 4) # cut top 40 percent of 4 lists
    '''
    outFiles = []
    fileArray = array_split(files, nArrays)
    for someFiles in fileArray:
        df = ad.create_df_of_files(someFiles)
        length = df.shape[0]
        nRows = int((1 - nPerc/100.0) * length)
        truncFiles = df.tail(nRows).file.tolist()
        outFiles += truncFiles
    return outFiles

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

def format_sysparams():
    defaultNpes = mp.cpu_count()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--logName", help="log file name", type=str, nargs='?', default='default.log')
    parser.add_argument("--subset", help="which dacs to use (minor, coriolis, aoml, trouble, deep, bgc, delayed, adjusted)", type=str, nargs='?', default='trouble')
    parser.add_argument("--npes", help="number of processors", type=int, nargs='?', default=defaultNpes)

    parser.add_argument("--dbName", help='name of database', type=str, nargs='?', default='argo')
    parser.add_argument("--replaceProfile", help="replace profile if existing", type=bool, nargs='?', default=True)
    parser.add_argument("--dbDumpThreshold", help="number of profiles to add at a time", type=int, nargs='?', default=1000)
    parser.add_argument("--qcThreshold", help="qc tolerance for", type=str, nargs='?', default='1')
    parser.add_argument("--basinFilename", help="number of processors", type=str, nargs='?', default='../basinmask_01.nc')
    parser.add_argument("--addToDb", help="number of processors", type=bool, nargs='?', default=True)
    parser.add_argument("--removeExisting", help="", type=bool, nargs='?', default=False)
    parser.add_argument("--removeAddedFileNames", help="number of processors", type=bool, nargs='?', default=False)
    parser.add_argument("--adjustedOnly", help="add adjusted profiles only", type=bool, nargs='?', default=False)
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

def reduce_files(args, files, ad):

    if args.adjustedOnly:
        df = ad.create_df_of_files(files)
        df = df[df.prefix.isin({'MR', 'MDD', 'D', 'RD', 'DD', 'MRD'})]
        files = df.file.tolist()

    if args.subset == 'bgc':
        bgcFiles = []
        for file in files:
            fileName = file.split('/')[-1]
            if fileName.startswith('M') or fileName.startswith('S'):
                bgcFiles.append(file)
        files = bgcFiles

    if args.subset == 'deep':
        with open('deepPlatforms.csv', 'r') as f:
            reader = csv.reader(f)
            deepProfList = list(reader)
            deepProfList = [x[0] for x in deepProfList]
        deepFiles = []
        for file in files:
            platform = file.split('/')[-3]
            if platform in deepProfList:
                deepFiles.append(file)
        files = deepFiles
    return files