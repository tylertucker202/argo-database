# -*- coding: utf-8 -*-
import logging
import os
from numpy import array_split
import multiprocessing as mp
from numpy import array_split

def format_logger(filename, level=logging.INFO):
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if os.path.exists(os.path.join(os.getcwd(), filename)):
        os.remove(filename)
    logging.basicConfig(format=FORMAT,
                        filename=filename,
                        level=level)    

def run_parallel_process(ad, files, ncFileDir, npes=1):
    if npes == 1:
        ad.add_locally(ncFileDir, files)
    else:
        fileArray = array_split(files, npes)
        processes = [mp.Process(target=ad.add_locally, args=(ncFileDir, fileChunk)) for fileChunk in fileArray]
        for p in processes:
            p.start()
        for p in processes:
            p.join()    
