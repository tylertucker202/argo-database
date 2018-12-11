#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 09:43:36 2018

@author: tyler
"""
import logging
import os
from numpy import array_split
import multiprocessing as mp
from numpy import array_split

def formatLogger(filename, level=logging.INFO):
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if os.path.exists(os.path.join(os.getcwd(), filename)):
        os.remove(filename)
    logging.basicConfig(format=FORMAT,
                        filename=filename,
                        level=level)    

def runParallelProcess(ad, files, npes=1, ncFileDir):
    npes = mp.cpu_count()
    fileArray = array_split(files, npes)
    processes = [mp.Process(target=ad.add_locally, args=(ncFileDir, fileChunk)) for fileChunk in fileArray]
    for p in processes:
        p.start()
    for p in processes:
        p.join()    