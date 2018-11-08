#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  1 22:03:18 2018

@author: tyler
"""

import logging
import os
import sys
import ftputil
import pdb
from datetime import datetime
from os import path as ospath
import ftplib

class FTPWalk:
    """
    This class is contain corresponding functions for traversing the FTP
    servers using BFS algorithm.
    """
    def __init__(self, connection):
        self.connection = connection

    def listdir(self, _path):
        """
        return files and directory names within a path (directory)
        """

        file_list, dirs, nondirs = [], [], []
        try:
            self.connection.cwd(_path)
        except Exception as exp:
            print ("the current path is : ", self.connection.pwd(), exp.__str__(),_path)
            return [], []
        else:
            self.connection.retrlines('LIST', lambda x: file_list.append(x.split()))
            for info in file_list:
                ls_type, name = info[0], info[-1]
                if ls_type.startswith('d'):
                    dirs.append(name)
                else:
                    nondirs.append(name)
            return dirs, nondirs

    def walk(self, path='/'):
        """
        Walk through FTP server's directory tree, based on a BFS algorithm.
        """
        dirs, nondirs = self.listdir(path)
        yield path, dirs, nondirs
        for name in dirs:
            path = ospath.join(path, name)
            yield from self.walk(path)
            # In python2 use:
            # for path, dirs, nondirs in self.walk(path):
            #     yield path, dirs, nondirs
            self.connection.cwd('..')
            path = ospath.dirname(path)
            
if __name__ == '__main__':
    minDate = datetime(2018, 10, 31)
    ftpPath = os.path.join(os.sep, 'pub', 'outgoing', 'argo', 'dac')
    GDAC = 'usgodae.org'
    profileText = 'ar_index_global_prof.txt'
    pdb.set_trace()
    ftp = ftplib.FTP(GDAC)     # connect to host, default port
    ftp.login()                     # user anonymous, passwd anonymous@
    ftp.cwd(ftpPath)           # change into "debian" directory
    '''
    dacs = ftp.nlst()
    lines = []
    ftp.retrlines('LIST', lines.append)
    for line in lines:
        line = ' '.join(line.split())
        words = line.split()
        date = words[-4:-2]
    
    ftpwalk = FTPWalk(ftp)
    
    for i in ftpwalk.walk(ftpPath):
        print(i)
    '''