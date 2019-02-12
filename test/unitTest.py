#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import sys
sys.path.append('..')
from argoDatabaseTest import argoDatabaseTest
from netCDFToDocTest import netCDFToDocTest
from testMeasToDf import measToDfTest
from deepTest import deepTest
from bgcTest import bgcTest
#from testTmpFunctions import testTmpFunctions
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

if __name__ == '__main__':
    unittest.main()


