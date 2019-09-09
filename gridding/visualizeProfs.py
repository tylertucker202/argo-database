import pandas as pd
import numpy as np

import os, glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pdb
import requests

import sys

def get_profile(profile_number):
    url = 'https://argovis.colorado.edu/catalog/profiles/{}'.format(profile_number)
    resp = requests.get(url)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    profile = resp.json()
    return profile

def parse_into_df(profiles):
    meas_keys = profiles[0]['measurements'][0].keys()
    df = pd.DataFrame(columns=meas_keys)
    for profile in profiles:
        profileDf = pd.DataFrame(profile['measurements'])
        profileDf['cycle_number'] = profile['cycle_number']
        profileDf['profile_id'] = profile['_id']
        profileDf['lat'] = profile['lat']
        profileDf['lon'] = profile['lon']
        profileDf['date'] = profile['date']
        profileDf['position_qc'] = profile['position_qc']
        profileDf['date_qc'] = profile['date_qc']
        df = pd.concat([df, profileDf], sort=False)
    return df

def get_selection_profiles(startDate, endDate, shape, presRange=None):
    url = 'https://argovis.colorado.edu/selection/profiles'
    url += '?startDate={}'.format(startDate)
    url += '&endDate={}'.format(endDate)
    url += '&shape={}'.format(shape)
    if not presRange == None:
        pressRangeQuery = '&presRange=' + presRange
        url += pressRangeQuery
    print(url)
    resp = requests.get(url)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    profiles = resp.json()
    return profiles
    
def build_selection_page_url(startDate, endDate, shape, presRange=None):
    url = 'https://argovis.colorado.edu/selection/profiles/devpage'
    url += '?startDate={}'.format(startDate)
    url += '&endDate={}'.format(endDate)
    url += '&shape={}'.format(shape)
    if not presRange == None:
        pressRangeQuery = '&presRange=' + presRange
        url += pressRangeQuery
    print(url)

def construct_box(coord, dLat=10., dLong=10.):
    shape = []
    for idx in range(1,5):
        latSign = (-1) ** ( (idx + 1)//2 % 2 + 1 )
        longSign = (-1) ** (idx // 2)
        corner = [coord['long'] + longSign * dLong/2, coord['lat'] + latSign * dLat/2]
        shape.append(corner)
    shape.append(shape[0])
    strShape = str([shape])
    strShape = strShape.replace(' ', '')
    return strShape

def get_profiles_by_id(_ids, presRange=None, printURL=False):
    '''get profiles listed in _ids list'''
    domainName = 'argovis.colorado.edu'
    baseURL = 'https://' + domainName + '/catalog/mprofiles/?'
    idsParam = 'ids=' + _ids
    url = baseURL + idsParam
    if presRange:
        presParam = '&presRange=' + presRange
        url += presParam
    if printURL:
        print(url)
    resp = requests.get(url)
    # Consider any status other than 2xx an error
    if not resp.status_code // 100 == 2:
        return "Error: Unexpected response {}".format(resp)
    profiles = resp.json()
    return profiles

def plot_scatter(df, title, xparam, yparam, ax):
    ax.set_title(title + ' ' + xparam)
    ax.set_xlabel(xparam)
    ax.set_ylabel(yparam)
    ax.invert_yaxis()
    x = df[xparam].values
    y = df[yparam].values
    ax.scatter(x, y, s=50)
    return ax
