import logging
import numpy as np
from datetime import datetime, timedelta
from measToDf import measToDf
import warnings
from math import isnan
import pdb
import pandas as pd

warnings.simplefilter('error', RuntimeWarning)
np.warnings.filterwarnings('ignore')

class netCDFToDoc(measToDf):

    def __init__(self,
                 variables,
                 dacName,
                 remotePath,
                 stationParameters,
                 platformNumber,
                 cycle,
                 nProf,
                 dataMode):
        logging.debug('initializing netCDFToDoc')
        self.platformNumber = platformNumber
        self.cycleNumber = cycle
        profileID = str(self.platformNumber) + '_' + str(self.cycleNumber)
        self.dataMode = dataMode
        self.decodeFormat = 'utf-8'
        measToDf.__init__(self, variables, stationParameters, nProf, profileID, dataMode)
        self.profileDoc = dict()
        self.deepFloatWMO = ['838' ,'849','862','874','864']
        self.stringValues = ['POSITIONING_SYSTEM', 'DATA_CENTRE', 'PI_NAME', 'WMO_INST_TYPE', 'VERTICAL_SAMPLING_SCHEME']
        self.make_profile_dict(dacName, remotePath)
    
    def get_profile_doc(self):
        return self.profileDoc

    def add_string_values(self, valueName):
        try:
            param = self.decode_param(valueName)
            self.profileDoc[valueName] = param
        except:
            logging.warning('error when adding {0} to document.'
                          ' Not going to add string'.format(valueName))
    def decode_param(self, valueName):
        param = self.variables[valueName]['data'][self.idx]
        return param.decode(self.decodeFormat).strip(' ')

    def add_param_data_mode(self):
        pdms = self.variables['PARAMETER_DATA_MODE']['data']
        dataModes = []
        for pdm in pdms:
            dataMode = []
            for dm in pdm:
                try:
                    dm = dm.decode(self.decodeFormat)
                except:
                        dm = '-999'
                dataMode.append(dm)
            dataModes.append(dataMode)
        self.profileDoc['PARAMETER_DATA_MODE'] = dataModes

    def add_platform_type(self):
        if 'PLATFORM_TYPE' in self.variables.keys():
            self.add_string_values('PLATFORM_TYPE')
        else:
            self.add_string_values('INST_REFERENCE')

    def add_max_min_pres(self, df, param, maxBoolean):
        if not param in df.columns:
            return
        try:
            reducedDf = df[ df[param] != -999 ]['pres']
            if reducedDf.empty:
                return
            if maxBoolean:
                presValue = reducedDf.max()
                presValue = presValue.astype(np.float64)
                maxMin = 'max'
            else:
                presValue = reducedDf.min()
                presValue = presValue.astype(np.float64)
                maxMin = 'min'

            paramName = 'pres_' + maxMin + '_for_' + param.upper()
            self.profileDoc[paramName] = presValue
        except:
            logging.warning('Profile {}: unable to get presmax/min, unknown exception.'.format(self.profileID))

    def add_date(self):
        try:
            date = self.variables['JULD']['data'][self.idx]
        except Exception as err:
            raise ValueError('Profile:{0} has unknown date. not going to add {1}'.format(self.profileID, err))
        self.profileDoc['date'] = date
        self.profileDoc['date_added'] = datetime.today()

    def add_date_qc(self):
        dateQC = int(self.decode_param('JULD_QC'))
        if dateQC in {3, 4}:
            raise ValueError('date_qc is a 3 or 4. Not going to add.')
        else:
            self.profileDoc['date_qc'] = dateQC

    def add_position_qc(self):
        position_qc = int(self.decode_param('POSITION_QC'))
        if not position_qc:
            logging.warning('setting POSITION_QC = -999')
            position_qc = '-999'
        if position_qc in {'3', '4'}:
            raise ValueError('POSITION_QC is a 3 or 4. Not going to add profile.')
        self.profileDoc['position_qc'] = position_qc

    def add_lat_lon(self):
        lat = self.variables['LATITUDE']['data'][self.idx]
        lon = self.variables['LONGITUDE']['data'][self.idx]
        if isnan(lat) or isnan(lon):
            lat, lon = -89.0, 0.0
            logging.warning('Profile:{0} has unknown lat-lon.'
                          ' Filling with 0, 0'.format(self.profileID))
        self.profileDoc['lat'] = lat
        self.profileDoc['lon'] = lon
        self.profileDoc['geoLocation'] = {'type': 'Point', 'coordinates': [lon, lat]}

    def check_if_deep_profile(self):
        measAndQC = self.format_measurements('PRES', 0)
        df = pd.DataFrame(measAndQC)
        df = self.do_qc_on_deep_meas(df, 'pres')
        maxPres = df['pres'].max()
        if maxPres >= 2500:
            isDeep = True
        else:
            isDeep = False
        return isDeep

    def add_BGC(self):
        try:
            self.profileDoc['bgcMeas'] = self.create_BGC()
        except Exception as err:
            logging.warning('Profile {0} bgc not created:{1}'.format(self.profileID, err))
        bgcMeasKeys = self.profileDoc['bgcMeas'][0].keys()
        #  Strip numbers
        bgcMeasKeys = [''.join(i for i in s if not i.isdigit()) for s in bgcMeasKeys]
        bgcKeys = [s.lower() for s in self.bgcList]
        if bool(set(bgcMeasKeys) & set(bgcKeys)):
            self.profileDoc['containsBGC'] = True
        else:
            del self.profileDoc['bgcMeas']
            logging.warning('Profile: {} contains poor quality bgc data. not going to include table'.format(self.profileID))

    def create_measurements_df(self, roundDec=True):
        try:
            if self.isDeep:
                #  self.profile_id = self.profile_id.strip('D') # D postfix should be used for direction only.
                self.profileDoc['isDeep'] = self.isDeep
                df = self.make_deep_profile_df(self.idx, self.coreList, includeQC=True)
            else:
                df = self.make_profile_df(self.idx, self.coreList, includeQC=True)
        except Exception as err:
            raise Exception('Profile {0} measurements not created: {1}'.format(self.profileID, err))
        if df.empty:
            raise ValueError('Profile {0} not created: No good measurements'.format(self.profileID))
        if roundDec:
            columns = df.columns
            colNames = [x.lower() for x in self.measList if x.lower() in columns]
            df[colNames] = df[colNames].apply(lambda x: round(x, 3))

        # if df.temp.isnull().values.any():
        #     logging.warning('{} has nan in temp'.format(self.profileID))

        # if df.psal.isnull().values.any():
        #     logging.warning('{} has nan in psal'.format(self.profileID))
        return df

    def make_profile_dict(self, dacName, remotePath):
        """
        Takes a profile measurement and formats it into a dictionary object.
        """
        for string in self.stringValues:
            if string in self.variables.keys():
                self.add_string_values(string)
            else:
                logging.debug('{} not in keys.'.format(string))
        self.profileDoc['DATA_MODE'] = self.dataMode
        # sometimes INST_REFERENCE is used instead of PLATFORM_TYPE
        self.add_platform_type()
        self.isDeep = self.check_if_deep_profile()
        profileDf = self.create_measurements_df(roundDec=True)

        self.profileDoc['measurements'] = profileDf.astype(np.float64).to_dict(orient='records')
        self.profileDoc['station_parameters'] = profileDf.columns.tolist()

        maxMinPresArray = [['temp', True] , ['temp', False], ['psal', True], ['psal', False]]
        for paramBool in maxMinPresArray:
            self.add_max_min_pres(profileDf, param=paramBool[0], maxBoolean=paramBool[1])

        maxPres = profileDf.pres.max()
        self.profileDoc['max_pres'] = np.float64(maxPres)

        self.add_date()
        self.add_date_qc()
        self.add_lat_lon()
        self.add_position_qc()

        self.profileDoc['cycle_number'] = self.cycleNumber
        self.profileDoc['dac'] = dacName
        self.profileDoc['platform_number'] = self.platformNumber
        
        stationParametersInNc = self.stationParameters
        self.profileDoc['station_parameters_in_nc'] = stationParametersInNc
        url = remotePath
        self.profileDoc['nc_url'] = url

        if 'PARAMETER_DATA_MODE' in self.variables.keys():
           self.add_param_data_mode()

        if any (k in self.bgcList for k in stationParametersInNc):
            self.add_BGC()

        """
        Normally, the floats take measurements on the ascent. 
        In the event that the float takes measurements on the descent, the
        cycle number doesn't change. So, to have a unique identifer, this 
        the _id field has a 'D' appended
        """
        direction = self.decode_param('DIRECTION')
        if not isinstance(direction, str):
            logging.debug('direction unknown')
        else:
            if direction == 'D':
                self.profileID += 'D'
            self.profileDoc['DIRECTION'] = direction
            self.profileDoc['_id'] = self.profileID
