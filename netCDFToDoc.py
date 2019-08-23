"""
Created on Sun Feb  4 15:46:14 2018
@author: tyler
"""
import logging
import numpy as np
from datetime import datetime, timedelta
from measToDf import measToDf
import warnings
import pdb

warnings.simplefilter('error', RuntimeWarning)
np.warnings.filterwarnings('ignore')

class netCDFToDoc(measToDf):

    def __init__(self, variables,
                 dacName,
                 remotePath,
                 stationParameters,
                 platformNumber,
                 nProf):
        logging.debug('initializing netCDFToDoc')
        measToDf.__init__(self, variables, stationParameters, nProf)
        self.platformNumber = platformNumber
        self.cycleNumber = int(self.variables['CYCLE_NUMBER'][self.idx].astype(str))
        self.profileId = self.platformNumber + '_' + str(self.cycleNumber)
        self.profileDoc = dict()
        self.deepFloatWMO = ['838' ,'849','862','874','864']  # Deep floats don't have QC
        # populate profileDoc
        self.make_profile_dict(dacName, remotePath)
    
    def get_profile_doc(self):
        return self.profileDoc

    def add_string_values(self, valueName):
        """
        Used to add POSITIONING_SYSTEM PLATFORM_TYPE DATA_MODE and PI_NAME fields.
        if missing or is masked, values will not be added to the document.
        """
        try:
            if isinstance(self.variables[valueName][self.idx], np.ma.core.MaskedConstant):
                value = self.variables[valueName][self.idx].astype(str).item()
                logging.debug('Profile:{0} has unknown {1}.'
                          ' Not going to add item to document'.format(self.profileId, valueName))
            else:
                if valueName == 'DATA_MODE':
                    value = self.variables[valueName][self.idx].astype(str).item()
                else:
                    value = ''.join([(x.astype(str).item()) for x in self.variables[valueName][self.idx].data])
                    value = value.strip(' ')

            if valueName == 'INST_REFERENCE': # renames 'INST_REFERENCE' to 'PLATFORM_TYPE'
                self.profileDoc['PLATFORM_TYPE'] = value
            else:
                self.profileDoc[valueName] = value
        except KeyError:
            if valueName == 'PLATFORM_TYPE':
                instRefExists = 'INST_REFERENCE' in self.variables.keys()
                logging.debug('PLATFORM_TYPE not found.'
                              'INST_REFERENCE exists? {}'.format(instRefExists))
                raise KeyError  
            logging.debug('unknown key {0}.'
                          ' Not going to add item to document'.format(valueName))
        except:
            logging.debug('error when adding {0} to document.'
                          ' Not going to add item to document'.format(valueName))

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
            logging.warning('Profile {}: unable to get presmax/min, unknown exception.'.format(self.profileId))

    def add_date(self):
        refDateArray = self.variables['REFERENCE_DATE_TIME'][:]
        refStr = ''.join([x.astype(str) for x in refDateArray])
        refDate = datetime.strptime(refStr, '%Y%m%d%H%M%S')
        if isinstance(self.variables['JULD'][self.idx], np.ma.core.MaskedConstant):
            logging.warning('Profile:{0} has unknown date. filling with refDate {1}'.format(self.profileId, refDate))
            self.profileDoc['date'] = refDate
        else:
            date = refDate + timedelta(self.variables['JULD'][self.idx].item())
            self.profileDoc['date'] = date
        self.profileDoc['date_added'] = datetime.today()
        try:
            dateQC = self.variables['JULD_QC'][self.idx].astype(np.float64).item()
            self.profileDoc['date_qc'] = dateQC
        except AttributeError:
            if isinstance(self.variables['JULD_QC'][self.idx], np.ma.core.MaskedConstant):
                dateQC = np.float64(self.variables['JULD_QC'][self.idx].item())
                self.profileDoc['date_qc'] = dateQC
            else:
                logging.warning('error with date_qc. filling with -999.')
                self.profileDoc['date_qc'] = -999

    def add_position_qc(self):
        try:
            positionQC = self.variables['POSITION_QC'][self.idx].astype(np.float64).item()
        except AttributeError as err:
            if type(self.variables['POSITION_QC'][self.idx] == np.ma.core.MaskedConstant):
                positionQC = self.variables['POSITION_QC'][self.idx].data.astype(np.float64).item()
            else:
                positionQC = -999
                logging.warning('Profile:{0} positionQc attribute error {1}. Filling with -999'.format(self.profileId, err))
        except ValueError as err:
            positionQC = -999
            logging.warning('Profile:{0} positionQc value error {1}. Filling with -999'.format(self.profileId, err))
        except Exception as err:
            positionQC = -999
            logging.warning('Profile:{0} positionQc exception {1}. Filling with -999'.format(self.profileId, err))
        if positionQC == 4:
            raise ValueError('position_qc is a 4. Not going to add.')
        self.profileDoc['position_qc'] = positionQC

    def add_lat_lon(self):
        lat = self.variables['LATITUDE'][self.idx].item()
        lon = self.variables['LONGITUDE'][self.idx].item()
        if isinstance(lat, np.ma.core.MaskedConstant) or isinstance(lon, np.ma.core.MaskedConstant):
            lat, lon = -89.0, 0.0
            logging.warning('Profile:{0} has unknown lat-lon.'
                          ' Filling with 0, 0'.format(self.profileId))
        self.profileDoc['lat'] = lat
        self.profileDoc['lon'] = lon
        self.profileDoc['geoLocation'] = {'type': 'Point', 'coordinates': [lon, lat]}

    def check_if_deep_profile(self):
        try:
            df = self.format_measurments('PRES', 0)
            df = self.do_qc_on_deep_meas(df, 'pres')
            maxPres = df.pres.max()
            if maxPres >= 2500:
                deepFloat = True
            else:
                deepFloat = False
        except Exception:
            deepFloat = False
        return deepFloat

    def add_BGC(self):
        try:
            self.profileDoc['bgcMeas'] = self.create_BGC()
        except ValueError as err:
            raise ValueError('Profile {0} bgc not created:{1}'.format(self.profileId, err))
        except KeyError as err:
            raise ValueError('Profile {0} bgc not created:{1}'.format(self.profileId, err))
        except UnboundLocalError as err:
            raise UnboundLocalError('Profile {0} bgc not created:{1}'.format(self.profileId, err))
        except AttributeError as err:
            raise AttributeError('Profile {0} bgc:{1}'.format(self.profileId, err))
        except Exception as err:
            raise UnboundLocalError('Profile {0} bgc have unknown error {1}'.format(self.profileId, err))
        bgcMeasKeys = self.profileDoc['bgcMeas'][0].keys()
        #  Strip numbers
        bgcMeasKeys = [''.join(i for i in s if not i.isdigit()) for s in bgcMeasKeys]
        bgcKeys = [s.lower() for s in self.bgcList]
        if bool(set(bgcMeasKeys) & set(bgcKeys)):
            self.profileDoc['containsBGC'] = True
        else:
            del self.profileDoc['bgcMeas']
            logging.warning('Profile: {} contains poor quality bgc data. not going to include table'.format(self.profileId))

    def create_measurements_df(self):
        try:
            if self.deepFloat:
                #  self.profile_id = self.profile_id.strip('D') # D postfix should be used for direction only.
                self.profileDoc['isDeep'] = self.deepFloat
                df = self.make_deep_profile_df(self.idx, self.coreList, includeQC=True)
            else:
                df = self.make_profile_df(self.idx, self.coreList, includeQC=True)
        except ValueError as err:
            raise ValueError('Profile {0} measurements not created: {1}'.format(self.profileId, err))
        except KeyError as err:
            raise KeyError('Profile {0} measurements not created: {1}'.format(self.profileId, err))
        except UnboundLocalError as err:
            raise UnboundLocalError('Profile {0} measurements not created: {1}'.format(self.profileId, err))
        except AttributeError as err:
            raise AttributeError('Profile {0} measurements not created: {1}'.format(self.profileId, err))
        except Exception as err:
            raise UnboundLocalError('Profile {0} measurements not created: {1}'.format(self.profileId, err))
        if df.empty:
            raise ValueError('Profile {0} not created: No good measurements'.format(self.profileId))
        return df

    def make_profile_dict(self, dacName, remotePath):
        """
        Takes a profile measurement and formats it into a dictionary object.
        """
        stringValues = ['POSITIONING_SYSTEM', 'DATA_MODE', 'DATA_CENTRE', 'PI_NAME', 'WMO_INST_TYPE', 'VERTICAL_SAMPLING_SCHEME']
        for string in stringValues:
            self.add_string_values(string)
        # sometimes INST_REFERENCE is used instead of PLATFORM_TYPE
        try:
            self.add_string_values('PLATFORM_TYPE')
        except KeyError:
            self.add_string_values('INST_REFERENCE')

        self.deepFloat = self.check_if_deep_profile()
        profileDf = self.create_measurements_df()
        self.profileDoc['measurements'] = profileDf.astype(np.float64).to_dict(orient='records')
        self.profileDoc['station_parameters'] = profileDf.columns.tolist()

        maxMinPresArray = [['temp', True] , ['temp', False], ['psal', True], ['psal', False]]
        for paramBool in maxMinPresArray:
            self.add_max_min_pres(profileDf, param=paramBool[0], maxBoolean=paramBool[1])

        maxPres = profileDf.pres.max()
        self.profileDoc['max_pres'] = np.float64(maxPres)

        self.add_date()
        self.add_lat_lon()
        self.add_position_qc()

        self.profileDoc['cycle_number'] = self.cycleNumber
        self.profileDoc['dac'] = dacName
        self.profileDoc['platform_number'] = self.platformNumber
        
        stationParametersInNc = [item for sublist in self.stationParameters for item in sublist]
        self.profileDoc['station_parameters_in_nc'] = stationParametersInNc
        url = remotePath
        self.profileDoc['nc_url'] = url
        if any (k in self.bgcList for k in stationParametersInNc):
            self.add_BGC()

        """
        Normally, the floats take measurements on the ascent. 
        In the event that the float takes measurements on the descent, the
        cycle number doesn't change. So, to have a unique identifer, this 
        the _id field has a 'D' appended
        """
        if isinstance(self.variables['DIRECTION'][self.idx], np.ma.core.MaskedConstant):
            logging.debug('direction unknown')
        else:
            direction = self.variables['DIRECTION'][self.idx].astype(str).item()
            if direction == 'D':
                self.profileId += 'D'
            self.profileDoc['DIRECTION'] = direction
            self.profileDoc['_id'] = self.profileId
