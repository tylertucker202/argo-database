import netCDF4
import numpy as np
from scipy.interpolate import griddata
import os.path
import logging
import pdb

class openArgoNcFile(object):
    def __init__(self, basinFilename='basinmask_01.nc'):
        self.N_PROF_select = 0
        self.inside_function = ''
        # initialize the dictionary in output
        self.baseProfileData = {}
        # initialize flags
        self.baseProfileData['flag_bad_pos_time'] = 0
        self.baseProfileData['flag_bad_data']     = 0
        self.baseProfileData['flag_bad_TEMP']     = 0
        self.baseProfileData['flag_bad_PSAL']     = 0
        self.baseProfileData['flag_no_file']      = 0
        self._init_profileData()
        self._init_basin(basinFilename)
        self.variablesInProfile = ['PLATFORM_NUMBER',
                                   'PI_NAME',
                                   'STATION_PARAMETERS',
                                   'CYCLE_NUMBER',
                                   'DIRECTION',
                                   'DATA_CENTRE',
                                   'DATA_MODE',
                                   'INST_REFERENCE',
                                   'WMO_INST_TYPE',
                                   'JULD',
                                   'JULD_QC',
                                   'LATITUDE',
                                   'LONGITUDE',
                                   'POSITION_QC',
                                   'POSITIONING_SYSTEM',
                                   'PRES',
                                   'PRES_QC',
                                   'PRES_ADJUSTED',
                                   'PRES_ADJUSTED_QC',
                                   'TEMP',
                                   'TEMP_QC',
                                   'TEMP_ADJUSTED',
                                   'TEMP_ADJUSTED_QC',
                                   'PSAL',
                                   'PSAL_QC',
                                   'PSAL_ADJUSTED',
                                   'PSAL_ADJUSTED_QC',
                                   'VERTICAL_SAMPLING_SCHEME',
                                   'PLATFORM_TYPE']
        self.strings_to_decode = ['DATA_CENTRE',
                                  'PI_NAME',
                                  'PLATFORM_NUMBER',
                                  'PLATFORM_TYPE',
                                  'POSITIONING_SYSTEM',
                                  'VERTICAL_SAMPLING_SCHEME',
                                  'WMO_INST_TYPE']
        self.measurements = ['PRES', 'TEMP', 'PSAL']
        self.measurementsTS = ['TEMP', 'PSAL']
        self.varsToNumber = ['LONGITUDE', 'LATITUDE','JULD','CYCLE_NUMBER']
        self.varsToFix = ['POSITION_QC','JULD_QC','DATA_MODE','DIRECTION','STATION_PARAMETERS']
        self.varsToRemove = ['PRES', 'PRES_QC', 'PSAL', 'PSAL_QC', 'TEMP', 'TEMP_QC']


        
    def _init_profileData(self):
        return self.baseProfileData
    
    def _init_basin(self, basinFilename):
        basinNc = netCDF4.Dataset(basinFilename)
        idx = np.nonzero(~basinNc.variables['BASIN_TAG'][:].mask)
    	    #assert self.basinNc.variables['LONGITUDE'].mask == True
    	    #assert self.basinNc.variables['LATITUDE'].mask == True
        self.basin = basinNc.variables['BASIN_TAG'][:][idx].astype('i')
        self.coords = np.stack([basinNc.variables['LATITUDE'][idx[0]], basinNc.variables['LONGITUDE'][idx[1]]]).T
        
    def create_profile_data_if_exists(self, filename):
        if os.path.isfile(filename.strip()) is True:
            self._create_profile_data(filename)
        else:
            self.profileData['flag_no_file'] = 1
   
    def get_profile_data(self):
        return self.profileData

    def _create_profile_data(self, filename):
        self.profileData = self._init_profileData()
        self.profileData['NaN_MongoDB'] = -999
        self.profileData['NaN_MongoDB_char'] = '-'
        self.profileData['min_PRES_DEEP_ARGO'] = 2000
        self.profileData['deep_wmo_inst_type'] = [838, 849, 862, 874, 868]
        ncData = netCDF4.Dataset(filename.strip())
        self._add_variables_in_profile(ncData)
        # if instrument reference is in dictionary, then we write platform type
        if 'INST_REFERENCE' in self.profileData.keys():
            self.profileData['PLATFORM_TYPE'] = self.profileData['INST_REFERENCE']
            del self.profileData['INST_REFERENCE']
        # write the basin mask
        lat = float(self.profileData['LATITUDE'])
        lon = float(self.profileData['LONGITUDE'])
        self.profileData['BASIN'] = self.get_basin(lat, lon)
        self._decode_strings()
        self._format_measurements()
        self._check_pos_time()
        self._format_vars_to_numbers()
        self._format_vars_to_fix()
        self._format_vars_to_remove()

    def _add_variables_in_profile(self, ncData):
        varsInProfile = [ncVar for ncVar in self.variablesInProfile if ncVar in ncData.variables]
        for ncVar in varsInProfile:
            if 'N_PROF' in ncData.variables[ncVar].dimensions:
                prof_idx = ncData.variables[ncVar].dimensions.index('N_PROF')
                if prof_idx == 0:
                    tmpNcVariables = ncData.variables[ncVar][self.N_PROF_select]
                elif prof_idx == 1:
                    tmpNcVariables = ncData.variables[ncVar][:, self.N_PROF_select]
                elif prof_idx == 2:
                    tmpNcVariables = ncData.variables[ncVar][:, :, self.N_PROF_select]
                elif prof_idx == 3:
                    tmpNcVariables = ncData.variables[ncVar][:, :, :, self.N_PROF_select]
            else:
                tmpNcVariables = ncData.variables[ncVar][:]

            tmpNcVariables = np.ma.asanyarray(tmpNcVariables)
            if np.ma.any(['STRING' in d for d in ncData.variables[ncVar].dimensions]):
                pass
            self.profileData[ncVar] = tmpNcVariables

    def _decode_strings(self):
        # attempt to decode some strings to be saved as strings.
        for ncVar in self.strings_to_decode:
            if ncVar in self.profileData.keys():
                tmpNcVariables = self.profileData[ncVar]
                if type(tmpNcVariables) == np.ma.core.MaskedArray:
                    tmpNcVariables = b"".join(tmpNcVariables).decode('utf-8').strip('\x00')
                    tmpNcVariables = tmpNcVariables.replace('\x00',' ',1)
                    self.profileData[ncVar] = tmpNcVariables.replace('\x00','')
                elif type(tmpNcVariables) == str:
                    self.profileData[ncVar] = tmpNcVariables
                else:
                    logging.warning('type error for: {1}. setting to \'-\''.format(ncVar))
                    self.profileData[ncVar] = self.profileData['NaN_MongoDB_char']
            else:
                self.profileData[ncVar] = self.profileData['NaN_MongoDB_char']

    def _format_measurements(self):
        # write x_ID
        suffix = ''
        if self.profileData['DIRECTION'] in [b'D']:
            suffix = 'D'

        self.profileData['x_id'] = self.profileData['PLATFORM_NUMBER'] + '_' + str(self.profileData['CYCLE_NUMBER']) + suffix
        # use ADJUSTED vars if available for DATA_MODE A/D, delete them for DATA_MODE R
        suffix = ''
        if self.profileData['DATA_MODE'] in [b'D', b'A']:
            suffix = '_ADJUSTED'

            for meas in self.measurements:
                if meas in self.profileData.keys():
                    myvar = meas + suffix
                    self.profileData[meas] = self.profileData[myvar]
                    self.profileData[meas+'_QC'] = self.profileData[myvar+'_QC']
                    '''
                    remove ADJUSTED items since they are no longer 
                    needed and fillValues with NaN value of choice
                    '''
                    del self.profileData[myvar]
                    del self.profileData[myvar+'_QC']

        else:
            for meas in self.measurements:
                if meas+'_ADJUSTED' in self.profileData.keys():
                    # remove ADJUSTED items
                    del self.profileData[meas+'_ADJUSTED']
                    del self.profileData[meas+'_ADJUSTED'+'_QC']
        # move masked array to regular array with NaN of choice
        for meas in self.measurements:
            if meas in self.profileData.keys():
                self.profileData[meas] = self.profileData[meas].filled(self.profileData['NaN_MongoDB'])
                self.profileData[meas+'_QC'] = self.profileData[meas+'_QC'].filled(self.profileData['NaN_MongoDB_char'])

    def _check_pos_time(self):
        # check if the position/time is OK
        if (self.profileData['POSITION_QC']==b'3') | (self.profileData['POSITION_QC']==b'4') | (self.profileData['JULD_QC']==b'3') | (self.profileData['JULD_QC']==b'4'):
            self.profileData['flag_bad_pos_time'] = 1

        # check if the position/time QC is all masked
        if (self.profileData['POSITION_QC'].count()==0) | (self.profileData['JULD_QC'].count()==0):
            self.profileData['flag_bad_pos_time'] = 2
        # let's continue only if the position/time info is good
        if not (self.profileData['flag_bad_pos_time']==0):
            return
    
        # let's create a main mask: if data are all bad, just assign a flag to indicate this instance of bad data
        mask_main = (self.profileData['PRES_QC']==b'1') & (self.profileData['TEMP_QC']==b'1')
        # if there is no good data to save, no need to continue, just activate the relevant flag
        if (sum(mask_main==True)!=0):
            # let's apply the main mask to all the relevant variables (will have to do something different for deep Argo)
            for meas in self.measurements:
                if meas in self.profileData.keys():
                    self.profileData[meas+'_inMongoDB'] = self.profileData[meas][mask_main]
                    self.profileData[meas+'_inMongoDB'][self.profileData[meas+'_QC'][mask_main]!=b'1']=self.profileData['NaN_MongoDB']
                    if int(self.profileData['WMO_INST_TYPE']) in self.profileData['deep_wmo_inst_type']:
                        #  Here go the deep ones: for now let's just save the QC flag... we should eventually change also the criteria...
                        # note that we decode the data using astype('U13'): we should just do that for all other vars that are encoded...
                        # rather than keeping instances like b'3'
                        self.profileData[meas+'_inMongoDB_QC'] = self.profileData[meas+'_QC'][mask_main].astype('U13')

            # write PRES in STATION_PARAMETERS_inMongoDB (this has to be there for the profile to be considered)
            self.profileData['STATION_PARAMETERS_inMongoDB'] = ['PRES']

            # let's save min/max pressure available for TEMP/PSAL
            for meas in self.measurementsTS:

                if meas in self.profileData.keys():
                    if self.profileData['PRES_inMongoDB'][self.profileData[meas+'_inMongoDB'] != self.profileData['NaN_MongoDB']].size != 0:
                        self.profileData['PRES_max_for_'+meas] = max(self.profileData['PRES_inMongoDB'][self.profileData[meas+'_inMongoDB']!=self.profileData['NaN_MongoDB']])
                        self.profileData['PRES_min_for_'+meas] = min(self.profileData['PRES_inMongoDB'][self.profileData[meas+'_inMongoDB']!=self.profileData['NaN_MongoDB']])
                        self.profileData['STATION_PARAMETERS_inMongoDB'].append(meas)

                    else:
                        # if there is no good data for that variable, delete the item in the dictionary
                        self.profileData['flag_bad_'+meas]     = 1
                        if meas=='TEMP':
                            print('%%%%%')
                            print('The code should not be here: '+ str(sum(mask_main==True)))
                            print(self.profileData[meas][mask_main])
                            print(self.profileData[meas+'_inMongoDB'])
                            print('%%%%%')
                        del self.profileData[meas+'_inMongoDB']

        else:
            # if there is no good data to save, activate the relevant flag
            self.profileData['flag_bad_data'] = 1

    def _format_vars_to_numbers(self):
        # we don't want to return masked arrays for any of the variables (if the value is masked, we assign NaN_MongoDB)
        for varToNum in self.varsToNumber:
            if (self.profileData[varToNum].count()!=0):
                self.profileData[varToNum] = self.profileData[varToNum].filled(self.profileData['NaN_MongoDB'])
            else:
                self.profileData[varToNum] = self.profileData['NaN_MongoDB']

    def _format_vars_to_fix(self):
        # this should be done different
        for varTF in self.varsToFix:
            if varTF in self.profileData.keys():
                if (self.profileData[varTF].count()!=0):
                    self.profileData[varTF] = str(self.profileData[varTF].filled(self.profileData['NaN_MongoDB_char']))
                    self.profileData[varTF] = self.profileData[varTF].replace("b'", "")
                    self.profileData[varTF] = self.profileData[varTF].replace("'", "")
                    self.profileData[varTF] = self.profileData[varTF].replace("-", "")
                else:
                    self.profileData[varTF] = self.profileData['NaN_MongoDB_char']

    def _format_vars_to_remove(self):
        # remove the items of the dictionary that you don't want to return
        for varTR in self.varsToRemove:
            if varTR in self.profileData.keys():
                del self.profileData[varTR]

    def get_basin(self, lat, lon):
        """Returns the basin code for a given lat lon coordinates
        	Ex.:
        basin = get_basin(15, -38, '/path/to/basinmask_01.nc')
        	"""
        basin = int(griddata(self.coords, self.basin, (lon, lat), method='nearest'))
        return basin

