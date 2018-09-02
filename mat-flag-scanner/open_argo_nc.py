import netCDF4
from numpy import ma as ma
import os.path
        
def open_Argo_ncfile(filename, path_files):

    N_PROF_select = 0

    # initialize the dictionary in output
    profileData = {}

    # initialize flags
    profileData['flag_bad_pos_time'] = 0
    profileData['flag_bad_data']     = 0
    profileData['flag_bad_TEMP']     = 0
    profileData['flag_bad_PSAL']     = 0
    profileData['flag_no_file']      = 0

    if os.path.isfile(filename.strip()) is True:
        profileData['NaN_MongoDB'] = -999
        profileData['NaN_MongoDB_char'] = '-'
        profileData['min_PRES_DEEP_ARGO'] = 2000
        profileData['deep_wmo_inst_type'] = [838, 849, 862, 874, 868]

        ncData = netCDF4.Dataset(filename.strip())

        variablesInProfile =  ['PLATFORM_NUMBER',
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

        variablesInProfile = [ncVar for ncVar in variablesInProfile if ncVar in ncData.variables]
        for ncVar in variablesInProfile:
            if 'N_PROF' in ncData.variables[ncVar].dimensions:
                prof_idx = ncData.variables[ncVar].dimensions.index('N_PROF')
                if prof_idx == 0:
                    tmpNcVariables = ncData.variables[ncVar][N_PROF_select]
                elif prof_idx == 1:
                    tmpNcVariables = ncData.variables[ncVar][:, N_PROF_select]
                elif prof_idx == 2:
                    tmpNcVariables = ncData.variables[ncVar][:, :, N_PROF_select]
                elif prof_idx == 3:
                    tmpNcVariables = ncData.variables[ncVar][:, :, :, N_PROF_select]
            else:
                tmpNcVariables = ncData.variables[ncVar][:]

            tmpNcVariables = ma.asanyarray(tmpNcVariables)
            if ma.any(['STRING' in d for d in ncData.variables[ncVar].dimensions]):
                pass

            profileData[ncVar] = tmpNcVariables

        # if instrument reference is in dictionary, then we write platform type
        if 'INST_REFERENCE' in profileData.keys():
            profileData['PLATFORM_TYPE']=profileData['INST_REFERENCE']
            del profileData['INST_REFERENCE']

        # attempt to decode some strings to be saved as strings.
        # We should find a solution for these variables:
        #'DATA_MODE', 'DIRECTION', 'STATION_PARAMETERS', ... 
        #since what below did not work... 
        #actually it just did not work for any of the strings...
        strings_to_decode = ['DATA_CENTRE',
                             'PI_NAME',
                             'PLATFORM_NUMBER',
                             'PLATFORM_TYPE',
                             'POSITIONING_SYSTEM',
                             'VERTICAL_SAMPLING_SCHEME',
                             'WMO_INST_TYPE']
        for ncVar in strings_to_decode:
            if ncVar in profileData.keys():
                tmpNcVariables = profileData[ncVar]
                tmpNcVariables = b"".join(tmpNcVariables).decode('utf-8').strip('\x00')
                tmpNcVariables = tmpNcVariables.replace('\x00',' ',1)
                profileData[ncVar] = tmpNcVariables.replace('\x00','')
            else:
                profileData[ncVar] = profileData['NaN_MongoDB_char']

        # write x_ID
        suffix = ''
        if profileData['DIRECTION'] in [b'D']:
            suffix = 'D'

        profileData['x_id'] =  profileData['PLATFORM_NUMBER'] + '_' + str(profileData['CYCLE_NUMBER']) + suffix
        # use ADJUSTED vars if available for DATA_MODE A/D, delete them for DATA_MODE R
        measurements = ['PRES', 'TEMP', 'PSAL']
        suffix = ''
        if profileData['DATA_MODE'] in [b'D', b'A']:
            suffix = '_ADJUSTED'

            for meas in measurements:
                if meas in profileData.keys():
                    myvar = meas + suffix
                    profileData[meas] = profileData[myvar]
                    profileData[meas+'_QC'] = profileData[myvar+'_QC']
                    # remove ADJUSTED items since they are no longer needed and fillValues with NaN value of choice
                    del profileData[myvar]
                    del profileData[myvar+'_QC']
        else:
            for meas in measurements:
                if meas+'_ADJUSTED' in profileData.keys():
                    # remove ADJUSTED items
                    del profileData[meas+'_ADJUSTED']
                    del profileData[meas+'_ADJUSTED'+'_QC']

        # move masked array to regular array with NaN of choice
        for meas in measurements:
            if meas in profileData.keys():
                profileData[meas] = profileData[meas].filled(profileData['NaN_MongoDB'])
                profileData[meas+'_QC'] = profileData[meas+'_QC'].filled(profileData['NaN_MongoDB_char'])

        # check if the position/time is OK
        if (profileData['POSITION_QC']==b'3') | (profileData['POSITION_QC']==b'4') | (profileData['JULD_QC']==b'3') | (profileData['JULD_QC']==b'4'):
            profileData['flag_bad_pos_time'] = 1

        # check if the position/time QC is all masked
        if (profileData['POSITION_QC'].count()==0) | (profileData['JULD_QC'].count()==0):
            profileData['flag_bad_pos_time'] = 2

        # let's continue only if the position/time info is good
        if (profileData['flag_bad_pos_time']==0):
            # let's create a main mask: if data are all bad, just assign a flag to indicate this instance of bad data
            mask_main = (profileData['PRES_QC']==b'1') & (profileData['TEMP_QC']==b'1')

            # if there is no good data to save, no need to continue, just activate the relevant flag
            if (sum(mask_main==True)!=0):
                # let's apply the main mask to all the relevant variables (will have to do something different for deep Argo)
                for meas in measurements:
                    if meas in profileData.keys():
                        profileData[meas+'_inMongoDB'] = profileData[meas][mask_main]
                        profileData[meas+'_inMongoDB'][profileData[meas+'_QC'][mask_main]!=b'1']=profileData['NaN_MongoDB']
                        if int(profileData['WMO_INST_TYPE']) in profileData['deep_wmo_inst_type']:
                            #  Here go the deep ones: for now let's just save the QC flag... we should eventually change also the criteria...
                            # note that we decode the data using astype('U13'): we should just do that for all other vars that are encoded...
                            # rather than keeping instances like b'3'
                            profileData[meas+'_inMongoDB_QC'] = profileData[meas+'_QC'][mask_main].astype('U13')

                # write PRES in STATION_PARAMETERS_inMongoDB (this has to be there for the profile to be considered)
                profileData['STATION_PARAMETERS_inMongoDB'] = ['PRES']

                # let's save min/max pressure available for TEMP/PSAL
                measurementsTS = ['TEMP', 'PSAL']
                for meas in measurementsTS:

                    if meas in profileData.keys():
                        if profileData['PRES_inMongoDB'][profileData[meas+'_inMongoDB']!=profileData['NaN_MongoDB']].size!=0:
                            profileData['PRES_max_for_'+meas] = max(profileData['PRES_inMongoDB'][profileData[meas+'_inMongoDB']!=profileData['NaN_MongoDB']])
                            profileData['PRES_min_for_'+meas] = min(profileData['PRES_inMongoDB'][profileData[meas+'_inMongoDB']!=profileData['NaN_MongoDB']])
                            profileData['STATION_PARAMETERS_inMongoDB'].append(meas)

                        else:
                            # if there is no good data for that variable, delete the item in the dictionary
                            profileData['flag_bad_'+meas]     = 1
                            if meas=='TEMP':
                                print('%%%%%')
                                print('The code should not be here: '+ str(sum(mask_main==True)))
                                print(profileData[meas][mask_main])
                                print(profileData[meas+'_inMongoDB'])
                                print('%%%%%')
                            del profileData[meas+'_inMongoDB']

                # write the basin mask
                profileData['BASIN'] = profileData['NaN_MongoDB']
            else:
                # if there is no good data to save, activate the relevant flag
                profileData['flag_bad_data'] = 1

        # we don't want to return masked arrays for any of the variables (if the value is masked, we assign NaN_MongoDB)
        varsToNumber = ['LONGITUDE', 'LATITUDE','JULD','CYCLE_NUMBER']

        for varToNum in varsToNumber:
            if (profileData[varToNum].count()!=0):
                profileData[varToNum] = profileData[varToNum].filled(profileData['NaN_MongoDB'])
            else:
                profileData[varToNum] = profileData['NaN_MongoDB']

        varsToFix = ['POSITION_QC','JULD_QC','DATA_MODE','DIRECTION','STATION_PARAMETERS']
        # this should be done different
        for varTF in varsToFix:
            if varTF in profileData.keys():
                if (profileData[varTF].count()!=0):
                    profileData[varTF] = str(profileData[varTF].filled(profileData['NaN_MongoDB_char']))
                    profileData[varTF] = profileData[varTF].replace("b'", "")
                    profileData[varTF] = profileData[varTF].replace("'", "")
                    profileData[varTF] = profileData[varTF].replace("-", "")
                else:
                    profileData[varTF] = profileData['NaN_MongoDB_char']

        # remove the items of the dictionary that you don't want to return
        varsToRemove = ['PRES', 'PRES_QC', 'PSAL', 'PSAL_QC', 'TEMP', 'TEMP_QC']#, 'DATA_MODE', 'DIRECTION', 'STATION_PARAMETERS'
        for varTR in varsToRemove:
            if varTR in profileData.keys():
                del profileData[varTR]

    else:
        profileData['flag_no_file'] = 1
    return profileData

