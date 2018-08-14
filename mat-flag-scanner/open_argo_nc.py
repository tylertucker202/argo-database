#! /usr/bin/python3.4
##! /Users/dgiglio/anaconda/envs/py34/bin/python3.4
#/Users/dgiglio/anaconda/bin/python3.4
#/Users/dgiglio/anaconda/envs/py36/bin/python3.6
#/usr/bin/env python
import netCDF4
from numpy import ma as ma
from scipy.io import savemat
import os.path

flag_run = 1

def get_file_names(flag_run):
    if (flag_run==0):
        # path files out
        path_files    = '/Users/giglio/Desktop/ACC_JISAO/code/giglio/Argovis_website2017/python/'#'/Users/giglio/Desktop/DATA/Argo_profiles_raw/test_argovis/' #'
        path_flags    = path_files
        # filelist in input
        filename_list = path_files + 'filename_test.txt'
        path_argo_mirror = ''
    else:
        # path files out
        path_files = '/druk/argovis/data/argovis_mat_python/'
        path_flags = '/druk/argovis/data/'
        # filelist in input
        filename_list = '/druk/argovis/data/prof_fname_mirror.txt'
        path_argo_mirror = '/drua/argo/mirror/'    
    # read list of files
    with open(filename_list) as f:
        file_names = f.readlines()

    return file_names, path_argo_mirror, path_flags, filename_list

def open_Argo_ncfile(filename, path_files):

    #print(filename)
    #print(os.path.isfile(filename.strip()))

    N_PROF_select = 0

    #filename = '/Users/giglio/Desktop/DATA/Argo_profiles_raw/test_argovis/R1901396_259.nc' #R1900167_001.nc'#D3900616_127.nc'

    # initialize the dictionary in output
    data = {}

    # initialize flags
    data['flag_bad_pos_time'] = 0
    data['flag_bad_data']     = 0
    data['flag_bad_TEMP']     = 0
    data['flag_bad_PSAL']     = 0
    data['flag_no_file']      = 0

    if os.path.isfile(filename.strip()) is True:
        data['NaN_MongoDB'] = -999
        data['NaN_MongoDB_char'] = '-'
        data['min_PRES_DEEP_ARGO'] = 2000
        data['deep_wmo_inst_type'] = [838, 849, 862, 874, 868]

        nc = netCDF4.Dataset(filename.strip())

        vars_N_PROF = ['PLATFORM_NUMBER', 'PI_NAME', 'STATION_PARAMETERS', 'CYCLE_NUMBER', 'DIRECTION', 'DATA_CENTRE', 'DATA_MODE', 'INST_REFERENCE', 'WMO_INST_TYPE', 'JULD', 'JULD_QC', 'LATITUDE', 'LONGITUDE', 'POSITION_QC', 'POSITIONING_SYSTEM', 'PRES', 'PRES_QC', 'PRES_ADJUSTED', 'PRES_ADJUSTED_QC', 'TEMP', 'TEMP_QC', 'TEMP_ADJUSTED', 'TEMP_ADJUSTED_QC', 'PSAL', 'PSAL_QC', 'PSAL_ADJUSTED', 'PSAL_ADJUSTED_QC', 'VERTICAL_SAMPLING_SCHEME', 'PLATFORM_TYPE']

        vars_N_PROF = [v for v in vars_N_PROF if v in nc.variables]
        for v in vars_N_PROF:
            if 'N_PROF' in nc.variables[v].dimensions:
                prof_idx = nc.variables[v].dimensions.index('N_PROF')
                if prof_idx == 0:
                    tmp = nc.variables[v][N_PROF_select]
                elif prof_idx == 1:
                    tmp = nc.variables[v][:, N_PROF_select]
                elif prof_idx == 2:
                    tmp = nc.variables[v][:, :, N_PROF_select]
                elif prof_idx == 3:
                    tmp = nc.variables[v][:, :, :, N_PROF_select]
            else:
                tmp = nc.variables[v][:]

            tmp = ma.asanyarray(tmp)
            if ma.any(['STRING' in d for d in nc.variables[v].dimensions]):
                #print(b"".join(tmp).decode('utf-8').strip('\x00'))
                #print(b"".join(tmp).decode('utf-8'))
                #print(tmp)
                pass
            #    tmp = ma.array(trim_string(tmp, nc.variables[v].dimensions))

            data[v] = tmp

        # if instrument reference is in dictionary, then we write platform type
        if 'INST_REFERENCE' in data.keys():
            data['PLATFORM_TYPE']=data['INST_REFERENCE']
            del data['INST_REFERENCE']

        # attempt to decode some strings to be saved as strings. We should find a solution for these variables: 'DATA_MODE', 'DIRECTION', 'STATION_PARAMETERS', ... since what below did not work... actually it just did not work for any of the strings...
        strings_to_decode = ['DATA_CENTRE', 'PI_NAME', 'PLATFORM_NUMBER', 'PLATFORM_TYPE', 'POSITIONING_SYSTEM', 'VERTICAL_SAMPLING_SCHEME', 'WMO_INST_TYPE']
        for v in strings_to_decode:
            if v in data.keys():
                tmp = data[v]
                tmp = b"".join(tmp).decode('utf-8').strip('\x00')
                tmp = tmp.replace('\x00',' ',1)
                data[v] = tmp.replace('\x00','')
            else:
                data[v] = data['NaN_MongoDB_char']

        strings_using_str = ['DATA_MODE', 'DIRECTION']

        # write x_ID
        suf = ''
        if data['DIRECTION'] in [b'D']:
            suf = 'D'

        data['x_id'] =  data['PLATFORM_NUMBER']+ '_' + str(data['CYCLE_NUMBER'])+suf
        # problem fixed:
        # data['x_id'] =  b"".join(data['PLATFORM_NUMBER']).decode('utf-8').strip('\x00') + '_' + str(data['CYCLE_NUMBER'])+suf

        # use ADJUSTED vars if available for DATA_MODE A/D, delete them for DATA_MODE R
        varnames = ['PRES', 'TEMP', 'PSAL']
        suf = ''
        if data['DATA_MODE'] in [b'D', b'A']:
            suf = '_ADJUSTED'

            for v in varnames:
                if v in data.keys():
                    myvar = v+suf
                    data[v] = data[myvar]
                    data[v+'_QC'] = data[myvar+'_QC']
                    # remove ADJUSTED items since they are no longer needed and fillValues with NaN value of choice
                    del data[myvar]
                    del data[myvar+'_QC']

        else:
            for v in varnames:
                if v+'_ADJUSTED' in data.keys():
                    #print('TO CHECK')
                    #print(data['DATA_MODE'])
                    # remove ADJUSTED items
                    del data[v+'_ADJUSTED']
                    del data[v+'_ADJUSTED'+'_QC']

        # move masked array to regular array with NaN of choice
        for v in varnames:
            if v in data.keys():
                data[v] = data[v].filled(data['NaN_MongoDB'])
                data[v+'_QC'] = data[v+'_QC'].filled(data['NaN_MongoDB_char'])

        # check if the position/time is OK
        if (data['POSITION_QC']==b'3') | (data['POSITION_QC']==b'4') | (data['JULD_QC']==b'3') | (data['JULD_QC']==b'4'):
            data['flag_bad_pos_time'] = 1

        # check if the position/time QC is all masked
        if (data['POSITION_QC'].count()==0) | (data['JULD_QC'].count()==0):
            data['flag_bad_pos_time'] = 2

        # let's continue only if the position/time info is good
        if (data['flag_bad_pos_time']==0):
            # let's create a main mask: if data are all bad, just assign a flag to indicate this instance of bad data
            mask_main = (data['PRES_QC']==b'1') & (data['TEMP_QC']==b'1')

            # if there is no good data to save, no need to continue, just activate the relevant flag
            if (sum(mask_main==True)!=0):
                # let's apply the main mask to all the relevant variables (will have to do something different for deep Argo)
                for v in varnames:
                    if v in data.keys():
                        data[v+'_inMongoDB'] = data[v][mask_main]
                        data[v+'_inMongoDB'][data[v+'_QC'][mask_main]!=b'1']=data['NaN_MongoDB']
                        if int(data['WMO_INST_TYPE']) in data['deep_wmo_inst_type']:
                            #  Here go the deep ones: for now let's just save the QC flag... we should eventually change also the criteria...
                            # note that we decode the data using astype('U13'): we should just do that for all other vars that are encoded...
                            # rather than keeping instances like b'3'
                            data[v+'_inMongoDB_QC'] = data[v+'_QC'][mask_main].astype('U13')

                # write PRES in STATION_PARAMETERS_inMongoDB (this has to be there for the profile to be considered)
                data['STATION_PARAMETERS_inMongoDB'] = ['PRES']

                # let's save min/max pressure available for TEMP/PSAL
                varnamesTS = ['TEMP', 'PSAL']
                for v in varnamesTS:

                    if v in data.keys():
                        if data['PRES_inMongoDB'][data[v+'_inMongoDB']!=data['NaN_MongoDB']].size!=0:
                            data['PRES_max_for_'+v] = max(data['PRES_inMongoDB'][data[v+'_inMongoDB']!=data['NaN_MongoDB']])
                            data['PRES_min_for_'+v] = min(data['PRES_inMongoDB'][data[v+'_inMongoDB']!=data['NaN_MongoDB']])
                            data['STATION_PARAMETERS_inMongoDB'].append(v)

                        else:
                            # if there is no good data for that variable, delete the item in the dictionary
                            data['flag_bad_'+v]     = 1
                            if v=='TEMP':
                                print('%%%%%')
                                print('The code should not be here: '+ str(sum(mask_main==True)))
                                print(data[v][mask_main])
                                print(data[v+'_inMongoDB'])
                                print('%%%%%')
                            del data[v+'_inMongoDB']

                # write the basin mask
                data['BASIN'] = data['NaN_MongoDB']
            else:
                # if there is no good data to save, activate the relevant flag
                data['flag_bad_data'] = 1

        #print(data.keys())

        # we don't want to return masked arrays for any of the variables (if the value is masked, we assign NaN_MongoDB)
        vars_to_fix_num = ['LONGITUDE', 'LATITUDE','JULD','CYCLE_NUMBER']

        for v in vars_to_fix_num:
            if (data[v].count()!=0):
                data[v] = data[v].filled(data['NaN_MongoDB'])
            else:
                data[v] = data['NaN_MongoDB']
                #del data[v]
            #print(data[v].size)

        vars_to_fix = ['POSITION_QC','JULD_QC','DATA_MODE','DIRECTION','STATION_PARAMETERS']
        # this should be done different
        for v in vars_to_fix:
            if v in data.keys():
                if (data[v].count()!=0):
                    data[v] = str(data[v].filled(data['NaN_MongoDB_char']))
                    data[v] = data[v].replace("b'", "")
                    data[v] = data[v].replace("'", "")
                    data[v] = data[v].replace("-", "")
                else:
                    data[v] = data['NaN_MongoDB_char']
                    #del data[v]

        # remove the items of the dictionary that you don't want to return
        vars_to_remove = ['PRES', 'PRES_QC', 'PSAL', 'PSAL_QC', 'TEMP', 'TEMP_QC']#, 'DATA_MODE', 'DIRECTION', 'STATION_PARAMETERS'
        for v in vars_to_remove:
            if v in data.keys():
                del data[v]

        # save to file
        #print(data['x_id'])
        #print(data['LONGITUDE'].size)
        #print(data['LONGITUDE'])
        #print(data['LONGITUDE'].filled(data['NaN_MongoDB']))
        #print(data)
        #print(data.keys())
        # check that none of the data are masked arrays
        # for v in data.keys():
        #     if isinstance(data[v],ma.MaskedArray):
        #         print(v)
        #         print(data[v])
        #         ciao

        #savemat(os.path.join(path_files,data['x_id']+'.mat'), data, oned_as='row')

    else:
        data['flag_no_file']      = 1
        #print('No file, flag is ' + str(data['flag_no_file']))
    return data

if __name__ == '__main__':
    file_names, path_argo_mirror, path_flags, filename_list = get_file_names(flag_run)
    # loop through each file and use the function to store info
    vars = ['flag_bad_pos_time','flag_bad_data','flag_bad_TEMP','flag_bad_PSAL','flag_no_file',]
    
    # allocate empty dictionary
    flags = {}
    for v in vars:
        flags[v] = []
    
    nfn = 0
    for fn in file_names:
        nfn = nfn + 1
        #print(os.path.isfile(fn.strip()))
        print(fn)
        d = open_Argo_ncfile(path_argo_mirror+fn, path_files)
        for v in vars:
           flags[v].append(d[v])
           if (d[v]!=0):
               print(v+' = '+str(d[v]))
        del d
        if (ma.remainder(nfn,100000)==0):
            print(str(nfn/len(file_names)*100) + '% done')
            # save the flags
            savemat(path_flags+'open_Argo_nc_flags.mat', flags, oned_as='row')
    # save at the end
    savemat(path_flags+'open_Argo_nc_flags.mat', flags, oned_as='row')
    # check: in open_Argo_nc_flags.mat we should perfom the sum below
    # sum(flag_bad_pos_time~=0|flag_bad_data~=0|flag_no_file~=0)+sum(flag_bad_pos_time==0&flag_bad_data==0&flag_no_file==0)
    # which should equal the number of lines in the list of filenames
    #
    # TO CHECK: some vars may be deleted before the function returns data (check which ones),
    # some strings should be trimmed (e.g. STATION_PARAMETERS,... and others...), STATION_PARAMETERS* should be written differently to be lists ?
    # check if masked arrays are converted properly to other things, store the flags in a better way ?
    # - check if you want the function to return all the variables that it does now (e.g. deep_wmo_inst_type is not crucial ? just there to keep track in case new deep floats come up)
    #
    # TO IMPLEMENT: flag basin (for now NaN is assigned), deep floats need different processing and QC flag should be included, check if the file already exists?,
    # decode strings properly (e.g. VERTICAL_SAMPLING_SCHEME is not showing properly, see example from Megan; PRES_inMongoDB_QC is also not great)
    #
    # DONE:
    # write a new item in data (STATION_PARAMETERS_inMongoDB)
    # check that none of the items in data are masked arrays (e.g. JULD/LAT/LON/...maybe others? may still be masked arrays and need to be converted)
