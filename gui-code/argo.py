import numpy as np
import xarray as xr


def only_best_data(ds):
    """Use ADJUSTED if D/A otherwise delete ADJUSTED

       The outcome is that the basic variables (PRES, TEMP, PSAL) are the best
         option available.
    """
    assert 'N_PROF' not in ds.dims

    varnames = [v for v in ds.variables if 'ADJUSTED' in v]
    if ds.DATA_MODE in [b'D', 'D', b'A', 'A']:
        for v in varnames:
            ds[v.replace('_ADJUSTED', '')] = ds[v]
    ds = ds.drop(varnames)
    return ds


def convert_byte_to_string(ds, inplace=False):
    """
    """
    if not inplace:
        ds = ds.copy(deep=True)

    for v in [v for v in ds.variables if ds[v].dtype.kind in ('O', 'S')]:
        if (ds[v].shape == ()) and (ds[v].values.dtype.kind == 'S'):
            ds[v].data = ds[v].data.tostring().decode('utf-8').strip()
        else:
            tmp = []
            for x in ds[v].values.flatten(order='C'):
                if type(x) == bytes:
                    x = x.decode('utf-8').strip()
                if x == '':
                    x = ' '
                tmp.append(x)
            ds[v].values = np.array(tmp, dtype='O').reshape(ds[v].shape,
                                                            order='C')

    if not inplace:
        return ds


def get_profiles_from_nc(filename):
    """
    """

    ds = xr.open_dataset(filename, decode_times=False, decode_cf=True)
    
    # There is a weak point here. In case of any invalid value, integer
    #   does not accept NaN
    for v in ds.variables:
        if (ds[v].dtype.kind == 'f') and (ds[v].encoding['dtype'].kind == 'i'):
            ds[v] = ds[v].astype(ds[v].encoding['dtype'].kind)

    if ('PLATFORM_TYPE' not in ds) and ('INST_REFERENCE' in ds):
        ds['PLATFORM_TYPE'] = ds['INST_REFERENCE']

    data = []
    for pn, p in ds.groupby('N_PROF'):
        p = only_best_data(p)
        convert_byte_to_string(p, inplace=True)
        data.append(p)

    return data


