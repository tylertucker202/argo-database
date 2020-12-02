import xarray as xr
import os
from datetime import datetime
import json


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def collect_ancilary_vars(ds: xr.Dataset):

    ancillary_variables = []

    # Example of a variable with ancillary_variables
    # float ctd_salinity(N_PROF=11, N_LEVELS=12);
    #   :_FillValue = NaNf; // float
    #   :whp_name = "CTDSAL";
    #   :whp_unit = "PSS-78";
    #   :standard_name = "sea_water_practical_salinity";
    #   :units = "1";
    #   :reference_scale = "PSS-78";
    #   :ancillary_variables = "ctd_salinity_qc";
    #   :coordinates = "sample expocode station longitude pressure time latitude cast";
    #   :_ChunkSizes = 11U, 12U; // uint

    # For filter_by_attrs, Can pass in key=value or key=callable
    for var in ds.filter_by_attrs(ancillary_variables=lambda x: x is not None):
        ancillary_variables.extend(ds[var].attrs["ancillary_variables"].split())
    
    return set(ancillary_variables)


def create_json(nc, json_dir):

    ancillary_variables = collect_ancilary_vars(nc)

    for var in nc:
        if var in ancillary_variables:
            continue

        # Save each variable profile, except ancillary, as a json file
        for _, group in nc[var].groupby("N_PROF", restore_coord_dims=False):

            # For each N_PROF value, get a group (a profile) at coords lat, lon, time

            lat = float(group.latitude)
            lon = float(group.longitude)
            time = group.time.values.astype("datetime64[m]").astype(datetime)
            
            fname = f"{lat}_{lon}_{time:%Y%m%dT%H%M}_{var}.json"

            os.makedirs(f"{json_dir}/{var}", exist_ok=True)
            
            path = os.path.join(json_dir, var, fname)

            # datetime is not JSON serializable so use dtjson to convert
            with open(path, "w") as f:
                json.dump(group.to_dict(), f, default=dtjson)


def main():

    netcdf_data_directory = './data/netcdf'
    json_data_directory = './data/json'

    for root, dirs, files in os.walk(netcdf_data_directory):

        for file in files:

            if not file.endswith('.nc'):
                continue

            fin = os.path.join(root, file)

            nc = xr.load_dataset(fin)
            
            create_json(nc, json_data_directory)


if __name__ == '__main__':
    main()

