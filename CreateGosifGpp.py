import xarray as xr
import numpy as np
import rioxarray
import zarr
import os
import glob
import datetime
import warnings

def main():
    
    print("Converting GOSIF GPP v2 consolidated data to Zarr format...")
    
    warnings.filterwarnings("ignore", category=UserWarning)
    
    from dask.distributed import Client
    client = Client(n_workers=8, threads_per_worker=2, memory_limit='4GB')
    # link to dashboard
    print("Dashboard available under: " + str(client.dashboard_link))

    # Set the directory where the data is stored
    tiff_dir = "GOSIF-GPP_v2"
    zarr_dir = "GOSIF-GPP_v2_2000_2023_1x3600x7200.zarr.zarr"
    fill_value_old_1 = 65535 # fill value in the original data from README
    fill_value_old_2 = 65534 # fill value in the original data from README
    fill_value_new = np.nan

    def CubeFile(file):
        # extract date from filename
        date = os.path.basename(file).split("_")[-2]
        # date = date.split(".")[0]
        year = date[0:4]
        dayofyear = date[5:7]
        dt = datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(dayofyear) - 1)
        cube = xr.open_dataarray(file, engine="rasterio")
        cube = cube.where(cube != fill_value_old_1, fill_value_new)
        cube = cube.where(cube != fill_value_old_2, fill_value_new)
        cube = cube.assign_coords({"time":dt})
        cube = cube.expand_dims("time")
        return cube
    
    print("Reading TIFF files...")

    # Create the new dataset
    files = glob.glob(tiff_dir + "/*.tif")
    cube = xr.concat([CubeFile(file) for file in files], dim="time")
    cube = cube.rename({"x":"lon", "y":"lat"})
    ds = cube.to_dataset(name="GPP")
    ds = ds.drop_vars("band")

    # set chunking
    ds["GPP"] = ds["GPP"].chunk({"time":1, "lat":3600, "lon":7200})

    gpp_attrs = {
        "long_name":"Gross Primary Production (GPP) from GOSIF",
        "Unit":"gC m-2 d-1",
        "_FillValue":fill_value_new,
        "Scale_Factor":0.001,
    }
    ds["GPP"].attrs = gpp_attrs
    ds.attrs = {
        "title":"GOSIF GPP v2",
        "history":"converted to zarr by Martin Reinhardt, RSC4Earth, University of Leipzig",
        "source":"https://climatedataguide.ucar.edu/climate-data/global-dataset-solar-induced-chlorophyll-fluorescence-gosif",
    }

    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)
    encoding = {vname: {
        'compressor': compressor,
        } for vname in ds.data_vars}
    
    print("Writing Zarr files...")

    ds.to_zarr(zarr_dir, mode="w", consolidated=True, compute=True, encoding=encoding)

    Client.close(client)

if __name__ == '__main__':
    main()