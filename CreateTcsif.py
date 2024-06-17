import xarray as xr
import numpy as np
import rioxarray
import zarr
import os
import glob
import datetime
import warnings
from dask.distributed import Client
from dask.diagnostics import ProgressBar

def main():
    
    print("Converting TCSIF to Zarr format...")
    
    warnings.filterwarnings("ignore", category=UserWarning)
    
    client = Client(n_workers=8, threads_per_worker=4, memory_limit='10GB')
    # client = Client(n_workers=2, threads_per_worker=2, memory_limit='8GB')
    # link to dashboard
    print("Dashboard available under: " + str(client.dashboard_link))

    # Set the directory where the data is stored
    tiff_dir = "TCSIF_level3"
    zarr_dir = "TCSIF_level3_2007_2021_1x360x720.zarr"
    fill_value_new = np.nan

    def CubeFile(file):
        # extract date from filename
        date = os.path.basename(file).split("_")[-1]
        date = date.split(".")[0]
        year = date[0:4]
        month = date[4:6]
        day = 15
        cube = xr.open_dataarray(file,
                                    engine="rasterio",
                                    chunks={}
        )
        dt = np.datetime64(datetime.datetime(int(year), int(month), int(day)))
        cube = cube.assign_coords({"time":dt})
        cube = cube.expand_dims("time")
        return cube
    
    print("Reading TIFF files...")

    # Create the new dataset
    files = glob.glob(tiff_dir + "/*.tif")
    cube = xr.concat([CubeFile(file) for file in files], dim="time")
    cube = cube.rename({"x":"lon", "y":"lat"})
    ds = cube.to_dataset(dim="band")
    ds = ds.rename_vars({1:"sif"})
    # cube = cube.sel(band=1).drop_vars("band")
    # ds = cube.to_dataset(name="sif")

    # set chunking
    ds["sif"] = ds["sif"].chunk({"time":1, "lat":360, "lon":720})

    sif_attrs = {
        "long_name":"solar-induced chlorophyll fluorescence (SIF)",
        "Unit":"W m-1 um-1 sr-1",
        "_FillValue":fill_value_new,
    }
    ds["sif"].attrs = sif_attrs
    ds.attrs = {
        "title":"TCSIF Level 3 SIF data (2007-2021)",
        "history":"converted to zarr by Martin Reinhardt, RSC4Earth, University of Leipzig",
        "source":"https://zenodo.org/records/8242928"
    }

    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)
    encoding = {vname: {
        'compressor': compressor,
        } for vname in ds.data_vars}
    
    print("Writing Zarr files...")

    with ProgressBar():
        ds.to_zarr(zarr_dir, mode="w", consolidated=True, compute=True, encoding=encoding)

    Client.close(client)

if __name__ == '__main__':
    main()