import xarray as xr
import numpy as np
import rioxarray
import zarr
import os
import glob
import datetime
import warnings

def main():
    
    print("Converting GIMMS NDVI AVHRR MODIS consolidated data to Zarr format...")
    
    warnings.filterwarnings("ignore", category=UserWarning)
    
    from dask.distributed import Client
    client = Client(n_workers=8, threads_per_worker=2, memory_limit='4GB')
    # link to dashboard
    print("Dashboard available under: " + str(client.dashboard_link))

    # Set the directory where the data is stored
    tiff_dir = "PKU_GIMMS_NDVI_AVHRR_MODIS_consolidated_1982_2022"
    zarr_dir = "PKU_GIMMS_NDVI_AVHRR_MODIS_consolidated_1982_2022_1x4320x2160.zarr"
    fill_value_old = 65535 # fill value in the original data from README
    fill_value_new = np.nan

    def CubeFile(file):
        # extract date from filename
        date = os.path.basename(file).split("_")[-1]
        date = date.split(".")[0]
        year = date[0:4]
        month = date[4:6]
        halfmonth = date[6:8]
        day = 8 if halfmonth == "01" else 23
        cube = xr.open_dataarray(file,
                                 engine="rasterio",
                                 chunks={-1}
        )
        cube = cube.where(cube != fill_value_old, fill_value_new)
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
    ds = ds.rename_vars({1:"NDVI", 2:"QC"})

    # set chunking
    ds["NDVI"] = ds["NDVI"].chunk({"time":1, "lat":2160, "lon":4320})
    ds["QC"] = ds["QC"].chunk({"time":1, "lat":2160, "lon":4320})

    ndvi_attrs = {
        "long_name":"Normalized Difference Vegetation Index",
        "_FillValue":fill_value_new,
        "ValidRange":[0, 1000],
    }
    qc_attrs = {
        "long_name":"Quality Control",
        "First digit: consolidation method":[0, 1, 2, 3, 4, 5, 9],
        "Second digit: quality I":[0, 1, 2, 9],
        "Third digit: quality II":[0, 1, 2, 3, 4, 9],
        "_FillValue":fill_value_new,
    }
    ds["NDVI"].attrs = ndvi_attrs
    ds["QC"].attrs = qc_attrs
    ds.attrs = {
        "title":"PKU GIMMS NDVI AVHRR MODIS consolidated",
        "history":"converted to zarr by Martin Reinhardt, RSC4Earth, University of Leipzig",
        "source":"https://zenodo.org/records/8253971",
        "README for QC":"https://zenodo.org/records/8253971/files/Readme_for_PKU_GIMMS_NDVI_Product_updated_20230817.pdf?download=1"
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