import xarray as xr
import zarr
import numpy as np
import os
import glob

def main():

    from dask.distributed import Client
    client = Client(n_workers=3, threads_per_worker=3, memory_limit='8GB')
    # link to dashboard
    print(client.dashboard_link)

    netcdf_dir = "Fluxcom-X-GPP-daily-0.25deg"
    zarr_dir = "Fluxcom-X-GPP-daily-0.25deg-100x720x1440.zarr"

    filelist = glob.glob(os.path.join(netcdf_dir,"*.nc"))
    ds = xr.open_mfdataset(filelist,
                           combine='by_coords',
                           chunks={}
    )

    encoding = {vname: {
        'compressor': zarr.Blosc(cname='zstd', clevel=5),
        } for vname in ds.data_vars}
    ds.attrs["history"] = "converted to zarr by Martin Reinhardt, RSC4Earth, University of Leipzig"

    ds["GPP"].attrs["_FillValue"] = np.nan
    ds["land_fraction"].attrs["_FillValue"] = np.nan
    ds["GPP"] = ds["GPP"].chunk({'time': 100, 'lat': 720, 'lon': 1440})
    ds["land_fraction"] = ds["land_fraction"].chunk({'time': 100, 'lat': 720, 'lon': 1440})

    ds.to_zarr(zarr_dir, encoding=encoding, consolidated=True, mode='w', compute=True)

    client.close()

if __name__ == '__main__':
    main()