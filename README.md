# ZarrConverter

## Installation

```bash
conda create -n ZarrConverter python pip
conda activate ZarrConverter
conda install -n ZarrConverter -c conda-forge xarray importlib_metadata ipykernel dask gdal zarr h5netcdf netcdf4 fsspec requests aiohttp rioxarray

python -Xfrozen_modules=off -m ipykernel install --user --name "ZarrConverter" --display-name "ZarrConverter Kernel"
```

## Usage

### NetCDF to Zarr

Check the notebook `netcdf2zarr.ipynb` how it works. A small cli script is also available:

```bash
python netcdf2zarr.py /path/to/netcdf/folder /somedir/outputpath.zarr
```

#### using dask

You can specify the scheduler with the `-ds` option. By default no dask is used. If you want to use dask, you have to specify the scheduler with the `-ds`. The default scheduler is `tcp://localhost:8786`. 

```bash
python netcdf2zarr.py /path/to/netcdf/folder /somedir/outputpath.zarr -d
python netcdf2zarr.py /path/to/netcdf/folder /somedir/outputpath.zarr -d -ds tcp://123.123.123.123:4444
```

#### chunking

You can specify the chunking with the `-c` option. By default the chunking is set to `1 1080 1080`.  The chunking is specified as `time,lat,lon`. An example for a chunking of `256 30 30`, which might be better for time series analysis would be:

```bash
python netcdf2zarr.py /path/to/netcdf/folder /somedir/outputpath.zarr -c 256 30 30
```