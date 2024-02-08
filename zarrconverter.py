#!/usr/bin/env python3

# This script converts a set of netCDF
# files to a zarr group using xarray and dask

import xarray as xr
import zarr
import os
import glob
import datetime
import argparse
import logging
from dask.distributed import Client

#setup the argument parser
parser = argparse.ArgumentParser(
                    prog='zarrconverter.py',
                    description='This program converts a set of netCDF files to a zarr group using xarray and dask and keeps metadata and variables untouched.',
                    epilog='(c) 2024, University of Leipzig, Germany')

parser.add_argument('netcdf_dir', help='The directory containing the netCDF files')
parser.add_argument('zarr_dir', help='The directory where the zarr group will be stored. This will be overwritten if it exists')
parser.add_argument('-d', '--dask', action='store_true', help='Use dask to parallelize the conversion')
parser.set_defaults(dask=False)
parser.add_argument('-ds', '--dask-scheduler', help='The address of the dask scheduler to use, if not tcp//localhost:8786')
parser.set_defaults(dask_scheduler='tcp://localhost:8786')
parser.add_argument('-c', '--chunk-size', nargs=3, type=int, help='The size of the chunks [time, longitude, latitude] to use for the zarr group (default: 1 1080 1080)')
parser.set_defaults(chunk_size=[1, 1080, 1080])
parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output')

# parsing the arguments
args = parser.parse_args()
netcdf_dir = args.netcdf_dir
zarr_dir = args.zarr_dir
usedask = args.dask
dask_scheduler = args.dask_scheduler
chunk_size = args.chunk_size
verbose = args.verbose
if verbose:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logging.info('Verbose output enabled')

logging.info('start of script')

# setup dask client
if usedask:
    try:
        client = Client(dask_scheduler)
    except Exception as e:
        logging.error('Error creating dask client: %s', e)
    else:
        logging.info('dask client is connected successfully')

# open the netCDF files
filelist = glob.glob(os.path.join(netcdf_dir,"*.nc"))
ds = xr.open_mfdataset(filelist[1:2], combine='by_coords',
                       chunks={'time': chunk_size[0], 'longitude' : chunk_size[1], 'latitude': chunk_size[2]},
                       parallel=usedask)
logging.info('Files are opened and combined to xarray dataset')

# setup of encoding and reprocessing attribute
encoding = {vname: {
    'compressor': zarr.Blosc(cname='zstd', clevel=5)
    } for vname in ds.data_vars}
ds.attrs['reprocessing'] = "rechunked and compressed with zarr using zarrconverter.py script."
logging.info('Encoding is set to Blosc compressor with zstd level 5 and reprocessing attribute is added to the dataset.')

logging.info('start of chunking and compression to zarr: ', datetime.datetime.now())
print('start of processing')
# save the dataset to zarr
ds.to_zarr(zarr_dir, encoding=encoding, consolidated=True, mode='w', compute=True)
logging.info('Dataset is saved to zarr group: ', datetime.datetime.now())

if usedask:
    client.close()
logging.info('dask client is closed')

logging.info('end of script')