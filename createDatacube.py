from dask.distributed import Client
import numpy as np
import xarray as xr
import argparse

import os
import glob

def main():

    chunk_size = {'x': 256, 'y': 256, 'time': 1}

    parser = argparse.ArgumentParser(description="Convert NetCDF files in a folder to a single Zarr dataset.")
    parser.add_argument("--input_dir", required=True, help="Path to the input directory containing NetCDF files.")
    parser.add_argument("--output_dir", required=False, help="Path to the output Zarr directory.")
    parser.add_argument("--save_zarr", required=False, help="Save the dataset to a Zarr store.", action='store_true')
    args = parser.parse_args()

    # Use glob to list all .nc files in the input directory
    input_files = glob.glob(os.path.join(args.input_dir, '*.nc'))

    client = Client()  # Starts a local Dask client
    # print dashboard link
    print("Dashboard available under: " + str(client.dashboard_link))

    # Open multiple datasets and add a new coordinate for year based on file names
    datasets = []
    for file_path in input_files:
        time = int(file_path.split('\\')[-1].split('_')[-1].split('.')[0])  # Extract year from file name
        ds = xr.open_dataset(file_path, chunks=chunk_size, decode_coords="all").drop_vars('lambert_azimuthal_equal_area').rename({'Band1': 'deadwood'})
        ds = ds.assign_coords(time=np.array(time))
        datasets.append(ds)

    # Concatenate along the new 'year' dimension
    combined_ds = xr.concat(datasets, dim='time')
    combined_ds = combined_ds.transpose('time', 'x', 'y')

    # clean the attributes
    # List of attributes to remove
    attrs_to_remove = ['Conventions', 'GDAL', 'history', 'GDAL_AREA_OR_POINT']

    # Loop through the list and delete each attribute if it exists
    for attr in attrs_to_remove:
        if attr in ds.attrs:
            del combined_ds.attrs[attr]
            
    # Print the combined dataset
    print(combined_ds)
    print('Dataset created successfully.')

    if not args.save_zarr:
        # ask to save as zarr
        save_zarr = input('Do you want to save the dataset to a Zarr store? (y/n): ')
        if save_zarr.lower() != 'y':
            print('Exiting without saving.')
            client.close()
            exit()
        
    # ask for the output zarr file name if not provided
    if not args.output_dir:    
        output_zarr = input('Enter the Zarr folder name: ')
        # Check if the user entered a file name
        if not output_zarr:
            print('No file name entered. Exiting without saving.')
            client.close()
            exit()
    else:
        output_zarr = args.output_dir

    # Add .zarr extension if not already present
    if not output_zarr.endswith('.zarr'):
        output_zarr += '.zarr'

    # Save to Zarr
    combined_ds.to_zarr(output_zarr, mode='w')

    client.close()
    
    print('Data saved to Zarr store.')

if __name__ == '__main__':
    main()