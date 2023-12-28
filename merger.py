from flask import Flask, request, jsonify
import xarray as xr
import cartopy.crs as ccrs
import s3fs
import numcodecs as ncd 
import numpy as np
from pprint import pprint
import os
from datetime import datetime

from flask_jwt_extended import JWTManager, create_access_token, jwt_required

# import setproctitle
# setproctitle.setproctitle("HRRRdataMerger")

mergerServerApp = Flask(__name__)# Set up Flask-JWT-Extended
mergerServerApp.config['JWT_SECRET_KEY'] = 'hrrr-weather-lawn'
jwt = JWTManager(mergerServerApp)

data_folder = os.path.join(os.path.dirname(os.getcwd()), 'dataStore/now/')
class ChunkIdFinder:
    fs = s3fs.S3FileSystem(anon=True)
    chunk_index = xr.open_zarr(s3fs.S3Map("s3://hrrrzarr/grid/HRRR_chunk_index.zarr", s3=fs))

    @classmethod
    def getChunkId(cls, lat, long):
        projection = ccrs.LambertConformal(central_longitude=262.5, 
                                           central_latitude=38.5, 
                                           standard_parallels=(38.5, 38.5),
                                           globe=ccrs.Globe(semimajor_axis=6371229, semiminor_axis=6371229))
        x, y = projection.transform_point(long, lat, ccrs.PlateCarree())
        nearest_point = cls.chunk_index.sel(x=x, y=y, method="nearest")
        fcst_chunk_id = nearest_point.chunk_id.values
        return fcst_chunk_id, nearest_point
    

@mergerServerApp.route('/test', methods=['GET'])
def hello():
    return jsonify({'message': 'Hello, World from merger'})


@jwt_required()
@mergerServerApp.route('/update', methods=['PUT'])
def update():
    data = request.json  # Assuming the incoming data is in JSON format

    lat = data['lat']
    lon = data['long']
    field = data['field']
    datetime = data['datetime']
    value = data['value']
    chunk_id_finder = ChunkIdFinder()
    chunk_id, nearest_point = chunk_id_finder.getChunkId(lat, lon)
    # Retrieve the zarr chunk file based on timestamp
    # chunk_filename = get_chunk_filename(chunk_id)  # Implement your own function to get the filename
    # ds = retrieve_data_local(chunk_filename)
    # pprint(chunk_filename)
    updateChunk(chunk_id,nearest_point,field,datetime,value)
    # ds = open_chunked_zarr_chunk(chunk_filename)
    # print(ds)
    # Find the nearest grid point in the chunk file based on lat/lon
    # nearest_index = find_nearest_grid_point(ds, lat, lon)

    # # Update the temperature at the nearest grid point
    # update_temperature(ds, nearest_index, temperature)

    # # Save the updated zarr chunk file
    # save_updated_data(ds, chunk_filename)
    return 'Updated'



def find_matching_folder(datetime_value, folder_path):
    target_datetime = datetime.strptime(datetime_value, '%Y-%m-%d_%H')
    folders = os.listdir(folder_path)

    for folder in folders:
        folder_datetime = datetime.strptime(folder, '%Y-%m-%d_%H')
        if folder_datetime == target_datetime:
            return os.path.join(folder_path, folder)
    

    return None  # Return None if no matching folder is found




def updateChunk(id, nearest_point,field,datetime, value):
    path = find_matching_folder(datetime,data_folder)
    relative_path = os.path.join(path, '1', field, str(id))
    current_directory = os.getcwd()
    url = os.path.join(current_directory, relative_path)
    data = retrieve_data_local(url)
    
    # Create a writable copy of the data array
    data_copy = np.copy(data)
    pprint(data_copy[nearest_point.in_chunk_x, nearest_point.in_chunk_y])
    # Update the value value in the writable copy
    data_copy[nearest_point.in_chunk_x, nearest_point.in_chunk_y] = value
    
    # Compress the modified data
    compressor = ncd.Blosc(cname='zstd', clevel=3, shuffle=ncd.Blosc.SHUFFLE)
    compressed_data = compressor.encode(data_copy)
    
    # Write the compressed data back to the file
    with open(url, 'wb') as f:
        f.write(compressed_data)
    
    f.close()

  


def retrieve_data_local(url):
    pprint(url)
    with open(url, 'rb') as compressed_data: # using regular file system
        buffer = ncd.blosc.decompress(compressed_data.read())
        # pprint(buffer)

        dtype = "<f4"
        if "surface/PRES" in url: # surface/PRES is the only variable with a larger data type
            dtype = "<f4"
        print(dtype)
        chunk = np.frombuffer(buffer, dtype)
        
        entry_size = 150*150
        num_entries = len(chunk)//entry_size

        if num_entries == 1: # analysis file is 2d
            data_array = np.reshape(chunk, (150, 150))
        else:
            data_array = np.reshape(chunk, (num_entries, 150, 150))

    return data_array




def open_chunked_zarr_chunk(chunk_filename):
    ds = xr.open_zarr(chunk_filename)
    # print(ds)
    return ds





def find_nearest_grid_point(ds, lat, lon):
    distance = ((ds.lat - lat) ** 2 + (ds.lon - lon) ** 2) ** 0.5
    nearest_index = distance.argmin()
    return nearest_index

def update_temperature(ds, nearest_index, temperature):
    ds['temperature'].values[nearest_index] = temperature

def save_updated_data(ds, url):
    ds.to_zarr(url, mode='w')




if __name__ == '__main__':
     mergerServerApp.run(host="0.0.0.0", port=3200, debug=True)
