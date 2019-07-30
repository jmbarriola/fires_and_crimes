import pandas as pd
import requests
import io
import shutil
import geopandas as gpd
import zipfile
import os

def return_zip(url, sink_path):
    response = requests.get(url)
    # Check if path exists
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Whoops it wasn't a 200
        print("Error: " + str(e))
        return "Error: " + str(e)

    # Must have been a 200 status code
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    # Extract zip files
    zip_file.extractall(path=sink_path)

def build_date_range(start_date, end_date):
    # Build date range as list
    date_range = [d.date() for d in pd.date_range(start_date,end_date)]
    return date_range

def convert_date_to_string(date):
    date = str(date)
    date = date.replace('-','')
    return date

def extract_geo_shape(url,sink_path, date, dataset):
    # Convert date to string for request
    sday =  convert_date_to_string(date)
    # Create url for dataset and day
    url = url.format(dataset.upper(),dataset,sday)
    print('Downloading {} {} shapefile...'.format(sday,dataset))
    z = return_zip(url, sink_path)
    print("Done")

def create_geo_df(date, shape_path, dataset, crs={'init' :'epsg:4326'}):
    # Convert date to string 
    sday =  convert_date_to_string(date)
    # Define local shape path
    shp_path = shape_path +'hms_{}{}.shp'.format(dataset, sday)
    #Empty df
    geo_df = pd.DataFrame()
    # Chech if path exists
    if os.path.isfile(shp_path):
        # Read as geo dataframe
        geo_df = gpd.read_file(shp_path)
        # Set projection
        geo_df.crs = crs
    return geo_df

def count_fires_by_city(fires_by_city_df, grouping_cols_list=['state','city']):
    # Select grouping columns
    fires_by_city_df = fires_by_city_df[grouping_cols_list]
    # Count crimes by state, city and date
    fires_by_city_df = fires_by_city_df.groupby(grouping_cols_list).size().reset_index(name='fires_count')
    return fires_by_city_df