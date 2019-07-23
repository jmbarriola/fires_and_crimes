import pandas as pd
import requests
import io
import shutil
import geopandas as gpd
import zipfile
import os
from datetime import datetime 
import matplotlib.pyplot as plt
%matplotlib inline


def return_zip(url):
    response = requests.get(url)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Whoops it wasn't a 200
        print("Error: " + str(e))
        return "Error: " + str(e)

    # Must have been a 200 status code
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    zip_file.extractall(path='tmp/')

def build_date_range(start_date, end_date):
    date_range = [d.date() for d in pd.date_range(start_date,end_date)]
    return date_range


def convert_date_to_string(date):
    date = str(date)
    date = date.replace('-','')
    return date

dataset = ['smoke','fire']
shape_path = 'tmp/data/oper/newhms/output/'
url = 'https://satepsanone.nesdis.noaa.gov/pub/volcano/FIRE/HMS_ARCHIVE/2018/GIS/{}/hms_{}{}.zip'

def extract_geo_shape(url, date, dataset):
     sday =  convert_date_to_string(date)
     url = url.format(d.upper(),d,sday)
     print('Downloading {} {} shapefile...'.format(day,d))
     z = return_zip(url)
     print("Done")

def create_geo_df(date,  shape_path, dataset):
    sday =  convert_date_to_string(date)
    shp_path = shape_path +'hms_{}{}.shp'.format(dataset, sday)
    if os.path.isfile(shp_path):
        geo_df = gpd.read_file(shp_path)
        geo_df.crs = {'init' :'epsg:4326'}
    return geo_df

for day in dates:
    for d in dataset:
        sday = day.replace('-','')
        url = 'https://satepsanone.nesdis.noaa.gov/pub/volcano/FIRE/HMS_ARCHIVE/2018/GIS/{}/hms_{}{}.zip'.format(d.upper(),d,sday)
        print('Downloading {} {} shapefile...'.format(day,d))
        z = return_zip(url)
        print("Done")
    fire_shp_path = shape_path+'hms_fire{}.shp'.format(sday)
    smoke_shp_path = shape_path+'hms_smoke{}.shp'.format(sday)
    if os.path.isfile(fire_shp_path) & os.path.isfile(smoke_shp_path):
        fire = gpd.read_file(fire_shp_path)    
        smoke = gpd.read_file(smoke_shp_path)
        
        fire.crs = {'init' :'epsg:4326'}
        smoke.crs = {'init' :'epsg:4326'}
