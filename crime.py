import pandas as pd
import geopandas as gpd
from sodapy import Socrata 
from shapely.geometry import Point, shape

client = Socrata("moto.data.socrata.com",None)

columns = ['incident_datetime','incident_type_primary','parent_incident_type','state','city','latitude','longitude']
columns = ','.join(columns)

def creame_crime_df(df_socrata_key, columns_list, date):
    columns = ','.join(columns)
    where_clause = "incident_datetime BETWEEN '{}T00:00:00.000' AND '{}T23:59:59.999'".format(date)
    # Extract results (JSON) 
    results = client.get(df_socrata_key, limit=1000000000,select = columns, where = where_clause)
    # Convert to dataframe 
    df = pd.DataFrame.from_records(results)
    # Convert datetime to date
    df['incident_datetime'] = pd.to_datetime(df['incident_datetime']).dt.date
    return df

def create_crime_by_date_city_df(crimes_df, grouping_cols=['incident_datetime','parent_incident_type', 'city']):
    
    crime_by_date_city_df = crimes_df[grouping_cols]
    crime_by_date_city_df = crimes.groupby(grouping_cols).size().reset_index(name='count')
    return crime_by_date_city_df

def create_cities_df(crimes_df, geo_cols=['city', 'latitude', 'longitude']):
    cities_df = df[geo_cols]
    cities_df['latitude'], cities_df['longitude'] = pd.to_numeric(cities_df['latitude']), pd.to_numeric(cities_df['longitude'])
    return cities

def assign_geom_cities(cities_df, crs = {'init':'epsg:4326'}):
    cities_df = cities_df.groupby('city').agg({'latitude':'mean', 'longitude':'mean'}).reset_index()
    geometry = [Point(xy) for xy in zip(cities_df['longitude'], cities_df['latitude'])] 
    cities_df = gpd.GeoDataFrame(cities, crs=crs, geometry=geometry)
    return cities_df
    