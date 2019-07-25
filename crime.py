import pandas as pd
import geopandas as gpd
from sodapy import Socrata 
from shapely.geometry import Point, shape

client = Socrata("moto.data.socrata.com",None)

def create_crime_df(df_socrata_key, columns_list, date):
    
    # Convert date to string
    date= str(date)
    # Create columns string for select statement
    columns = ','.join(columns_list)
    # Define where clause
    where_clause = "incident_datetime BETWEEN '{}T00:00:00.000' AND '{}T23:59:59.999'".format(date,date)
    
    # Extract results (JSON) from socrata API 
    results = client.get(df_socrata_key, limit=1000000000,select = columns, where = where_clause)
    # Convert to dataframe 
    df = pd.DataFrame.from_records(results)
    # Convert datetime to date
    df['incident_date'] = pd.to_datetime(df['incident_datetime']).dt.date
    return df

def create_crime_by_date_city_df(crimes_df, grouping_cols_list=['incident_date','parent_incident_type','state','city']):
    # Select grouping columns
    crime_by_date_city_df = crimes_df[grouping_cols_list]
    # Count crimes by state, city and date
    crime_by_date_city_df = crime_by_date_city_df.groupby(grouping_cols_list).size().reset_index(name='crime_count')
    return crime_by_date_city_df

def create_cities_df(crimes_df, geo_cols_list=['state', 'city', 'latitude', 'longitude']):
    # Select location columns
    cities_df = crimes_df[geo_cols_list]
    # Convert latitude and longitude to numeric
    cities_df['latitude'], cities_df['longitude'] = pd.to_numeric(cities_df['latitude']), pd.to_numeric(cities_df['longitude'])
    # Get mean latitude and longitude for crimes in a city
    cities_df = cities_df.groupby(['state','city']).agg({'latitude':'mean', 'longitude':'mean'}).reset_index()
    return cities_df

def assign_geom_cities(cities_df, crs = {'init':'epsg:4326'}):
    # Create geometry of a city from latitude and longitude
    geometry = [Point(xy) for xy in zip(cities_df['longitude'], cities_df['latitude'])]
    # Create cities geo dataframe
    cities_df = gpd.GeoDataFrame(cities_df, crs=crs, geometry=geometry)
    return cities_df

def create_city_buffer(cities_df,buffer_radius):
    # Create cities buffer in meters
    cities_df['geometry'] = cities_df.geometry.buffer(buffer_radius)
    # Assign value of the buffer as column
    cities_df['buffer'] = buffer_radius
    return cities_df