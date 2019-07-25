from fire_and_smoke import *
from crime import *

# Fire & smoke params
shape_path = 'test-firecrime/data/oper/newhms/output/'
url = 'https://satepsanone.nesdis.noaa.gov/pub/volcano/FIRE/HMS_ARCHIVE/2018/GIS/{}/hms_{}{}.zip'
sink_path="test-firecrime"

# Crime params
client = Socrata("moto.data.socrata.com",None)
df_socrata_key = "p6kq-vsa3"
columns_list = ['incident_datetime','incident_type_primary','parent_incident_type','state','city','latitude','longitude']

# Date
start_date = '2018-07-01'
end_date = '2018-07-31'
date_range = build_date_range(start_date, end_date)

df_list = [] 

for date in date_range:
    # Smoke df
    extract_geo_shape(url,sink_path, date, dataset='smoke')
    smoke = create_geo_df(date, shape_path, dataset='smoke')
    # Select density and geometry
    smoke = smoke[['Density', 'geometry']]

    # Fire df
    # ESRI:102009 North America Lambert Conformal Conic (unit is in meters, good for North America)
    meters_crs='+proj=lcc +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs'
    extract_geo_shape(url,sink_path, date, dataset='fire')
    fire = create_geo_df(date,shape_path, dataset='fire')
    fire = fire.to_crs(meters_crs)

    # Crime df
    crimes = create_crime_df(df_socrata_key, columns_list, date)
    crime_by_city = create_crime_by_date_city_df(crimes)

    # Cities df
    cities = create_cities_df(crimes)
    cities = assign_geom_cities(cities, crs = {'init':'epsg:4326'})

    # Spatial join between cities and smoke
    cities_smoke = gpd.sjoin(cities, smoke, how='left',op="within")

    #Cities in meters crs
    cities = cities.to_crs(meters_crs)
    cities = create_city_buffer(cities,buffer_radius=50000)

    # Spatial join between cities and fires
    cities_fire = gpd.sjoin(cities, fire, how='left')
    # Count fires
    cities_fire = count_fires_by_city(cities_fire)

    # Join crimes and smoke
    cities_smoke =pd.merge(crime_by_city, cities_smoke)
    # Final df
    final_df = pd.merge(cities_smoke, cities_fire)[['incident_date','parent_incident_type','crime_count','city','state','Density','fires_count']]
   
    print('Writing df for {}'.format(date))
    df_list.append(final_df)
    

df_list = pd.concat(df_list)
df_list.to_csv("df_test.csv",index=False)