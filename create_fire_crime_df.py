from fire_and_smoke import *
from crime import *
import time

# Fire & smoke params
shape_path = 'test-firecrime/data/oper/newhms/output/'
url = 'https://satepsanone.nesdis.noaa.gov/pub/volcano/FIRE/HMS_ARCHIVE/2018/GIS/{}/hms_{}{}.zip'
sink_path="test-firecrime"
# ESRI:102009 North America Lambert Conformal Conic (unit is in meters, good for North America)
meters_crs='+proj=lcc +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs'

# Crime params
client = Socrata("moto.data.socrata.com",None)
df_socrata_key_list = ["p6kq-vsa3", "jrat-ef37", 'wrmr-tdyp','nnzs-rxi5']
columns_list = ['incident_datetime','incident_type_primary','parent_incident_type','state','city','latitude','longitude']

# Date
start_date = '2018-01-01'
end_date = '2018-12-31'
date_range = build_date_range(start_date, end_date)

df_list = [] 

for date in date_range:
    # Smoke df
    extract_geo_shape(url,sink_path, date, dataset='smoke')
    smoke = create_geo_df(date, shape_path, dataset='smoke')
    # Fire df
    extract_geo_shape(url,sink_path, date, dataset='fire')
    fire = create_geo_df(date,shape_path, dataset='fire')

    # df is empty if there are no shapes
    if not smoke.empty:
        # Select density and geometry
        smoke = smoke[['Density', 'geometry']]

    # df is empty if there are no shapes
    if not fire.empty:
        fire = fire.to_crs(meters_crs)

    # Crimes and cities lists
    crimes_list = []
    cities_list = []

    # For loop for different cities
    for df_socrata_key in df_socrata_key_list:

        # Crime df
        crimes = create_crime_df(df_socrata_key, columns_list, date)
        # If there's no data for city returns None
        if crimes is not None:
            crime_by_city = create_crime_by_date_city_df(crimes)

            # Cities df
            cities = create_cities_df(crimes)
            cities = assign_geom_cities(cities, crs = {'init':'epsg:4326'})

        #Append df to lists
        crimes_list.append(crime_by_city)
        cities_list.append(cities)
        time.sleep(0.5)
    
    # Crimes dataframe
    crime_by_city = pd.concat(crimes_list)
    # Cities dataframe
    cities = pd.concat(cities_list)

    # df is empty if there are no shapes
    if not smoke.empty:
        # Spatial join between cities and smoke
        cities_smoke = gpd.sjoin(cities, smoke, how='left',op="within")
        cities_smoke.fillna(0,inplace=True)
        # Get total density of smoke clouds
        cities_smoke = cities_smoke.groupby(['state','city'])['Density'].sum().reset_index(name='total_smoke_density')
    else:
        cities_smoke = cities[['state','city']]
        cities_smoke['total_smoke_density'] = float('nan')
    
    #Cities in meters crs
    cities = cities.to_crs(meters_crs)
    cities = create_city_buffer(cities,buffer_radius=50000)

    # df is empty if there are no shapes
    if not fire.empty:
        # Spatial join between cities and fires
        cities_fire = gpd.sjoin(cities, fire, how='left')
        # Count fires
        cities_fire = count_fires_by_city(cities_fire)
    else:
        cities_fire = cities[['state','city']]
        cities_fire['fires_count'] = float('nan')

    # Join crimes and smoke
    cities_smoke =pd.merge(crime_by_city, cities_smoke)
    # Final df
    final_df = pd.merge(cities_smoke, cities_fire)[['incident_date','parent_incident_type','crime_count','city','state','total_smoke_density','fires_count']]
   
    print('Writing df for {}'.format(date))
    df_list.append(final_df)
    

df_list = pd.concat(df_list)
df_list.to_csv("df_test_2018.csv",index=False)