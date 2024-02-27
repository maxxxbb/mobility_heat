import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
from rasterstats import zonal_stats

# read shapefile

# read shapefile
taxi_zones = gpd.read_file('NYC_Taxi_Zones/geo_export_1c7083fc-0597-4b3a-990a-eafab7fcd68a.shp')
# read tif file with satelitte image temperature deviations
tif_path = 'NYC_weather/f_deviation_smooth.tif'  # Replace with your TIF file path
temperature_deviation = rasterio.open(tif_path)


stats = zonal_stats(taxi_zones, tif_path, stats='mean')
df_stats = pd.DataFrame(stats)

# merge with taxi zones 

taxi_zones_with_deviation = taxi_zones.join(df_stats)
taxi_zones_with_deviation.sort_values(by='mean', ascending=False).head(10)
taxi_zones_with_deviation.rename(columns={'mean': 'temperature_deviation_summer'}, inplace=True)

# get socioeconomic data 

taxi_socio = pd.read_csv("ACS_data/taxi_zones_ACS_parks_beaches.csv")

# merge with deviation column on LocationID and location_i only keep deviation column from taxi_zones_with_deviation
taxi_socio_deviation = taxi_socio.merge(taxi_zones_with_deviation[["location_i" , "temperature_deviation_summer"]], left_on='LocationID', right_on='location_i')
taxi_socio_deviation.drop(columns=['LocationID'], inplace=True)
# save as csv in ACS_data folder

taxi_socio_deviation.to_csv("ACS_data/taxi_zones_ACS_parks_beaches_deviation.csv", index=False)