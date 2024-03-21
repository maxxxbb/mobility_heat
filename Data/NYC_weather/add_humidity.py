import pandas as pd 
import numpy as np
from datetime import timedelta, datetime

## Adds humidity data to climate data 

# 1. 3576162 - Central Park Weather station data including humidity from NOAA
climate = pd.read_csv("Data/NYC_weather/3576162.csv")
climate_NYC = pd.read_csv("Data/NYC_weather/climate_data_NYC_2014_2019.csv")

climate_NYC['DATE'] = pd.to_datetime(climate_NYC['DATE'])

rows_to_keep = ["DATE","REPORT_TYPE","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyMaximumDryBulbTemperature","DailyPrecipitation","DailySnowDepth","DailySnowfall","Sunrise","Sunset"]
climate_filtered = climate[climate["REPORT_TYPE"] == "SOD  "][rows_to_keep]
climate_filtered[["DailySnowfall", "DailyPrecipitation", "DailySnowDepth"]] = climate_filtered[["DailySnowfall", "DailyPrecipitation", "DailySnowDepth"]].replace("T", 0.00).astype(float)
# convert DATE to M_D_Y
climate_filtered["DATE"] = climate_filtered["DATE"].str.split("T").str[0]
climate_filtered["DATE"] = pd.to_datetime(climate_filtered["DATE"], format="%Y-%m-%d")

def convert_to_time(numeric_time):
    hours, minutes = divmod(numeric_time, 100)
    return (datetime.min + timedelta(hours=hours, minutes=minutes)).time()

# Apply the function to the DataFrame column
climate_filtered['Sunrise'] = climate_filtered['Sunrise'].apply(convert_to_time)
climate_filtered['Sunset'] = climate_filtered['Sunset'].apply(convert_to_time)

# Function to calculate time difference in minutes
def calculate_time_difference_in_minutes(row):
    sunrise = pd.to_datetime(row['Sunrise'], format="%H:%M:%S")
    sunset = pd.to_datetime(row['Sunset'], format="%H:%M:%S")
    time_diff = (sunset - sunrise).total_seconds() / 60  # Convert to minutes
    return int(time_diff)

# Add a new column 'Time Difference (minutes)' to the DataFrame
climate_filtered['daylight_time'] = climate_filtered.apply(calculate_time_difference_in_minutes, axis=1)

climate_merged = climate_NYC.merge(climate_filtered, on="DATE", how="left")

climate_merged.to_csv("Data/NYC_weather/climate_NYC_with_humidity.csv", index=False)