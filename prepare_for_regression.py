import pandas as pd
import numpy as np
import holidays
from datetime import datetime
from matplotlib.cbook import boxplot_stats 
import time


def fahrenheit_to_celsius(f):
        return (f - 32) * 5/9


def impute_zeros(taxi_data, level: str):
     
    """
    Imputes zero data
    
    taxi_data (dataframe): pooled data aggregated on daily level
    level (str): 'zone' or 'borough'
    
    """
    
    # Impute zeros: 1. : Create a grid of all combinations of dates and taxi zones - wo/airports
    all_dates = taxi_data['date_pickup'].unique()
    all_zones = list(range(2, 132)) + list(range(133, 138)) + list(range(139, 266))

    date_location_grid = pd.MultiIndex.from_product([all_dates, all_zones], names=['date_pickup', f'{level}LocationID']).to_frame(index=False)

    # 2.: Merge with daily data
    merged_data = date_location_grid.merge(taxi_data, on=['date_pickup', f'{level}LocationID'], how='left')

    # 3. : Replace NaNs with zeros in specified columns
    columns_to_fill = ['trip_number', 'trip_distance_mean', 'total_amount_mean']
    merged_data[columns_to_fill] = merged_data[columns_to_fill].fillna(0)

    # 4.: Create zero trips indicator
    merged_data['zero_trips'] = np.where(merged_data['trip_number'] == 0, 1, 0)

    return merged_data
     


def prepare_data_for_regression(input_data,climate_data, level: str, subset: str):
    """
    Merges cab data with climate variables, 
    adds month and year factors as columns, adds time trends (chebyshevs)
    filters out outliers and adds holiday indicators.

    Args:
        input_data (str): path to preprocessed grouped taxi csv
        climate_data (str): path to climate csv
        level (str): PU, DO , or OD
        subset (str): all, FHV , YG(Yellow-Green)

    Returns:
        regression_df: Dataframe with merged taxi and tourism data
    """
    

    # load green cab data grouped and merge with tourism data (careful some duplicate pickup_date and location combinations in tourism data)

    grouped_data = pd.read_csv(input_data)
    climate = pd.read_csv(climate_data)

    # 'AWND' : 'windspeed_obs' to be included once NOAA site is up again
    climate['TMAX'] = climate['TMAX'].apply(fahrenheit_to_celsius)

    climate.rename(columns={'DATE': 'date_pickup' , 'TMAX' : 'tmax_obs' , 'PRCP' : 'pr_obs' , 'SNWD': 'Snowdepth' }, inplace=True)

    if level == 'OD':
        taxi_data = impute_zeros_od(grouped_data)
    else:
        taxi_data = impute_zeros(grouped_data, level)


    taxi_data = pd.merge(taxi_data, climate, on=['date_pickup'], how='left')
    
    # drop duplicates ( for some locations there are double entries)
    if level == 'OD':
        taxi_data.drop_duplicates(subset = ["PULocationID", "DOLocationID","date_pickup"],keep='first', inplace=True, ignore_index=True)
    else:
        taxi_data.drop_duplicates(subset = [f"{level}LocationID", "date_pickup"],keep='first', inplace=True, ignore_index=True)

    # add month and year factors
    taxi_data['Year_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.year)[0] + 1
    taxi_data['Month_fact'] = pd.to_datetime(taxi_data['date_pickup']).dt.month

    # add weekday index

    taxi_data['Weekday_index'] = pd.to_datetime(taxi_data['date_pickup']).dt.dayofweek + 1

    us_holidays = holidays.US()

    # Create holiday column
    taxi_data['holiday'] = taxi_data['date_pickup'].apply(lambda x: 1 if x in us_holidays else 0)
    taxi_data['holiday'] = taxi_data['holiday'].astype('category')

    # log the dependent variable
    taxi_data['log_total'] = np.log(taxi_data['trip_number'] + 1)

    # add chebyshev_polynomials- time trends
    num_days = len(taxi_data["date_pickup"].unique())
    
    taxi_data['cheby_0'] = 1
    taxi_data['cheby_1'] = taxi_data['date_pickup'].rank(method='dense').astype(int)/num_days
   
    
    # recursively defining other chebyshev polynomials for each day until 5th order
    for i in range(2, 6):
        taxi_data[f"cheby_{i}"] = (2  * taxi_data["cheby_1"] * taxi_data[f"cheby_{i-1}"]) - taxi_data[f"cheby_{i-2}"]

    
    

    if not level == 'OD':

        out_yearly = pd.DataFrame()
        filtered_yearly = pd.DataFrame()

        # Assuming there is a 'year' column in your DataFrame representing the year of each data point
        for year in taxi_data['Year_fact'].unique():
            year_data = taxi_data[taxi_data['Year_fact'] == year]
            
            for z in year_data[f'{level}LocationID'].unique():
                zcta_data = year_data[year_data[f'{level}LocationID'] == z]
                
                for w in year_data['Weekday_index'].unique():
                    zcta_weekday = zcta_data[zcta_data['Weekday_index'] == w]
                    
                    # Perform outlier detection using boxplot_stats
                    out = np.ravel(boxplot_stats(zcta_weekday['trip_number'])[0]['fliers'])
                    out_ids = np.where(np.isin(zcta_weekday['trip_number'], out))[0]
                    out_df = zcta_weekday.iloc[out_ids]
                    
                    # Create a DataFrame without outliers
                    zcta_filtered = zcta_weekday.drop(out_df.index)
                    
                    # Concatenate results into yearly DataFrames
                    out_yearly = pd.concat([out_yearly, out_df])
                    filtered_yearly = pd.concat([filtered_yearly, zcta_filtered])


        
        
        # compute count of outliers per day
        date_count = out_yearly.groupby('date_pickup').size().reset_index(name='n')


        # get all days that are outliers in at least 40% of the neighborhoods - maybe move from neighborhood level to total level. If within a
        date_system_outliers = date_count[date_count['n'] >= 100]['date_pickup']
        non_outliers = taxi_data[~taxi_data['date_pickup'].isin(date_system_outliers)]

        # outliers = taxi_data[taxi_data['date_pickup'].isin(date_system_outliers)]


        taxi_data = non_outliers
        
    
        
        date_count.to_csv(f'{input_data[:input_data.find("/")]}/{level}/final/outliers_{subset}_{level}.csv', index=False)
        taxi_data.to_csv(f'{input_data[:input_data.find("/")]}/{level}/final/final_data_{subset}_{level}.csv', index=False)
    
        

def impute_zeros_od(taxi_data):
         
    """
    Imputes zero data
    
    taxi_data (dataframe): pooled data aggregated on daily level
    level (str): 'zone' or 'borough'
    
    """
    taxi_data.rename(columns={'trip_count': 'trip_number'}, inplace=True)
    # Impute zeros: 1. : Create a grid of all combinations of dates and taxi zones
    all_dates = taxi_data['date_pickup'].unique()
    all_zones_PU = list(range(2, 132)) + list(range(133, 138)) + list(range(139, 266))
    all_zones_DO = list(range(2, 132)) + list(range(133, 138)) + list(range(139, 266))

    date_location_grid = pd.MultiIndex.from_product([all_dates, all_zones_PU, all_zones_DO], names=['date_pickup', 'PULocationID', 'DOLocationID']).to_frame(index=False)

    # 2.: Merge with daily data
    merged_data = date_location_grid.merge(taxi_data, on=['date_pickup', 'PULocationID' , 'DOLocationID'], how='left')

    # 3. : Replace NaNs with zeros in specified columns
    columns_to_fill = ['trip_number']
    merged_data[columns_to_fill] = merged_data[columns_to_fill].fillna(0)

    # 4.: Create zero trips indicator
    merged_data['zero_trips'] = np.where(merged_data['trip_number'] == 0, 1, 0)

    return merged_data



time_start = time.time()

for level in ['PU','DO']:
    # ['PU','DO' , 'OD']
    for subset in ['YG','FHV']:
        taxi_data = f"Pooled_data/{level}/data_grouped_{subset}_{level}.csv"
        climate_data = 'Data/NYC_weather/climate_data_NYC_2014_2019.csv'
        prepare_data_for_regression(taxi_data,climate_data, level, subset)
time_end = time.time()

print(f"Time elapsed: {time_end - time_start} seconds")





