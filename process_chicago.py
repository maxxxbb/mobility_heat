import pandas as pd
import numpy as np
import datetime as dt
import holidays
from matplotlib.cbook import boxplot_stats 



def impute_zeros(taxi_data, level: str):
     
    """
    Imputes zero data
    
    taxi_data (dataframe): pooled data aggregated on daily level
    level (str): 'zone' or 'borough'
    
    """
    
    # Impute zeros: 1. : Create a grid of all combinations of dates and taxi zones
    all_dates = taxi_data['trip_start_timestamp'].unique()
    all_zones = list(range(2, 132)) + list(range(133, 138)) + list(range(139, 266))

    if level == "PU":
        date_location_grid = pd.MultiIndex.from_product([all_dates, all_zones], names=['trip_start_timestamp', 'pickup_community_area']).to_frame(index=False)

     # 2.: Merge with daily data
        merged_data = date_location_grid.merge(taxi_data, on=['trip_start_timestamp', 'pickup_community_area'], how='left')

        # 3. : Replace NaNs with zeros in specified columns
        columns_to_fill = ['trip_count', 'avg_trip_miles', 'avg_fare']
        merged_data[columns_to_fill] = merged_data[columns_to_fill].fillna(0)

        # 4.: Create zero trips indicator
        merged_data['zero_trips'] = np.where(merged_data['trip_count'] == 0, 1, 0)

    elif level == "DO":
        date_location_grid = pd.MultiIndex.from_product([all_dates, all_zones], names=['trip_start_timestamp', 'dropoff_community_area']).to_frame(index=False)
        merged_data = date_location_grid.merge(taxi_data, on=['trip_start_timestamp', 'dropoff_community_area'], how='left')
        columns_to_fill = ['trip_count', 'trip_distance_mean', 'total_amount_mean']
        merged_data[columns_to_fill] = merged_data[columns_to_fill].fillna(0)
        merged_data['zero_trips'] = np.where(merged_data['trip_count'] == 0, 1, 0)


    return merged_data

def fahrenheit_to_celsius(f):
        return (f - 32) * 5/9


def process_chicago(PU_or_DO):
    """
    Merges aggregated chicago cab date with weather data and creates a csv file for
    further analysis - at the community level
    
    input: PU_or_DO: string, either "PU" or "DO"    
    
    """


    trips = pd.read_csv(f"Chigaco_data/aggregated_data_{PU_or_DO}_communityarea.csv")
    climate = pd.read_csv("Chigaco_data/chicago_weather.csv")
    climate.drop(columns=["TAVG", "NAME" , "STATION"], inplace = True)

    # sort by date
    trips = trips.sort_values(by=['trip_start_timestamp']).reset_index(drop=True)
    # drop Nas
    trips = trips.dropna()
    # convert pickup_community_area to int
    if PU_or_DO == "PU":
        trips['pickup_community_area'] = trips['pickup_community_area'].astype(int)
    else:
        trips['dropoff_community_area'] = trips['dropoff_community_area'].astype(int)
    # only keep date of the date time format column
    trips['trip_start_timestamp'] = trips['trip_start_timestamp'].str.split(' ').str[0]

    trips = impute_zeros(trips, PU_or_DO)

    # merge trips and climate data on date
    trips = pd.merge(trips, climate, how='left', left_on='trip_start_timestamp', right_on='DATE')

    us_holidays = holidays.US()

    # Create a new column indicating whether each date is a holiday or not
    trips['holiday'] = trips['trip_start_timestamp'].apply(lambda x: 1 if x in us_holidays else 0)


    # Apply the conversion function to the Fahrenheit column
    trips['TMAX'] = trips['TMAX'].apply(fahrenheit_to_celsius)

    # add a weekday index to the dataframe starting with Mondays = 0 tuesdays = 1 etc.
    trips['Weekday_index'] = pd.to_datetime(trips['trip_start_timestamp']).dt.dayofweek

    taxi_data = trips

    if PU_or_DO == "PU":
        taxi_data.rename(columns={'trip_start_timestamp':'date_pickup' , 'pickup_community_area' : 'PULocationID' , 'trip_count' : 'trip_number' , 'TMAX' : 'tmax_obs'}, inplace=True)
    else:
        taxi_data.rename(columns={'trip_start_timestamp':'date_pickup' , 'dropoff_community_area' : 'DOLocationID' , 'trip_count' : 'trip_number' , 'TMAX' : 'tmax_obs'}, inplace=True)

    # add month and year factors
    taxi_data['Year_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.year)[0] + 1
    taxi_data['Month_fact'] = pd.to_datetime(taxi_data['date_pickup'])
    # log the dependent variable
    taxi_data['log_total'] = np.log(taxi_data['trip_number'] + 1)
    # remove NAs
    taxi_data = taxi_data.dropna(subset=['tmax_obs'])

    # add chebyshev_polynomials

    # add chebyshev_polynomials- time trends
    num_days = len(taxi_data["date_pickup"].unique())
    
    taxi_data['cheby_0'] = 1
    taxi_data['cheby_1'] = taxi_data['date_pickup'].rank(method='dense').astype(int)/num_days



    # recursively defining other chebyshev polynomials for each day until 5th order
    for i in range(2, 6):
        taxi_data[f"cheby_{i}"] = (2  * taxi_data["cheby_1"] * taxi_data[f"cheby_{i-1}"]) - taxi_data[f"cheby_{i-2}"]

           # Assuming there is a 'year' column in your DataFrame representing the year of each data point
        
    out_yearly = pd.DataFrame()
    filtered_yearly = pd.DataFrame()   
        
    for year in taxi_data['Year_fact'].unique():
        year_data = taxi_data[taxi_data['Year_fact'] == year]
        
        for z in year_data[f"{PU_or_DO}LocationID"].unique():
            zcta_data = year_data[year_data[f"{PU_or_DO}LocationID"] == z]
            
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


    # get all days that are outliers in at least 30 community areas.
    date_system_outliers = date_count[date_count['n'] >= 30]['date_pickup']
    non_outliers = taxi_data[~taxi_data['date_pickup'].isin(date_system_outliers)]

    # outliers = taxi_data[taxi_data['date_pickup'].isin(date_system_outliers)]


    taxi_data = non_outliers

    # add temperature bins
    sequence_bins = np.arange(np.floor(taxi_data['tmax_obs'].min()), np.ceil(taxi_data['tmax_obs'].max()) + 1, 3)
    temp_bins = pd.cut(taxi_data['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered = True)
    taxi_data['temp_bins'] = temp_bins

    taxi_data.to_csv(f"chicago_{PU_or_DO}_regression.csv", index=False)


def process_chicago_census_tract(PU_or_DO):
    """
    Merges aggregated chicago cab date with weather data and creates a csv file for
    further analysis
    
    input: PU_or_DO: string, either "PU" or "DO"    
    
    """


    trips = pd.read_csv(f"Chigaco_data/aggregated_data_{PU_or_DO}_censustract.csv")
    climate = pd.read_csv("Chigaco_data/chicago_weather.csv")
    climate.drop(columns=["SNOW","SNWD","TAVG", "NAME" , "STATION"], inplace = True)

    # sort by date
    trips = trips.sort_values(by=['trip_date']).reset_index(drop=True)
    # drop Nas
    trips = trips.dropna()
    

    # merge trips and climate data on date
    trips = pd.merge(trips, climate, how='left', left_on='trip_date', right_on='DATE')

    us_holidays = holidays.US()

    # Create a new column indicating whether each date is a holiday or not
    trips['holiday'] = trips['trip_date'].apply(lambda x: 1 if x in us_holidays else 0)


    # Apply the conversion function to the Fahrenheit column
    trips['TMAX'] = trips['TMAX'].apply(fahrenheit_to_celsius)

    # add a weekday index to the dataframe starting with Mondays = 0 tuesdays = 1 etc.
    trips['Weekday_index'] = pd.to_datetime(trips['trip_date']).dt.dayofweek

    taxi_data = trips

    if PU_or_DO == "PU":
        taxi_data.rename(columns={'trip_date':'date_pickup' , 'pickup_census_tract' : 'PULocationID' , 'trip_count' : 'trip_number' , 'TMAX' : 'tmax_obs'}, inplace=True)
    else:
        taxi_data.rename(columns={'trip_date':'date_pickup' , 'dropoff_census_tract' : 'DOLocationID' , 'trip_count' : 'trip_number' , 'TMAX' : 'tmax_obs'}, inplace=True)

    # add month and year factors
    taxi_data['Year_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.year)[0] + 1
    taxi_data['Month_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.month)[0] + 1
    # log the dependent variable
    taxi_data['log_total'] = np.log(taxi_data['trip_number'] + 1)
    # remove NAs
    taxi_data = taxi_data.dropna(subset=['tmax_obs'])

    # add chebyshev_polynomials
    num_days = len(taxi_data["date_pickup"].unique())
    taxi_data["cheby_0"] = 1
    taxi_data["cheby_1"] = taxi_data['date_pickup'].rank(method='dense').astype(int)/num_days

    # recursively defining other chebyshev polynomials for each day until 5th order
    for i in range(2, 6):
        taxi_data[f"cheby_{i}"] = (2  * taxi_data["cheby_1"] * taxi_data[f"cheby_{i-1}"]) - taxi_data[f"cheby_{i-2}"]

    # outlier filtering
    out_weekdays = pd.DataFrame()
    filtered_weekdays = pd.DataFrame()
    counter = 1
    for z in taxi_data[f'{PU_or_DO}LocationID'].unique():
        zcta_data = taxi_data[taxi_data[f'{PU_or_DO}LocationID'] == z]
        for w in taxi_data['Weekday_index'].unique():
            zcta_weekday = zcta_data[zcta_data['Weekday_index'] == w]
            out = np.ravel(boxplot_stats(zcta_weekday['log_total'])[0]['fliers'])
            out_ids = np.where(np.isin(zcta_weekday['log_total'], out))[0]
            out_df = zcta_weekday.iloc[out_ids]
            zcta_filtered = zcta_weekday = zcta_weekday.drop(out_df.index)
            if counter == 1:
                out_weekdays = out_df
                filtered_weekdays = zcta_filtered
            else:
                out_weekdays = pd.concat([out_weekdays, out_df])
                filtered_weekdays = pd.concat([filtered_weekdays, zcta_filtered])
            counter += 1

     # compute count of outliers per day
    date_count = out_weekdays.groupby('date_pickup').size().reset_index(name='n')


    # get all days that are outliers in at least 35% of the neighborhoods -- ???
    date_system_outliers = date_count[date_count['n'] >= 20]['date_pickup']
    out_weekdays_system = taxi_data[~taxi_data['date_pickup'].isin(date_system_outliers)]

    taxi_data = out_weekdays_system

    # add temperature bins
    sequence_bins = np.arange(np.floor(taxi_data['tmax_obs'].min()), np.ceil(taxi_data['tmax_obs'].max()) + 1, 3)
    temp_bins = pd.cut(taxi_data['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered = True)
    taxi_data['temp_bins'] = temp_bins

    # restrict taxi_data to temperature range -13 to 35
    taxi_data_cut = taxi_data[(taxi_data['tmax_obs'] >= -13) & (taxi_data['tmax_obs'] <= 35)]

    taxi_data_cut.to_csv(f"Chigaco_data/chicago_{PU_or_DO}_regression_censustract.csv", index=False)


for PU_or_DO in ["PU", "DO"]:
    process_chicago(PU_or_DO)
