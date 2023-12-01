import pandas as pd
import numpy as np
from datetime import datetime
from matplotlib.cbook import boxplot_stats 
import holidays

def extract_tourism(input_path):
    """
    Extracts tourism columns from preprocessed csv file
    and saves as separate csv file

    Args:
        input_path (str): path to preprocessed yellow taxi csv
        output_path (str): output path for tourism csv

    Returns:
        tourism_df: Dataframe with tourism columns
    """
    # read the CSV file into a DataFrame
    preprocessed = pd.read_csv(input_path)

    # select the columns to keep
    selected_columns = ["date_pickup", "PULocationID", "user_ratings_total", "Population", "average_monthly_tourism", "dynamic_tourism"]

    # select the columns and reset the index
    tourism_df = preprocessed.loc[:, selected_columns]

    tourism_df.to_csv('tourism_bydate.csv', index=False)


def prepare_data_for_regression(input_data,tourism_data):
    """
    Merges cab data with tourism variables, 
    adds month and year factors as columns,
    filters out outliers and adds holiday indicators.

    Args:
        input_data (str): path to preprocessed grouped taxi csv
        tourism_data (str): path to tourism csv

    Returns:
        regression_df: Dataframe with merged taxi and tourism data
    """
    

    # load green cab data grouped and merge with tourism data (careful some duplicate pickup_date and location combinations in tourism data)

    taxi_grouped = pd.read_csv(input_data)
    tourism_weekdays_climate = pd.read_csv(tourism_data)

    if 'DOLocationID' not in taxi_grouped.columns:
    # rename 'PU Location ID' to 'PULocationID'
        taxi_grouped = taxi_grouped.rename(columns={'DOlocationID': 'DOLocationID'})


    taxi_data = pd.merge(taxi_grouped, tourism_weekdays_climate, left_on=['date_pickup','DOLocationID'], right_on= ["date_pickup", "PULocationID"], how='left')
    # drop duplicates ( for some locations there are double entries)
    taxi_data.drop_duplicates(subset = ["DOLocationID", "date_pickup"],keep='first', inplace=True, ignore_index=True)

    # add month and year factors
    taxi_data['Year_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.year)[0] + 1
    taxi_data['Month_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.month)[0] + 1
    # log the dependent variable
    taxi_data['log_total'] = np.log(taxi_data['trip_number'] + 1)
    # remove NAs
    taxi_data = taxi_data.dropna(subset=['tmax_obs'])


     # add chebyshev_polynomials

    num_days = len(taxi_data["date_pickup"].unique())
    taxi_data['cheby_0'] = 1
    taxi_data['cheby_1'] = taxi_data['date_pickup'].rank(method='dense').astype(int)/num_days
   
    
    # recursively defining other chebyshev polynomials for each day until 5th order
    for i in range(2, 6):
        taxi_data[f"cheby_{i}"] = (2  * taxi_data["cheby_1"] * taxi_data[f"cheby_{i-1}"]) - taxi_data[f"cheby_{i-2}"]

    # outlier filtering
    out_weekdays = pd.DataFrame()
    filtered_weekdays = pd.DataFrame()
    counter = 1
    for z in taxi_data['DOLocationID'].unique():
        zcta_data = taxi_data[taxi_data['DOLocationID'] == z]
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


    us_holidays = holidays.US()

    # Create holiday column
    taxi_data['holiday'] = taxi_data['date_pickup'].apply(lambda x: 1 if x in us_holidays else 0)
    taxi_data['holiday'] = taxi_data['holiday'].astype('category')
    taxi_data['holiday'] = taxi_data['holiday'].astype('category')# filter temperature outliers
    

    # compute count of outliers per day
    date_count = out_weekdays.groupby('date_pickup').size().reset_index(name='n')


    # get all days that are outliers in at least 35% of the neighborhoods -- ???
    date_system_outliers = date_count[date_count['n'] >= 43]['date_pickup']
    out_weekdays_system = taxi_data[~taxi_data['date_pickup'].isin(date_system_outliers)]

    taxi_data = out_weekdays_system

    # add temperature bins
    sequence_bins = np.arange(np.floor(taxi_data['tmax_obs'].min()), np.ceil(taxi_data['tmax_obs'].max()) + 1, 3)
    temp_bins = pd.cut(taxi_data['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered = True)
    taxi_data['temp_bins'] = temp_bins    
    
    # restrict taxi_data to temperature range -7 to 35
    taxi_data_cut = taxi_data[(taxi_data['tmax_obs'] >= -7) & (taxi_data['tmax_obs'] <= 35)]
    
    taxi_data_cut.to_csv(f'{input_data[:input_data.find("/")]}/data_regression_DO.csv', index=False)
    

# input data string until the first /




taxi_data = "High_Volume_FHV/merged_grouped_DO.csv"
tourism_data = 'tourism_bydate.csv'

prepare_data_for_regression(taxi_data,tourism_data)



    

