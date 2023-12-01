import datetime as dt
import os
import time

import fastparquet as fp
import pandas as pd

## 1. Define Functions


def preprocess_climate_data(path : str):
    """
    Preprocesses climate data from  CSV file.

    Parameters:
    file_path (str): The path to the CSV file containing the climate data.

    Returns:
    pandas.DataFrame: A DataFrame containing the preprocessed climate data.
    """
    
    combined_data = pd.read_csv(path)

    # Subset the subway data to get relevant climate variables
    climate_only = combined_data[["Date", "tmax_obs", "Weekday_index", "pr_obs", "season", "windspeed_obs", "Snowdepth"]]
    climate_only = climate_only.rename(columns={"Date": "date_pickup"})
    climate_only["date_pickup"] = pd.to_datetime(climate_only["date_pickup"])
    climate_only = climate_only.drop_duplicates(subset=["date_pickup"])

    return climate_only


# green and yellow cab different pickup variable names lpep for green and tpep for yellow

def preprocess_cab_data(path : str , PU_or_DO : str):
    """
    Groups taxitrip data by day and merges with climate data on date.

    Args:
        path (str): The top-level directory where the parquet files are located.
        dataset (str) : Green, Yellow or FHV data
        PU_or_DO (str) : binary indicator whether trips should be aggregated by Pickup or Dropoff Location

    Returns:
        csv file per month with daily trip data and climate data merged on date.
    """
    # empty list to keep track of done files
    # if dataset green pickup string "lpep" if yellow pickup string "tpep" , if fhv = "Pickup_datetime"
    
    year = 2019

    
    climate_only = preprocess_climate_data("data_zcta_with_demographic_scaled_more_climate_tourism_aggregated_mitigation_hvi_radius_weighted_with_cheby.csv")

    pickup_string = "pickup_datetime"
        
    year_path = os.path.join(path, str(year))

    # Loop over the files in year folders
    for i, file in enumerate(os.listdir(year_path)):
        # Set the path to the file
        file_path = os.path.join(year_path, file)

        # read the file
        df = fp.ParquetFile(file_path).to_pandas()

        # extract year and month from file name and stores in year_month var
        stop_char = len(file) - 8
        start_char = stop_char - 7
        year_month = file[start_char:stop_char].split("-")
        year_month[1] = str(int(year_month[1].lstrip("0")))
        
        # extracts date from pickup_date and stores in new columns
        df["date_pickup"] = df[pickup_string].dt.strftime("%Y-%m-%d")
        df["year"] = df[pickup_string].dt.year
        df["month"] = df[pickup_string].dt.month
        # only keep rows where monthyear from filename matches monthyear from pickup_date
        df = df[(df["year"] == int(year_month[0])) & (df["month"] == int(year_month[1]))]
        # drop rows where either PULocationID or DOLocationID is in [1,132,138] (airport)
    
        airporttrips = (df['PULocationID'].isin([1, 132, 138])) | (df['DOLocationID'].isin([1, 132, 138]))
        df = df[~airporttrips]

        # create new dataframe for each month where trips are aggregated by date ( and PULocationID) and invlude the mean fare_amount and total amount per group while still including the number of trips per group
        if PU_or_DO == "PU":
            df_day_pickup = df.groupby(["date_pickup", "PULocationID"]).agg({"trip_miles": ["mean"], "base_passenger_fare": ["mean"], "PULocationID": ["count"]})
            # rename count column to trip_count
            df_day_pickup.columns = ["trip_distance_mean", "base_fare_mean", "trip_number"]
        
        elif PU_or_DO == "DO":
            df_day_pickup = df.groupby(["date_pickup", "PULocationID"]).agg({"trip_miles": ["mean"], "base_passenger_fare": ["mean"], "DOLocationID": ["count"]})
            # rename count column to trip_count
            df_day_pickup.columns = ["trip_distance_mean", "base_fare_mean", "trip_number"]
    
        df_day_pickup = df_day_pickup.reset_index()
        # convert grouped dfs and climate date columns to date format
        df_day_pickup["date_pickup"] = pd.to_datetime(df_day_pickup["date_pickup"])
        climate_only["date_pickup"] = pd.to_datetime(climate_only["date_pickup"])
        
        # merge daily trip data with climate data on pickup date
        df_day_pickup = pd.merge(df_day_pickup, climate_only, on="date_pickup", how="left")

        # extract month and year for file naming
        month_year = df_day_pickup["date_pickup"].iloc[0].strftime("%Y-%m")
        if PU_or_DO == "DO":
            save_string = os.path.join(path, f"preprocessed_{PU_or_DO}/{month_year}.csv")
        else:
            save_string = os.path.join(path, f"preprocessed_{PU_or_DO}/{month_year}.csv")
        
        df_day_pickup.to_csv(save_string, index=False)
        
        # Progress counter (per year)
        progress = (i + 1) / 12 * 100
        print(f"Progress:{year} : {progress:.2f}%")



def merge_taxi_data(file_dir: str, output_file: str):
    """
    Merges all preprocessed CSV files (aggregated on the daily level) into a single DataFrame.

    Parameters:
        file_dir (str): The path to the directory containing the preprocessed CSV files.
        output_file (str): output path of the merged CSV file.

    Returns:
        csv file containing merged data for either green or yellow cabs
    
    """
    # Load all the processed files
    files_processed = os.listdir(file_dir)
    csv_files = [f for f in files_processed if f.endswith('.csv')]
    # Concatenate all the CSV files into one DataFrame
    taxi_data = pd.concat([pd.read_csv(os.path.join(file_dir, f)) for f in csv_files], ignore_index=True)

    # Write the concatenated DataFrame to a CSV file
    taxi_data.to_csv(output_file, index=False)



## 2. Specify paths 

path = "High_Volume_FHV"
processed_path = os.path.join(path, "preprocessed_DO")
output_path = os.path.join(path, "merged_grouped_DO.csv")


## 3. Run functions

start_time = time.time()
preprocess_cab_data(path, "DO")
end_time = time.time()

print(f"Execution time: {end_time - start_time:.2f} seconds")


merge_taxi_data(processed_path,output_path)

# pool_datasets()