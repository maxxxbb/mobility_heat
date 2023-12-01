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

def preprocess_cab_data(path: str, dataset: str):
    """
    Groups taxitrip data by day and origin-destination pairs and merges with climate data on date.

    Args:
        path (str): The top-level directory where the parquet files are located.
        dataset (str): Green, Yellow or FHV data
        PU_or_DO (str): binary indicator whether trips should be aggregated by Pickup or Dropoff Location

    Returns:
        CSV file per month with daily trip data and climate data merged on date.
    """
    
    # Initialization
    if dataset == "Green":
        pickup_string = "lpep_pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = range(2014, 2020)
    elif dataset == "Yellow":
        pickup_string = "tpep_pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = range(2014, 2020)
    elif dataset == "FHV": # only available form 2015 onwards: 
        pickup_string = "pickup_datetime"
        PU_location_string = "PUlocationID"
        DO_location_string = "DOlocationID"
        years = range(2015, 2020)
    elif dataset == "FHVHV": 
        pickup_string = "pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = [2019]
    
    
    # Preprocess climate data (assuming this function exists and works correctly)
    climate_only = preprocess_climate_data("Data/data_zcta_with_demographic_scaled_more_climate_tourism_aggregated_mitigation_hvi_radius_weighted_with_cheby.csv")

    for year in years:
        year_path = os.path.join(path, str(year))

        # Loop over the files in year folders
        for i, file in enumerate(os.listdir(year_path)):
            file_path = os.path.join(year_path, file)
            df = fp.ParquetFile(file_path).to_pandas()

            # Date extraction
            df["date_pickup"] = df[pickup_string].dt.strftime("%Y-%m-%d")
            df["year"] = df[pickup_string].dt.year
            df["month"] = df[pickup_string].dt.month
            
            # Filtering by file date
            stop_char = len(file) - 8
            start_char = stop_char - 7
            year_month = file[start_char:stop_char].split("-")
            year_month[1] = str(int(year_month[1].lstrip("0")))
            df = df[(df["year"] == int(year_month[0])) & (df["month"] == int(year_month[1]))]
            
            # Remove airport trips
            airport_trips = (df[PU_location_string].isin([1, 132, 138])) | (df[DO_location_string].isin([1, 132, 138]))
            df = df[~airport_trips]

            # Grouping by date and origin-destination pairs: counts number 
            df_grouped = df.groupby(["date_pickup", PU_location_string, DO_location_string]).size().reset_index(name='trip_count')

            # Convert grouped df and climate date columns to date format
            df_grouped["date_pickup"] = pd.to_datetime(df_grouped["date_pickup"])
            climate_only["date_pickup"] = pd.to_datetime(climate_only["date_pickup"])
            
            # Merge with climate data
            df_merged = pd.merge(df_grouped, climate_only, on="date_pickup", how="left")

            # Save to CSV
            month_year = df_merged["date_pickup"].iloc[0].strftime("%Y-%m")
            save_string = os.path.join(path, f"preprocessed_origin_destination/{month_year}.csv")
            df_merged.to_csv(save_string, index=False)
            
            # Progress counter
            progress = (i + 1) / 12 * 100
            print(f"Progress: {year} : {progress:.2f}%")



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



## 2. Run Functions



## 3. Run functions



""" # Preprocess green cab data
start_time = time.time()
preprocess_cab_data("Data/Green_Cab_data", "Green")
end_time = time.time()


# Preprocess yellow cab data
start_time = time.time()
preprocess_cab_data("Data/Yellow_Cab_data", "Yellow")
end_time = time.time()

# preprocess FHV data
start_time = time.time()
preprocess_cab_data("Data/For_Hire_Vehicle_data", "FHV")
end_time = time.time() """

start_time = time.time()
preprocess_cab_data("Data/High_Volume_FHV", "FHVHV")
end_time = time.time()


print(f"Execution time: {end_time - start_time:.2f} seconds")