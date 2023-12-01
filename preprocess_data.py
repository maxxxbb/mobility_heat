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

def preprocess_cab_data(path : str , dataset : str , PU_or_DO : str):
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
    if dataset == "Green":
        pickup_string = "lpep_pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = range(2014,2020)
    elif dataset == "Yellow":
        pickup_string = "tpep_pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = range(2014,2020)
    elif dataset == "FHV":
        pickup_string = "pickup_datetime"
        PU_location_string = "PUlocationID"
        DO_location_string = "DOlocationID"
        years = range(2015,2020)
    
    climate_only = preprocess_climate_data("Data/data_zcta_with_demographic_scaled_more_climate_tourism_aggregated_mitigation_hvi_radius_weighted_with_cheby.csv")

    for year in years :
        
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
            
            # extreacts date from pickup_date and stores in new columns
            df["date_pickup"] = df[pickup_string].dt.strftime("%Y-%m-%d")
            df["year"] = df[pickup_string].dt.year
            df["month"] = df[pickup_string].dt.month
            # only keep rows where monthyear from filename matches monthyear from pickup_date
            df = df[(df["year"] == int(year_month[0])) & (df["month"] == int(year_month[1]))]
            
            # remove trips to or from airport
            airporttrips = (df[PU_location_string].isin([1, 132, 138])) | (df[DO_location_string].isin([1, 132, 138]))
            df = df[~airporttrips]
            # create new dataframe for each month where trips are aggregated by date ( and PULocationID) and invlude the mean fare_amount and total amount per group while still including the number of trips per group
            if PU_or_DO == "PU":
                if dataset == "FHV":
                    df_day_pickup = df.groupby(["date_pickup", "PUlocationID"]).agg({"PUlocationID": ["count"]})
                    df_day_pickup.columns = ["trip_number"]
                else :
                    df_day_pickup = df.groupby(["date_pickup", "PULocationID"]).agg({"trip_distance": ["mean"], "total_amount": ["mean"], "PULocationID": ["count"]})
                    # rename count column to trip_count
                    df_day_pickup.columns = ["trip_distance_mean", "total_amount_mean", "trip_number"]
            
            elif PU_or_DO == "DO":
                if dataset == "FHV":
                    df_day_pickup = df.groupby(["date_pickup", "DOlocationID"]).agg({"DOlocationID": ["count"]})
                    df_day_pickup.columns = ["trip_number"]
                else :
                    df_day_pickup = df.groupby(["date_pickup", "DOLocationID"]).agg({"trip_distance": ["mean"], "total_amount": ["mean"], "DOLocationID": ["count"]})
                    # rename count column to trip_count
                    df_day_pickup.columns = ["trip_distance_mean", "total_amount_mean", "trip_number"]
        
            df_day_pickup = df_day_pickup.reset_index()
            # convert grouped dfs and climate date columns to date format
            df_day_pickup["date_pickup"] = pd.to_datetime(df_day_pickup["date_pickup"])
            climate_only["date_pickup"] = pd.to_datetime(climate_only["date_pickup"])
            
            # merge daily trip data with climate data on pickup date
            df_day_pickup = pd.merge(df_day_pickup, climate_only, on="date_pickup", how="left")

            # extract month and year for file naming
            month_year = df_day_pickup["date_pickup"].iloc[0].strftime("%Y-%m")
            
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


def pool_datasets():
    """
    Merges Green, Yellow and FHV data into a pooled dataset.

    Returns: saves pooled csv
    
    """

    Green = pd.read_csv('Green_Cab_data/merged_grouped.csv')
    Yellow = pd.read_csv('Yellow_Cab_data/merged_grouped.csv')
    FHV = pd.read_csv('For_Hire_Vehicle_data/merged_grouped.csv')

    FHV.rename(columns={'PUlocationID': 'PULocationID'}, inplace=True)
    # merge those three datasets on columns : PULocation and pickup_date

    # Green and Yellow
    Green_Yellow_FHV = pd.concat([Green,Yellow,FHV],ignore_index=True)

    # group data by PULocation and pickup_date where only PULocation is calculated as the sum of the two datasets and the rest of the columns are the first appeareance
    Green_Yellow_FHV = Green_Yellow_FHV.groupby(["PULocationID","date_pickup"]).agg({"trip_number": "sum", "fare_amount_mean": "mean", "total_amount_mean": "mean", 'tmax_obs' : "first", 'Weekday_index' : "first", 'pr_obs' : "first", 'season' : "first",
           'windspeed_obs': "first", 'Snowdepth' : "first"}).reset_index()

    # drop wrong data entries

    Green_Yellow_FHV = Green_Yellow_FHV[Green_Yellow_FHV["PULocationID"] != -1]

    # save as csv

    Green_Yellow_FHV.to_csv("pooled_grouped.csv",index=False)


## 2. Specify paths 

Dataset = "FHV"
PU_or_DO = "origin_destination"
path = "Data\High_Volume_FHV"
processed_path = os.path.join(path, f"preprocessed_{PU_or_DO}")
output_path = os.path.join(path, f"merged_grouped_{PU_or_DO}.csv")


## 3. Run functions


# preprocess_cab_data(path,Dataset , PU_or_DO)


start_time = time.time()
merge_taxi_data(processed_path,output_path)
end_time = time.time()

print(f"Execution time: {end_time - start_time:.2f} seconds")
# pool_datasets()