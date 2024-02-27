import datetime as dt
import os
import time
import fastparquet as fp
import pandas as pd


def preprocess_cab_data(path : str , PU_or_DO : str):
    """
    Groups taxitrip data by day.

    Args:
        path (str): The top-level directory where the parquet files are located.
        dataset (str) : Green, Yellow or FHV data
        PU_or_DO (str) : binary indicator whether trips should be aggregated by Pickup or Dropoff Location

    Returns:
        csv file per month with daily trip data and climate data merged on date.
    """
    # empty list to keep track of done files
    # if dataset green pickup string "lpep" if yellow pickup string "tpep" , if fhv = "Pickup_datetime"
    if path == "Green_Cab_data":
        pickup_string = "lpep_pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = range(2014,2020)
    elif path == "Yellow_Cab_data":
        pickup_string = "tpep_pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = range(2014,2020)
    elif path == "For_Hire_Vehicle_data":
        pickup_string = "pickup_datetime"
        PU_location_string = "PUlocationID"
        DO_location_string = "DOlocationID"
        years = range(2015,2020)
    elif path == "High_Volume_FHV":
        pickup_string = "pickup_datetime"
        PU_location_string = "PULocationID"
        DO_location_string = "DOLocationID"
        years = [2019]
    
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

            # remove trips with negative trip distance and trip distances over 200 if trip distance column exists- remove faulty trips
            if "trip_distance" in df.columns:
                df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 200)]
            if "trip_miles" in df.columns:
                df = df[(df["trip_miles"] > 0) & (df["trip_miles"] <= 200)]
            
            # remove trips with unrealistiv total amount 
            if "total_amount" in df.columns:
                df = df[(df["total_amount"] > 0) & (df["total_amount"] <= 1000)]
            
            if "base_passenger_fare" in df.columns:
                df = df[(df["base_passenger_fare"] > 0) & (df["base_passenger_fare"] <= 1000)]

            
            # create indicator one if a trip is during daytime
            df["daytime"] = 0
            df.loc[(df[pickup_string].dt.hour >= 8) & (df[pickup_string].dt.hour <= 20), "daytime"] = 1
            
            # create dataframe for each month where trips are aggregated by date and zone:
            
            
            if PU_or_DO == "PU":
                if path == "For_Hire_Vehicle_data":
                    df_day_pickup = df.groupby(["date_pickup", "PUlocationID"]).agg({"PUlocationID": ["count"], "daytime" : ["mean"]})
                    df_day_pickup.columns = ["trip_number" , "daytime_perc"]
                elif path == "High_Volume_FHV":
                    df_day_pickup = df.groupby(["date_pickup", "PULocationID"]).agg({"trip_miles": ["mean"], "base_passenger_fare": ["mean"], "PULocationID": ["count"], "daytime" : ["mean"]})
                    df_day_pickup.columns = ["trip_distance_mean", "base_fare_mean", "trip_number" , "daytime_perc"]
                else :
                    df_day_pickup = df.groupby(["date_pickup", "PULocationID"]).agg({"trip_distance": ["mean"], "total_amount": ["mean"], "PULocationID": ["count"], "daytime" : ["mean"]})
                    # rename count column to trip_count
                    df_day_pickup.columns = ["trip_distance_mean", "total_amount_mean", "trip_number" , "daytime_perc"]
                
            
            elif PU_or_DO == "DO":
                if path == "For_Hire_Vehicle_data":
                    df_day_pickup = df.groupby(["date_pickup", "DOlocationID"]).agg({"DOlocationID": ["count"], "daytime" : ["mean"]})
                    df_day_pickup.columns = ["trip_number", "daytime_perc"]
                
                elif path == "High_Volume_FHV":
                    df_day_pickup = df.groupby(["date_pickup", "DOLocationID"]).agg({"trip_miles": ["mean"], "base_passenger_fare": ["mean"], "DOLocationID": ["count"], "daytime" : ["mean"]})
                    df_day_pickup.columns = ["trip_distance_mean", "base_fare_mean", "trip_number", "daytime_perc"]
                else :
                    df_day_pickup = df.groupby(["date_pickup", "DOLocationID"]).agg({"trip_distance": ["mean"], "total_amount": ["mean"], "DOLocationID": ["count"] , "daytime" : ["mean"]})
                    df_day_pickup.columns = ["trip_distance_mean", "total_amount_mean", "trip_number" , "daytime_perc"]

            # get rid of multiindex
            df_day_pickup = df_day_pickup.reset_index()
            df_day_pickup["date_pickup"] = pd.to_datetime(df_day_pickup["date_pickup"])

            # extract month and year for file naming
            month_year = df_day_pickup["date_pickup"].iloc[0].strftime("%Y-%m")
            
            save_string = os.path.join(path, f"preprocessed_{PU_or_DO}/{month_year}.csv")
            
            df_day_pickup.to_csv(save_string, index=False)
            
            # Progress counter (per year)
            progress = (i + 1) / 12 * 100
            print(f"Progress:{month_year} : done")
            print(f"Progress:{year} : {progress:.2f}%")



def concat_taxi_data(file_dir: str, output_file: str ):
    """
    Concatenates all preprocessed CSV files (aggregated on the daily level) into a single DataFrame.

    Parameters:
        file_dir (str): The path to the directory containing the preprocessed CSV files.
        output_file (str): output path of the merged CSV file.

    Returns:
        csv file containing merged data for either green or yellow cabs
    
    """
    # Load processed files
    files_processed = os.listdir(file_dir)
    csv_files = [f for f in files_processed if f.endswith('.csv')]
    # Concatenate CSV files into one DataFrame
    taxi_data = pd.concat([pd.read_csv(os.path.join(file_dir, f)) for f in csv_files], ignore_index=True)

   
    taxi_data.to_csv(output_file, index=False)


def get_daily_data(paths, levels):
    """
    Output: saves csv files with daily data over all years for of the taxidatasets

    Input: 

    paths: list of paths to the taxidatasets (str)
    levels: list of levels to aggregate on (PU or DO) (str)


    """

    ## 3. Preprocess (Group by Day-Zone) and concatenate monthly preprocessed csv
    for path in paths:
        for level in levels:
        
            
            start_time = time.time()
            preprocess_cab_data(path, level)
            end_time = time.time()

            print(f"Execution time {path} processing: {end_time - start_time:.2f} seconds")
            
            processed_path = os.path.join(path, f"preprocessed_{level}")
            output_path = os.path.join(path, f"merged_grouped_{level}.csv")
            start_time = time.time()
            concat_taxi_data(processed_path,output_path)
            end_time = time.time()

            print(f"Execution time {path} {level} merging: {end_time - start_time:.2f} seconds")


#paths = ["Green_Cab_data", "Yellow_Cab_data", "High_Volume_FHV" , "For_Hire_Vehicle_data"]
paths = ["For_Hire_Vehicle_data"]


levels = ["PU", "DO"]


get_daily_data(paths = paths , levels = levels)
