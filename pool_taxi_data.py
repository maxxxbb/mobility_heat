import pandas as pd
import numpy as np
import datetime as dt


def pool_all_datasets_PU(yellow_cab_PU, green_cab_PU,fhv_PU, hvfhv_PU , yellow_green = False, fhv_only = False):
    """
    Pools grouped individual taxi datasets into one dataset.

    Parameters:
    - yellow_cab_PU: A pandas DataFrame containing Yellow Cab pickup data.
    - green_cab_PU: A pandas DataFrame containing Green Cab pickup data.
    - fhv_PU: A pandas DataFrame containing FHV pickup data. (no information on trip_distance , ...)
    - hvfhv_:PU: A pandas DataFrame containing FHV pickup data with 2019 HVFHV data added.
    - yellow_green: A boolean indicating whether to pool only yellow and green cab data.
    - fhv_only: A boolean indicating whether to pool only fhv data.

    Returns:
    - A pandas DataFrame with all input data pooled and aggregated at Pickup_Location Level.
    """

    hvfhv_PU.rename(columns={'base_fare_mean': 'total_amount_mean'}, inplace=True)
    fhv_PU.rename(columns={'PUlocationID': 'PULocationID'}, inplace=True)

    if yellow_green == True :

        combined_df = pd.concat([yellow_cab_PU, green_cab_PU])

        # For weighted average of trip distance and total amount column

        combined_df["weighted_mean_distance"] = combined_df["trip_number"] * combined_df["trip_distance_mean"]
        combined_df["weighted_mean_amount"] = combined_df["trip_number"] * combined_df["total_amount_mean"]
        combined_df["weighted_perc_daytime"] = combined_df["trip_number"] * combined_df["daytime_perc"]


        
        pooled_trips = combined_df.groupby(['date_pickup', 'PULocationID'], as_index=False).agg({
            'trip_number': 'sum',
            'weighted_mean_distance': 'sum',
            'weighted_mean_amount': 'sum',
            'weighted_perc_daytime': 'sum'
            
        })

        pooled_trips["trip_distance_mean"] = pooled_trips["weighted_mean_distance"] / pooled_trips["trip_number"]
        pooled_trips["total_amount_mean"] = pooled_trips["weighted_mean_amount"] / pooled_trips["trip_number"]
        pooled_trips["daytime_perc"] = pooled_trips["weighted_perc_daytime"] / pooled_trips["trip_number"]

        # drop weighted columns
        pooled_trips.drop(columns=['weighted_mean_distance', 'weighted_mean_amount', 'weighted_perc_daytime'], inplace=True)
    
    elif fhv_only == True :

        combined_df = pd.concat([fhv_PU, hvfhv_PU])

        # For weighted average of trip distance and total amount column

        combined_df["weighted_mean_distance"] = combined_df["trip_number"] * combined_df["trip_distance_mean"]
        combined_df["weighted_mean_amount"] = combined_df["trip_number"] * combined_df["total_amount_mean"]
        combined_df["weighted_perc_daytime"] = combined_df["trip_number"] * combined_df["daytime_perc"]



        
        pooled_trips = combined_df.groupby(['date_pickup', 'PULocationID'], as_index=False).agg({
            'trip_number': 'sum',
            'weighted_mean_distance': 'sum',
            'weighted_mean_amount': 'sum',
            'weighted_perc_daytime': 'sum'
            
        })

        pooled_trips["trip_distance_mean"] = pooled_trips["weighted_mean_distance"] / pooled_trips["trip_number"]
        pooled_trips["total_amount_mean"] = pooled_trips["weighted_mean_amount"] / pooled_trips["trip_number"]
        pooled_trips["daytime_perc"] = pooled_trips["weighted_perc_daytime"] / pooled_trips["trip_number"]

        # drop weighted columns
        pooled_trips.drop(columns=['weighted_mean_distance', 'weighted_mean_amount', 'weighted_perc_daytime'], inplace=True)
    
    else:
        
        # Concatenate the dataframes into one
        combined_df = pd.concat([yellow_cab_PU, green_cab_PU, fhv_PU, hvfhv_PU])

        # For weighted average of trip distance and total amount column

        combined_df["weighted_mean_distance"] = combined_df["trip_number"] * combined_df["trip_distance_mean"]
        combined_df["weighted_mean_amount"] = combined_df["trip_number"] * combined_df["total_amount_mean"]
        combined_df["weighted_perc_daytime"] = combined_df["trip_number"] * combined_df["daytime_perc"]


        
        pooled_trips = combined_df.groupby(['date_pickup', 'PULocationID'], as_index=False).agg({
            'trip_number': 'sum',
            'weighted_mean_distance': 'sum',
            'weighted_mean_amount': 'sum',
            'weighted_perc_daytime': 'sum'
            
        })

        pooled_trips["trip_distance_mean"] = pooled_trips["weighted_mean_distance"] / pooled_trips["trip_number"]
        pooled_trips["total_amount_mean"] = pooled_trips["weighted_mean_amount"] / pooled_trips["trip_number"]
        pooled_trips["daytime_perc"] = pooled_trips["weighted_perc_daytime"] / pooled_trips["trip_number"]

        # drop weighted columns
        pooled_trips.drop(columns=['weighted_mean_distance', 'weighted_mean_amount', 'weighted_perc_daytime'], inplace=True)
    
    
    return pooled_trips

def pool_all_datasets_DO(yellow_cab_DO, green_cab_DO,fhv_DO, hvfhv_DO , yellow_green = False, fhv_only = False):
    """
    Pools grouped individual taxi datasets into one dataset.

    Parameters:
    - yellow_cab_DO: A pandas DataFrame containing Yellow Cab dropoff data.
    - green_cab_DO: A pandas DataFrame containing Green Cab dropoff data.
    - fhv_DO: A pandas DataFrame containing FHV dropoff data. (no information on trip_distance , ...)
    - hvfhv_DO: A pandas DataFrame containing FHV dropoff data with 2019 HVFHV data added.
    - yellow_green: A boolean indicating whether to pool only yellow and green cab data.
    - fhv_only: A boolean indicating whether to pool only fhv data.

    Returns:
    - A pandas DataFrame with all input data pooled and aggregated at Dropoff Location Level.
    """

    hvfhv_DO.rename(columns={'base_fare_mean': 'total_amount_mean'}, inplace=True)
    fhv_DO.rename(columns={'DOlocationID': 'DOLocationID'}, inplace=True)

    if yellow_green == True :

        combined_df = pd.concat([yellow_cab_DO, green_cab_DO])

        # For weighted average of trip distance and total amount column

        combined_df["weighted_mean_distance"] = combined_df["trip_number"] * combined_df["trip_distance_mean"]
        combined_df["weighted_mean_amount"] = combined_df["trip_number"] * combined_df["total_amount_mean"]
        combined_df["weighted_perc_daytime"] = combined_df["trip_number"] * combined_df["daytime_perc"]


        
        pooled_trips = combined_df.groupby(['date_pickup', 'DOLocationID'], as_index=False).agg({
            'trip_number': 'sum',
            'weighted_mean_distance': 'sum',
            'weighted_mean_amount': 'sum',
            'weighted_perc_daytime': 'sum'
            
        })

        pooled_trips["trip_distance_mean"] = pooled_trips["weighted_mean_distance"] / pooled_trips["trip_number"]
        pooled_trips["total_amount_mean"] = pooled_trips["weighted_mean_amount"] / pooled_trips["trip_number"]
        pooled_trips["daytime_perc"] = pooled_trips["weighted_perc_daytime"] / pooled_trips["trip_number"]

        # drop weighted columns
        pooled_trips.drop(columns=['weighted_mean_distance', 'weighted_mean_amount', 'weighted_perc_daytime'], inplace=True)

    
    
    elif fhv_only == True :


        # exlude any obersvations where date pickup is before June 2017 - data before
        fhv_DO_filtered = fhv_DO[fhv_DO["date_pickup"] >= "2017-06-01"]
        combined_df = pd.concat([fhv_DO_filtered, hvfhv_DO])

        # For weighted average of trip distance and total amount column
        

        combined_df["weighted_mean_distance"] = combined_df["trip_number"] * combined_df["trip_distance_mean"]
        combined_df["weighted_mean_amount"] = combined_df["trip_number"] * combined_df["total_amount_mean"]
        combined_df["weighted_perc_daytime"] = combined_df["trip_number"] * combined_df["daytime_perc"]
        
        pooled_trips = combined_df.groupby(['date_pickup', 'DOLocationID'], as_index=False).agg({
            'trip_number': 'sum',
            'weighted_mean_distance': 'sum',
            'weighted_mean_amount': 'sum',
            'weighted_perc_daytime': 'sum'
            
        })

        pooled_trips["trip_distance_mean"] = pooled_trips["weighted_mean_distance"] / pooled_trips["trip_number"]
        pooled_trips["total_amount_mean"] = pooled_trips["weighted_mean_amount"] / pooled_trips["trip_number"]
        pooled_trips["daytime_perc"] = pooled_trips["weighted_perc_daytime"] / pooled_trips["trip_number"]

        # drop weighted columns
        pooled_trips.drop(columns=['weighted_mean_distance', 'weighted_mean_amount', 'weighted_perc_daytime'], inplace=True)
    
    else:
        
        # Concatenate the dataframes into one
       
        fhv_DO_filtered = fhv_DO[fhv_DO["date_pickup"] >= "2017-06-01"]
       
        combined_df = pd.concat([yellow_cab_DO, green_cab_DO, fhv_DO_filtered, hvfhv_DO])

        # For weighted average of trip distance and total amount column

        combined_df["weighted_mean_distance"] = combined_df["trip_number"] * combined_df["trip_distance_mean"]
        combined_df["weighted_mean_amount"] = combined_df["trip_number"] * combined_df["total_amount_mean"]
        combined_df["weighted_perc_daytime"] = combined_df["trip_number"] * combined_df["daytime_perc"]
    


        
        pooled_trips = combined_df.groupby(['date_pickup', 'DOLocationID'], as_index=False).agg({
            'trip_number': 'sum',
            'weighted_mean_distance': 'sum',
            'weighted_mean_amount': 'sum',
            'weighted_perc_daytime': 'sum'
            
        })

        pooled_trips["trip_distance_mean"] = pooled_trips["weighted_mean_distance"] / pooled_trips["trip_number"]
        pooled_trips["total_amount_mean"] = pooled_trips["weighted_mean_amount"] / pooled_trips["trip_number"]
        pooled_trips["daytime_perc"] = pooled_trips["weighted_perc_daytime"] / pooled_trips["trip_number"]

        # drop weighted columns
        pooled_trips.drop(columns=['weighted_mean_distance', 'weighted_mean_amount', 'weighted_perc_daytime'], inplace=True)
    
    
    return pooled_trips

def pool_all_dataset_OD(yellow_cab_OD, green_cab_OD,fhv_OD, hvfhv_OD , yellow_green = False, fhv_only = False):
    """
    Pools grouped individual taxi datasets into one dataset.

    Parameters:
    - yellow_cab_PU: A pandas DataFrame containing Yellow Cab pickup data.
    - green_cab_PU: A pandas DataFrame containing Green Cab pickup data.
    - fhv_PU: A pandas DataFrame containing FHV pickup data. (no information on trip_distance , ...)
    - hvfhv_:PU: A pandas DataFrame containing FHV pickup data with 2019 HVFHV data added.
    - yellow_green: A boolean indicating whether to pool only yellow and green cab data.
    - fhv_only: A boolean indicating whether to pool only fhv data.

    Returns:
    - A pandas DataFrame with all input data pooled and aggregated at Pickup_Location Level.
    """
    fhv_OD.rename(columns={'PUlocationID': 'PULocationID', "DOlocationID" : "DOLocationID"}, inplace=True)
    fhv_OD_filtered = fhv_OD[fhv_OD['date_pickup'] >= '2017-06-01']


    if yellow_green == True :

        combined_df = pd.concat([yellow_cab_OD, green_cab_OD])

        # For weighted average of trip distance and total amount column

        
        pooled_trips = combined_df.groupby(['date_pickup','PULocationID','DOLocationID']).agg({'trip_number':'sum'}).reset_index()
    
    
    elif fhv_only == True :

        combined_df = pd.concat([fhv_OD_filtered, hvfhv_OD])

        pooled_trips = combined_df.groupby(['date_pickup','PULocationID','DOLocationID']).agg({'trip_number':'sum'}).reset_index()
    
    else:
        
        # Concatenate the dataframes into one
        combined_df = pd.concat([yellow_cab_OD, green_cab_OD, fhv_OD_filtered, hvfhv_OD])

        # For weighted average of trip distance and total amount column

        pooled_trips = combined_df.groupby(['date_pickup','PULocationID','DOLocationID']).agg({'trip_number':'sum'}).reset_index()
    
    
    return pooled_trips

    
# 2. Read inputs
daytime = ""
# 2.1 Pickup
 
yellow_cab_PU = pd.read_csv(f'Yellow_Cab_data/merged_grouped_PU.csv')
green_cab_PU = pd.read_csv(f'Green_Cab_data/merged_grouped_PU.csv')
fhv_PU = pd.read_csv(f'For_Hire_Vehicle_data/merged_grouped_PU.csv')
hvfhv_PU = pd.read_csv(f'High_Volume_FHV/merged_grouped_PU.csv')

# 2.2 Dropoff
yellow_cab_DO = pd.read_csv(f'Yellow_Cab_data/merged_grouped_DO.csv')
green_cab_DO = pd.read_csv(f'Green_Cab_data/merged_grouped_DO.csv')
fhv_DO = pd.read_csv(f'For_Hire_Vehicle_data/merged_grouped_DO.csv')
hvfhv_DO = pd.read_csv(f'High_Volume_FHV/merged_grouped_DO.csv')

# 2.3 OD
yellow_cab_OD = pd.read_csv('Yellow_Cab_data/merged_grouped_origin_destination.csv')
green_cab_OD = pd.read_csv('Green_Cab_data/merged_grouped_origin_destination.csv')
fhv_OD = pd.read_csv('For_Hire_Vehicle_data/merged_grouped_origin_destination.csv')
hvfhv_OD = pd.read_csv('High_Volume_FHV/merged_grouped_origin_destination.csv')




# 4. Pool datasets

for subset in ["FHV","YG","all"]:
    for level in ["PU", "DO"]:
                #["PU", "DO", "OD"]  
        if subset == "FHV":
            fhv_only = True
            yellow_green = False
        elif subset == "YG":
            yellow_green = True
            fhv_only = False
        else:
            fhv_only = False
            yellow_green = False
        
        if level == "PU":
            pooled_trips_PU = pool_all_datasets_PU(yellow_cab_PU, green_cab_PU, fhv_PU , hvfhv_PU , yellow_green = yellow_green, fhv_only = fhv_only)
            pooled_trips_PU.to_csv(f'Pooled_data/PU/data_grouped_{subset}_PU.csv', index=False)
        
        elif level == "DO":   
            pooled_trips_DO = pool_all_datasets_DO(yellow_cab_DO, green_cab_DO, fhv_DO , hvfhv_DO , yellow_green = yellow_green, fhv_only = fhv_only)
            pooled_trips_DO.to_csv(f'Pooled_data/DO/data_grouped_{subset}_DO.csv', index=False)
        
        else:
            pooled_trips_OD = pool_all_dataset_OD(yellow_cab_OD, green_cab_OD, fhv_OD , hvfhv_OD , yellow_green = yellow_green, fhv_only = fhv_only)
            pooled_trips_OD.to_csv(f'Pooled_data/OD/data_grouped_{subset}_OD.csv', index=False)



# 5. Clear memory
# del yellow_cab_PU, green_cab_PU, fhv_PU, hvfhv_PU, pooled_trips_PU, pooled_trips_DO, yellow_cab_DO, green_cab_DO, fhv_DO, hvfhv_DO , pooled_trips_OD, yellow_cab_OD, green_cab_OD, fhv_OD, hvfhv_OD