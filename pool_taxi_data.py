import pandas as pd
import numpy as np
import datetime as dt


def weighted_mean_distance(group):
    # Filter out rows where 'trip_distance_mean' is NaN
    valid_rows = group[~group['trip_distance_mean'].isna()]
    d = valid_rows['trip_distance_mean']
    w = valid_rows['trip_number']
    if w.sum() == 0:
        return np.nan
    return (d * w).sum() / w.sum()

def weighted_mean_amount(group):
    # Filter out rows where 'total_amount_mean' is NaN
    valid_rows = group[~group['total_amount_mean'].isna()]
    d = valid_rows['total_amount_mean']
    w = valid_rows['trip_number']
    if w.sum() == 0:
        return np.nan
    return (d * w).sum() / w.sum()

def weighted_mean_tip(group):
    # Filter out rows where 'tip_amount_mean' is NaN
    valid_rows = group[~group['tip_amount_mean'].isna()]
    d = valid_rows['tip_amount_mean']
    w = valid_rows['trip_number']
    if w.sum() == 0:
        return np.nan
    return (d * w).sum() / w.sum()

def add_2019_hvf_data(fhv_PU, hvfhv_PU):
    """
    Add 2019 High Volume For-Hire Vehicle (HVFHV) data to the For-Hire Vehicle (FHV) data.

    Parameters:
    - fhv_PU: A pandas DataFrame containing FHV pickup data.
    - hvfhv_PU: A pandas DataFrame containing HVFHV pickup data.

    Returns:
    - A pandas DataFrame with HVFHV data added to FHV data and additional calculated columns.
    """
    # Correct the year factor of high volume data
    hvfhv_PU["Year_fact"] = 5
    fhv_PU_with_2019 = pd.concat([fhv_PU, hvfhv_PU], ignore_index=True)
    
    
    # Calculate the number of unique days
    num_days = len(fhv_PU_with_2019["date_pickup"].unique())

    # Initialize Chebyshev polynomials
    fhv_PU_with_2019['cheby_0'] = 1
    fhv_PU_with_2019['cheby_1'] = fhv_PU_with_2019['date_pickup'].rank(method='dense').astype(int) / num_days

    # Recursively defining other Chebyshev polynomials for each day until 5th order
    for i in range(2, 6):
        fhv_PU_with_2019[f"cheby_{i}"] = (2 * fhv_PU_with_2019["cheby_1"] * fhv_PU_with_2019[f"cheby_{i-1}"]) - fhv_PU_with_2019[f"cheby_{i-2}"]

    # Redefine temperature bins to coincide with other datasets: max and min temperature were taken from yellow cab dates
    sequence_bins = np.arange(np.floor(-10.55), np.ceil(36.11) + 1, 3)
    temp_bins = pd.cut(fhv_PU_with_2019['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered=True)
    fhv_PU_with_2019['temp_bins'] = temp_bins

    return fhv_PU_with_2019


def pool_datasets(yellow_cab_PU, green_cab_PU, fhv_PU_with_2019):
    """
    Pool multiple datasets into a single dataset.

    Parameters:
    - yellow_cab_PU: A pandas DataFrame containing Yellow Cab pickup data.
    - green_cab_PU: A pandas DataFrame containing Green Cab pickup data.
    - fhv_PU_with_2019: A pandas DataFrame containing FHV pickup data with 2019 HVFHV data added.

    Returns:
    - A pandas DataFrame with all input data pooled and aggregated at Pickup_Location Level.
    """
    # Concatenate the dataframes into one
    combined_df = pd.concat([yellow_cab_PU, green_cab_PU, fhv_PU_with_2019])

    # Group by 'date_pickup' and 'PULocationID' and aggregate the data
    pooled_trips = combined_df.groupby(['date_pickup', 'PULocationID'], as_index=False).agg({
        'trip_distance_mean': weighted_mean_distance,  
        'total_amount_mean': weighted_mean_amount,   
        'trip_number': 'sum',
        # Assuming other columns are the same for each group, take 'first'
        **{col: 'first' for col in combined_df.columns if col not in ['date_pickup', 'PULocationID', 
                                                                      'trip_distance_mean', 'total_amount_mean', 
                                                                      'trip_number']}
    })

    # Recalculate Chebyshev polynomials and year factors
    num_days = len(pooled_trips["date_pickup"].unique())
    pooled_trips['Year_fact'] = pd.factorize(pd.to_datetime(pooled_trips['date_pickup']).dt.year)[0] + 1
    pooled_trips['cheby_0'] = 1
    pooled_trips['cheby_1'] = pooled_trips['date_pickup'].rank(method='dense').astype(int) / num_days

    for i in range(2, 6):
        pooled_trips[f"cheby_{i}"] = (2 * pooled_trips["cheby_1"] * pooled_trips[f"cheby_{i-1}"]) - pooled_trips[f"cheby_{i-2}"]

    pooled_trips["log_total"] = np.log(pooled_trips["trip_number"] + 1)

    return pooled_trips


# 2. Read inputs

yellow_cab_PU = pd.read_csv('Yellow_Cab_data/data_regression_PU.csv')
green_cab_PU = pd.read_csv('Green_Cab_data/data_regression_PU.csv')
fhv_PU = pd.read_csv('For_Hire_Vehicle_data/data_regression_PU.csv')
hvfhv_PU = pd.read_csv('High_Volume_FHV/data_regression_PU.csv')


# 3. Add 2019 HVFHV data to FHV data

fhv_PU_with_2019 = add_2019_hvf_data(fhv_PU, hvfhv_PU)

fhv_PU_with_2019.to_csv('For_Hire_Vehicle_data/data_regression_PU_with_HV.csv', index=False)

# 4. Pool datasets

pooled_trips = pool_datasets(yellow_cab_PU, green_cab_PU, fhv_PU_with_2019)

pooled_trips.to_csv('Pooled_data/data_regression_PU.csv', index=False)

# 5. Clear memory
# del yellow_cab_PU, green_cab_PU, fhv_PU, hvfhv_PU, fhv_PU_with_2019, pooled_trips 