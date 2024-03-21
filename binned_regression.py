import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
from linearmodels.panel import PanelOLS
from matplotlib.gridspec import GridSpec
import statsmodels.api as sm
import statsmodels.formula.api as smf




def binned_regression_data(level, temp_bin_size, subset = False, income_split = None , workday_split = "None" , exclude_minimum_bin = False , daytime = "all" , hotel_control = False , exclude_zeros = False , temp_split = None):
    """

    Prepares data for binned regression analysis with several additional options
    
    
    
    level (str): "PU" or "DO" - Pickup or Dropoff Location level

    subset (str): "Yellow_Green" ,"all", "FHV" - only for subset analysis

    temp_bin_size(int): size of temperature bins in °C

    income_split(str): "upper" or "lower" - only for median split

    workday_split(str): "workday" or "weekend" - only for workday split . "None" if no split

    exclude_minimum_bin(bool): True if temperature bins with less than 1% of total days should be excluded

    dropunknown(bool): True if rows with PULocationID == 264 or DOLocationID == 265 (unknown taxi zones)

    daytime(str): "day" or "" - If "day" only trips between 8am and 8 pm are used in the analysis.

    """

    ## 1. DATA PREPARATION

    
    taxi_data_cut = pd.read_csv(f'Data/Pooled_data/{level}/final/final_data_{subset}_{level}.csv')
    
    if subset == "YG":
            # drop observations where Year_fact == 1: 2014
            taxi_data_cut = taxi_data_cut[taxi_data_cut['Year_fact'] != 1]
    
    elif subset == "FHV" and level == "DO":
            # drop observations where Year_fact == 1 : 2017 DO Location only available for half the year
            taxi_data_cut = taxi_data_cut[taxi_data_cut['Year_fact'] != 1]

    
    ## 1.1 Load covariates

    # taxi zone shapefile for borough and community district    
    taxi_zones = pd.read_csv("Data/Shapefiles/taxi+_zone_lookup.csv")
    taxi_data_cut = pd.merge(taxi_data_cut,taxi_zones[["LocationID", "Borough" , "community_district"]], left_on= f"{level}LocationID" , right_on= "LocationID" , how = "left")
    # daily weather measures
    climate_new = pd.read_csv("Data/NYC_weather/climate_NYC_with_humidity.csv")
    # monthly hotel occupancy
    monthly_hotel = pd.read_excel("Data/NYC_weather/NYC_monthly_hotel.xlsx")
    # socioeconomic covariates ACS
    covariates = pd.read_csv('Data/ACS_data/taxi_zones_ACS_parks_beaches_deviation.csv')

    

    # 1.2 Add: Monthly hotel occupancy and humidity measures:
    taxi_data_cut['Year_Month'] = pd.to_datetime(taxi_data_cut['date_pickup']).dt.to_period('M')
    monthly_hotel['Year_Month'] = pd.to_datetime(monthly_hotel['Year_Month'], format='%Y-%m').dt.to_period('M')
    taxi_data_cut = pd.merge(taxi_data_cut, monthly_hotel, on='Year_Month', how='inner', )
    taxi_data_cut = pd.merge(taxi_data_cut,climate_new[["DATE", "daylight_time", "DailyAverageRelativeHumidity" , "DailyAverageWetBulbTemperature"]], left_on= "date_pickup" , right_on= "DATE" , how = "left")
    taxi_data_cut = pd.merge(taxi_data_cut, covariates, left_on=f'{level}LocationID', right_on='LocationID', how='left')

    
    # 1.3 If option: Only include daytime trips: 8am to 8pm

    if daytime == "day":
            # only include trips from 8 to 10 
    
            taxi_data_cut['daytime_perc'] = taxi_data_cut['daytime_perc'].fillna(0)
            taxi_data_cut["trip_number"] = taxi_data_cut["trip_number"]*taxi_data_cut["daytime_perc"]
            taxi_data_cut["log_total"] = np.log(taxi_data_cut["trip_number"]+1)
    
    
    # 1.4 Drop zones with little coverage
    # 1.5 sum column zero_trips by PULocationID
    taxi_data_cut_zeros = taxi_data_cut.groupby([f'{level}LocationID']).agg({'zero_trips': 'sum'}).reset_index()
    # 1.6 get IDs of all Locations where days with zero trips are more than 1000
    non_covered_zones = list(taxi_data_cut_zeros[taxi_data_cut_zeros['zero_trips'] > 1000][f'{level}LocationID']) + [264,265]

    taxi_data_cut = taxi_data_cut[~taxi_data_cut[f'{level}LocationID'].isin(non_covered_zones)]

    # 1.7 Rearrange bins into 3 degree
            
    sequence_bins = np.arange(-10, 41, temp_bin_size)
    temp_bins = pd.cut(taxi_data_cut['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered = True)
    # Add days in 35-38 degree bin (little coverage) to 32-35 degree bin
    temp_bins = temp_bins.apply(lambda x: pd.Interval(32.0, 35.0, closed='right') if x == pd.Interval(35.0, 38.0, closed='right') else x)        
    taxi_data_cut['temp_bins'] = temp_bins
    
    # 1.7.1 Option: Exclude bins with less than 1% of total days

    if exclude_minimum_bin == True:
            unique_days = taxi_data_cut[['date_pickup', 'temp_bins']].drop_duplicates()

            sum_days = unique_days['temp_bins'].value_counts().sum()

            value_counts = unique_days['temp_bins'].value_counts()

            # drop bins with less than 1 % of total days from taxi_data_cut
            droplist = value_counts[value_counts < 0.01 * sum_days].index.tolist()

            # drop all rows with temp_bins_2 in droplist
            taxi_data_cut = taxi_data_cut[~taxi_data_cut['temp_bins'].isin(droplist)]
    
            # exclude days witt less than - 7 degrees
            taxi_data_cut = taxi_data_cut[taxi_data_cut['tmax_obs'] > -7]
            

    taxi_data_cut['temp_bins'] = pd.Categorical(taxi_data_cut['temp_bins'], ordered=False).astype(str)
    # rename bins so linearmodels can handle them
    taxi_data_cut['temp_bins'] = taxi_data_cut['temp_bins'].str.replace('\(', '[', regex=True)


    # 1.8 Option: Sample splits

    # 1.8.1 Median Income
    median_income = covariates['medincome'].median()

    # get all location IDs where medincome is above median and all where medincome is below median
    medincome_above = covariates[covariates['medincome'] > median_income]
    medincome_below = covariates[covariates['medincome'] <= median_income]

    # only keep the list of location IDs
    medincome_above_IDs= medincome_above['LocationID'].tolist()
    medincome_below_IDs= medincome_below['LocationID'].tolist()

    income_75 = covariates['medincome'].quantile(0.75)
    income_50 = covariates['medincome'].quantile(0.5)
    income_25 = covariates['medincome'].quantile(0.25)

    # get all location IDs where medincome is above median and all where medincome is below median
    above_75 = covariates[covariates['medincome'] > income_75]
    above_50 = covariates[(covariates['medincome'] > income_50) & (covariates['medincome'] <= income_75)]
    above_25 = covariates[(covariates['medincome'] > income_25) & (covariates['medincome'] <= income_50)]
    below_25 = covariates[covariates['medincome'] <= income_25]

    above_75_IDs = above_75['LocationID'].tolist()
    below_25_IDs = below_25['LocationID'].tolist()
    above_25_IDs = above_25['LocationID'].tolist()
    above_50_IDs = above_50['LocationID'].tolist()

    # MEDIAN SPLITS
    if income_split == "upper":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(medincome_above_IDs)]
    elif income_split == "lower":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(medincome_below_IDs)]
    elif income_split == "upper_75":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(above_75_IDs)]
    elif income_split == "lower_25":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(below_25_IDs)]
    elif income_split == "upper_25":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(above_25_IDs)]
    elif income_split == "upper_50":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(above_50_IDs)]
    
    # 1.8.2 WORKDAY SPLIT 

    # if Weekday index is 5 or 6 or holiday = 1 then weekday = 0 else weekday = 1

    taxi_data_cut['weekday'] = np.where((taxi_data_cut['Weekday_index'] == 5) | (taxi_data_cut['Weekday_index'] == 6), 0, 1)
    taxi_data_cut['weekday'] = taxi_data_cut['weekday']

    if workday_split == "weekday":
            taxi_data_cut = taxi_data_cut[taxi_data_cut['weekday'] == 1]
    elif workday_split == "weekend":
            taxi_data_cut = taxi_data_cut[taxi_data_cut['weekday'] == 0]



    # 1.8.3 TEMP SPLIT 

        # do a median ssplit for medincome 
    temp_median = covariates['temperature_deviation_summer'].median()

    quartiles = pd.qcut(covariates['temperature_deviation_summer'], 4, labels=False)

    # Add quartile information as a new column in the covariates DataFrame
    covariates['temp_quartile'] = quartiles

    # Split the data based on quartiles
    temp_q1 = covariates[covariates['temp_quartile'] == 0]  # First quartile
    temp_q2 = covariates[covariates['temp_quartile'] == 1]  # Second quartile
    temp_q3 = covariates[covariates['temp_quartile'] == 2]  # Third quartile
    temp_q4 = covariates[covariates['temp_quartile'] == 3]  # Fourth quartile

    # Only keep the list of location IDs for each quartile
    temp_q1_IDs = temp_q1['LocationID'].tolist()
    temp_q2_IDs = temp_q2['LocationID'].tolist()
    temp_q3_IDs = temp_q3['LocationID'].tolist()
    temp_q4_IDs = temp_q4['LocationID'].tolist()

    # Filter taxi data based on quartiles
    if temp_split == "q1":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(temp_q1_IDs)]
    elif temp_split == "q2":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(temp_q2_IDs)]
    elif temp_split == "q3":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(temp_q3_IDs)]
    elif temp_split == "q4":
            taxi_data_cut = taxi_data_cut[taxi_data_cut[f'{level}LocationID'].isin(temp_q4_IDs)]


    # 1.9 Create a panel data structure for linearmodels.PanelOLS
    panel_data = taxi_data_cut.set_index([f'{level}LocationID', 'Year_fact'])
    panel_data = panel_data[panel_data['temp_bins'] != 'nan']


    # 1.10 create borough by month cluster for error clustering
    
    panel_data["date_pickup"] = pd.to_datetime(panel_data["date_pickup"])
    panel_data['borough_day'] = panel_data['Borough'] + '_' + panel_data['date_pickup'].astype(str)

    # Extract the month and year from 'date_pickup'
    panel_data['month_year'] = panel_data['date_pickup'].dt.strftime('%B-%Y')

    # Create the 'borough_month_year' variable by combining 'Borough' and 'month_year'
    panel_data['borough_month_year'] = panel_data['Borough'] + '_' + panel_data['month_year']

    return panel_data

def binned_regression(panel_data, level, workday_split, exclude_zeros = False):
    """
    Estimates 2WFE-binned-panel model.
    
    panel_data (DataFrame): panel data with location and year as index
    
    level (str): "PU" or "DO" - Pickup or Dropoff Location level

    workday_split(str): "workday" or "weekend" - only for workday split . "None" if no split

    exclude_minimum_bin(bool): True if temperature bins with less than 1% of total days should be excluded

    exclude_zeros(bool): exclude zero-valued observations from estimation (pre-log+1 transformation)

    """

    ##  2. REGRESSIONS

    # Option to only include observations (day_zone pairs) with trip number > 0
    if exclude_zeros == True:
            panel_data = panel_data[panel_data['trip_number'] > 0]
            panel_data['log_total'] = np.log(panel_data['trip_number'])
    
    
    
    if workday_split == "weekday" or workday_split == "weekend":
            model_formula = f'log_total~  1+ C(temp_bins, Treatment(reference = "[17.0, 20.0]")) + pr_obs + Snowdepth + AWND + holiday  + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + EntityEffects + TimeEffects'
    
    else:
            model_formula = f'log_total~  1+ C(temp_bins, Treatment(reference = "[17.0, 20.0]")) + pr_obs + Snowdepth + AWND + weekday + holiday + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + EntityEffects + TimeEffects'

    # estimate model with borough by month clustered errors
    
    model = PanelOLS.from_formula(model_formula, data=panel_data)
    
    results = model.fit(cov_type='clustered', cluster_entity= "borough_month_year")

    return results

def binned_regression_plots(results, panel_data , temp_bin_size):
    """
    Plots the coefficients of the binned regression model along with
    95-% CI and days in each temperature bin

    results (PanelResults): results from the binned regression model
    panel_data (DataFrame): panel data with location and year as index
    temp_bin_size(int): size of temperature bins in °C
    
    """
    coefficients = results.params
    conf_int = results.conf_int()

    # Combine coefficients and confidence intervals into a single DataFrame
    df = pd.DataFrame(pd.concat([coefficients, conf_int], axis=1))
    df.columns = ['Coefficient', 'Lower CI', 'Upper CI']
    # add omitted point- 0
    omitted_index = f'C(temp_bins, Treatment(reference = "[17.0, 20.0]"))[T.[17.0, 20.0]]'
    df.loc[omitted_index] = [0,0,0]
    
    
    df.reset_index(inplace=True)

    # only keep rows where index starts with C(temp_bins)
    df = df[df['index'].str.startswith('C(temp_bins')]

    # extract temperature bin string

    df['Temperature'] = df['index'].str.split(r'\[T\.').str[1].str[:-1]

    df.drop(columns=['index'], inplace=True)

    # Convert Temperature intervals into numerical values
    df['Temperature'] = df['Temperature'].str.strip('[]').str.split(',').apply(lambda x: (float(x[0]) + float(x[1])) / 2)
    # order the dataframe by temperature
    df = df.sort_values(by=['Temperature'])
    # plot max bin as well 

    # convert the coeffients into percentages and adapt CI accordingly - only with log outcome
    
    df['Coefficient'] = df['Coefficient'] * 100
    df['Lower CI'] = df['Lower CI'] * 100
    df['Upper CI'] = df['Upper CI'] * 100

    # Extract Temperature and Coefficient values
    temperature = df['Temperature']
    coefficient = df['Coefficient']

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

    # Create the plot

    ax1.scatter(temperature, coefficient, color='blue', label='Data')
    
    ax1.set_ylabel(f'Trip number response in %')
    
    # Add confidence intervals if needed
    lower_ci = df['Lower CI']
    upper_ci = df['Upper CI']
    ax1.errorbar(temperature, coefficient, yerr=[coefficient - lower_ci, upper_ci - coefficient], fmt='o', color='red' , capsize= 3,  barsabove = True , label='Confidence Interval')

    # Fit a polynomial
    degree = 2
    coefficients_poly = np.polyfit(temperature, coefficient, degree)
    y_poly = np.polyval(coefficients_poly, temperature)

    # Plot the polynomial curve
    ax1.plot(temperature, y_poly, label=f'Polynomial Fit (Degree {degree})', color='green')

    # add a dotted line at 0 percent
    ax1.axhline(y=0, color='blue', linestyle='--' , label = 'Zero')

    # set consistent y-axis
    
    ax1.set_ylim(-10, 10)


    # For temp bin plotting
    sequence_bins = np.arange(-10, 41, temp_bin_size)
    temp_bins = pd.cut(panel_data['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered = True)
    temp_bins = temp_bins.apply(lambda x: pd.Interval(32.0, 35.0, closed='right') if x == pd.Interval(35.0, 38.0, closed='right') else x)
    panel_data['temp_bins'] = temp_bins
    # Create a new DataFrame with unique days
    unique_days = panel_data.reset_index()[['date_pickup', 'temp_bins']].drop_duplicates()

    # Count the occurrences of each bin
    temp_bin_counts = unique_days['temp_bins'].value_counts().sort_index()
    temp_bin_counts = temp_bin_counts[temp_bin_counts > 1]

    # Create a color array with 'grey' for all bars and 'red' for the bar with the maximum count
    color_bin= '(17.0, 20.0]'
    colors = ['grey' if str(bin) != color_bin else 'red' for bin in temp_bin_counts.index]
    temp_bin_counts.plot(kind='bar', color=colors)
    ax2.set_xlabel('Daily maximum temperature (°C)')
    ax2.set_ylabel('Number of Days per temperature bin')
    ax2.tick_params(axis='x', rotation=45)
    # prvent that the picture shows
    plt.close()

    

    return fig


def binned_regression_poisson(panel_data, level, workday_split):
        """
        Poisson Estimation
        """
        

        data = panel_data.dropna(subset=['trip_number', 'temp_bins', 'pr_obs', 'Snowdepth', 'AWND', 'weekday', 'holiday', 'Month_fact', 'cheby_1', 'cheby_2', 'Borough']).reset_index()

        
        #rename temp_bins 
        data['temp_bins'] = pd.Categorical(data['temp_bins'], ordered=False).astype(str)
        # rename bins so linearmodels can handle them
        data['temp_bins'] = data['temp_bins'].str.replace('\(', '[', regex=True)

        if workday_split == "weekday" or workday_split == "weekend":
                model = smf.poisson(f'trip_number ~ C(temp_bins, Treatment(reference = "[17.0, 20.0]")) + pr_obs + Snowdepth + AWND + holiday + C(Year_fact) + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 +C({level}LocationID)', data=data)                 

        else:


                model = smf.poisson(f'trip_number ~ C(temp_bins, Treatment(reference = "[17.0, 20.0]")) + pr_obs + Snowdepth + AWND + weekday + holiday + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + C({level}LocationID) + C(Year_fact)', data=data)                 


        results = model.fit(cov_type='cluster', cov_kwds={'groups': data["community_district"]})

        return results