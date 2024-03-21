## Chicago Ridesharing Analysis

import pandas as pd
import numpy as np
import datetime as dt
import holidays
from matplotlib.cbook import boxplot_stats 
import matplotlib.pyplot as plt
from linearmodels.panel import PanelOLS
import statsmodels.api as sm
import statsmodels.formula.api as smf

def fahrenheit_to_celsius(f):
        return (f - 32) * 5/9

def preprocess_chicago_ridesharing():
    """
    Concats 2018-2022 and 2023 trip records.
    Trip records were preaggregated to quarter hour - community zone level
    on Chicago Data Portal.
    
    
    Input: Trip records 2018-2022 and 2023 (preaggregated to 15 min intervals)
    
    """

    chi_tnp_2018_22 = pd.read_csv("Data/Chicago_data/Transportation_Network_Providers_-_Trips__2018_-_2022_13_12.csv")
    chi_tnp_2019_23 = pd.read_csv("Data/Chicago_data/Transportation_Network_Providers_-_Trips__2023-_.csv")
    # Concatenation
    chi_tnp = pd.concat([chi_tnp_2018_22, chi_tnp_2019_23], ignore_index=True)
    # Rename columns
    chi_tnp.rename(columns={'Trip Start Timestamp':'date_pickup' , 'Pickup Community Area' : 'PULocationID' , 'Trip Miles' : 'trip_distance' , 'Fare' : 'total_amount' , 'Trip ID' : 'trip_number'}, inplace=True)
    # Convert Datetime column - Aggregation by day

    chi_tnp["date_pickup"].astype(str)
    # extract only the date from the timestamp
    chi_tnp['date_pickup'] = chi_tnp['date_pickup'].str[:10]
    chi_tnp['date_pickup'] = pd.to_datetime(chi_tnp['date_pickup']).dt.date
    # drop observations where PUlocationID is missing
    chi_tnp = chi_tnp.dropna(subset=['PULocationID'])

    # Calculating weighted values for aggregation- no predefined weighted agg function in pandas
    chi_tnp["weighted_mean_distance"] = chi_tnp["trip_number"] * chi_tnp["trip_distance"]
    chi_tnp["weighted_mean_amount"] = chi_tnp["trip_number"] * chi_tnp["total_amount"]
    chi_tnp["weighted_tip"] = chi_tnp["trip_number"] * chi_tnp["Tip"]

    # Aggregate by day and location ID
    chi_grouped_by_day = chi_tnp.groupby(["date_pickup", "PULocationID"]).agg({
        "weighted_mean_distance": ["sum"],
        "weighted_mean_amount": ["sum"],
        "trip_number": ["sum"],
        "weighted_tip": ["sum"]
    })
    chi_grouped_by_day.columns = ["weighted_mean_distance", "weighted_mean_amount", "trip_number", "weighted_tip"]

    # Calculating mean values and percentage for the grouped data
    chi_grouped_by_day["trip_distance_mean"] = chi_grouped_by_day["weighted_mean_distance"] / chi_grouped_by_day["trip_number"]
    chi_grouped_by_day["total_amount_mean"] = chi_grouped_by_day["weighted_mean_amount"] / chi_grouped_by_day["trip_number"]
    chi_grouped_by_day["tip_mean"] = chi_grouped_by_day["weighted_tip"] / chi_grouped_by_day["trip_number"]

    # drop the weighted columns
    chi_grouped_by_day.drop(columns=["weighted_mean_distance", "weighted_mean_amount", "weighted_tip"], inplace=True)

    chi_grouped_by_day.reset_index(inplace=True)

    # save the preprocessed data
    chi_grouped_by_day.to_csv("Data/Chicago_data/Chi_TNP_Trips_grouped_by_day_2018_2023.csv", index=False)


def preprocess_chicago_weather():
    """
    Preprocesses the weather data for Chicago. Source (NOAA- O-Hare Airport Weather Station)
    and adds Oxford Covid Stringency Index.




    """

    climate = pd.read_csv("Data/Chicago_data/CHI_weather_2018-2023.csv")
    covid_stringency = pd.read_csv("Data/Chicago_data/owid-covid-data.csv")


    # prepare covid stringency control

    climate["DATE"] = pd.to_datetime(climate["DATE"]).dt.date

    covid_stringency = pd.read_csv("Data/Chicago_data/owid-covid-data.csv")
    covid_stringency_usa = covid_stringency[covid_stringency["iso_code"] == "USA"]
    covid_control = covid_stringency_usa[["date","stringency_index"]]
    covid_control['date'] = pd.to_datetime(covid_control['date']).dt.date



    # merge with climate data
    climate_covid = pd.merge(climate, covid_control, how='left', left_on='DATE', right_on='date')
    climate_covid.drop(columns=["date"], inplace=True)
    # fillup missing covid stringency with 0
    climate_covid.fillna(0, inplace=True)

    
    # convert temperature to celsius
    climate_covid['TMAX'] = climate_covid['TMAX'].apply(fahrenheit_to_celsius)
    climate_covid.rename(columns={ 'TMAX' : 'tmax_obs'}, inplace=True)

    # save weather data
    climate_covid.to_csv("Data/Chicago_data/CHI_weather_2018-2023_covid.csv", index=False)


def prepare_chicago():
    """
    Merges aggregated trip records and weather data for Chicago.
    Adds holiday information, Chebyshev polynomials and outlier filtering. 
    
    
    Input: trips:
           weather: 
    
    
    """

    trips = pd.read_csv("Data/Chicago_data/Chi_TNP_Trips_grouped_by_day_2018_2023.csv")
    weather = pd.read_csv("Data/Chicago_data/CHI_weather_2018-2023_covid.csv")

    

    # sort by date
    trips = trips.sort_values(by=['date_pickup']).reset_index(drop=True)
    # drop Nas
    trips = trips.dropna()
    # convert pickup_community_area to int

    trips['PULocationID'] = trips['PULocationID'].astype(int)

    # only keep date of the date time format column
    trips['date_pickup'] = trips['date_pickup'].str.split(' ').str[0]

    # merge trips and climate data on date
    trips = pd.merge(trips, weather, how='left', left_on='date_pickup', right_on='DATE')

    us_holidays = holidays.US()

    # Create a new column indicating whether each date is a holiday or not
    trips['holiday'] = trips['date_pickup'].apply(lambda x: 1 if x in us_holidays else 0)




    # add a weekday index to the dataframe starting with Mondays = 0 tuesdays = 1 etc.
    trips['Weekday_index'] = pd.to_datetime(trips['date_pickup']).dt.dayofweek

    taxi_data = trips


    # add month and year factors
    taxi_data['Year_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.year)[0] + 1
    taxi_data['Month_fact'] = pd.factorize(pd.to_datetime(taxi_data['date_pickup']).dt.month)[0] + 1
    # log+1 transformation
    taxi_data['log_total'] = np.log(taxi_data['trip_number'] + 1)
    # remove days without temperature measure
    taxi_data = taxi_data.dropna(subset=['tmax_obs'])


    # Add Chebyshev polynomials
    num_days = len(taxi_data["date_pickup"].unique())
    
    taxi_data['cheby_0'] = 1
    taxi_data['cheby_1'] = taxi_data['date_pickup'].rank(method='dense').astype(int)/num_days
   
    
    for i in range(2, 6):
        taxi_data[f"cheby_{i}"] = (2  * taxi_data["cheby_1"] * taxi_data[f"cheby_{i-1}"]) - taxi_data[f"cheby_{i-2}"]

    
    # Add outlier filtering

    out_yearly = pd.DataFrame()
    filtered_yearly = pd.DataFrame()   
        
    for year in taxi_data['Year_fact'].unique():
        year_data = taxi_data[taxi_data['Year_fact'] == year]
        
        for z in year_data[f"PULocationID"].unique():
            zcta_data = year_data[year_data[f"PULocationID"] == z]
            
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

    taxi_data = non_outliers


    taxi_data.to_csv("Data/Chicago_data/chicago_TNP2019_regression.csv", index=False)


def chicago_binned_regression(outcome,temp_bin_size, exclude_2020 = True):
        """
        Binned-Fixed Effects Regression for Chicago - can be split up 
        into separate functions for estimation and plotting.
        
        Input: outcome: "trip_number" or "trip_distance_mean"        
        """





        ## 1. DATA PREPARATION

      
        
        taxi_data_cut = pd.read_csv('Data/Chicago_data/chicago_TNP2019_regression.csv') 

        if exclude_2020 == True:
                taxi_data_cut = taxi_data_cut[taxi_data_cut['Year_fact'] != 3]
                

        sequence_bins = np.arange(-10, 41, temp_bin_size)
        temp_bins = pd.cut(taxi_data_cut['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered = True)
        taxi_data_cut['temp_bins'] = temp_bins


        unique_days = taxi_data_cut[['date_pickup', 'temp_bins']].drop_duplicates()

        sum_days = unique_days['temp_bins'].value_counts().sum()

        value_counts = unique_days['temp_bins'].value_counts()

        # drop bins with less than 1 % of total days from taxi_data_cut
        droplist = value_counts[value_counts < 0.01 * sum_days].index.tolist()

        # extend droplist with the bin with less than 25 days over the 4 year period
        droplist.extend(value_counts[value_counts <= 20].index.tolist())

        # drop all rows with temp_bins_2 in droplist
        taxi_data_cut = taxi_data_cut[~taxi_data_cut['temp_bins'].isin(droplist)]

        taxi_data_cut["log_trip_count"] = np.log(taxi_data_cut["trip_number"] + 1)


        taxi_data_cut['workday'] = np.where((taxi_data_cut['Weekday_index'] == 5) | (taxi_data_cut['Weekday_index'] == 6) | (taxi_data_cut['holiday'] == 1), 0, 1)
        taxi_data_cut['workday'] = taxi_data_cut['workday']



        # rename bins so linearmodels-formula can handle them

        taxi_data_cut['temp_bins'] = pd.Categorical(taxi_data_cut['temp_bins'], ordered=False).astype(str)        
        taxi_data_cut['temp_bins'] = taxi_data_cut['temp_bins'].str.replace('\(', '[', regex=True)
        # Create a panel data structure
        taxi_data_cut["date_pickup"] = pd.to_datetime(taxi_data_cut["date_pickup"])

        taxi_data_cut['borough_month'] = taxi_data_cut['PULocationID'].astype(str) + '_' + taxi_data_cut['Month_fact'].astype(str)


        panel_data = taxi_data_cut.set_index(['PULocationID', 'Year_fact'])
        panel_data = panel_data[panel_data['temp_bins'] != 'nan']

        

        # For plotting and ommitting the bin with highest number of days

        # Create a new DataFrame with unique days
        unique_days = taxi_data_cut[['date_pickup', 'temp_bins']].drop_duplicates()

        # Count the occurrences of each bin, respecting the categorical order
        temp_bin_counts = unique_days['temp_bins'].value_counts().sort_index()

        # Only plot bins with more than one day
        temp_bin_counts = temp_bin_counts[temp_bin_counts > 1]
        # Find the index of the maximum count
        max_days_bin = temp_bin_counts.idxmax()
        # exlude days with zero trips



        ##  2. REGRESSIONS

        
        if outcome == "trip_number":
                model_formula = f'log_trip_count ~ 1+  C(temp_bins, Treatment(reference = "[17.0, 20.0]")) + PRCP + AWND + SNWD  + workday + holiday + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + stringency_index + EntityEffects + TimeEffects'
                model = PanelOLS.from_formula(model_formula, data=panel_data)
                results = model.fit(cov_type='clustered', cluster_entity="PULocationID")

                # Poisson Estimation model
                # model = smf.poisson(f"trip_number ~ C(temp_bins, Treatment(reference = '[17.0, 20.0]')) + PRCP + AWND + AWND + workday + holiday + C(Year_fact) + C(Month_fact) + cheby_1 + cheby_2 + stringency_index + C(PULocationID)", data=taxi_data_cut)                 





                # results = model.fit(cov_type='cluster', cov_kwds={'groups' : taxi_data_cut[f"{level}LocationID"]})
                
                

        if outcome == "trip_distance_mean":
                
                panel_data = panel_data[panel_data['trip_distance_mean'] > 0]
                panel_data['trip_distance_mean'] = np.log(panel_data['trip_distance_mean'])

                model_formula = f'trip_distance_mean ~ 1+  C(temp_bins, Treatment(reference = "[17.0, 20.0]")) + PRCP + AWND + SNWD + workday + holiday + stringency_index + C(Month_fact) + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + EntityEffects + TimeEffects'
                model = PanelOLS.from_formula(model_formula, data=panel_data)
                results = model.fit(cov_type='clustered', cluster_entity="PULocationID")

                


        ## 3. PLOTS

        coefficients = results.params
        conf_int = results.conf_int()

        # Combine coefficients and confidence intervals into a single DataFrame
        df = pd.DataFrame(pd.concat([coefficients, conf_int], axis=1))
        df.columns = ['Coefficient', 'Lower CI', 'Upper CI']
        
        # add omitted point- 0
        omitted_index = 'C(temp_bins, Treatment(reference = "[17.0, 20.0]"))[T.[17.0, 20.0]]'
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

        # convert the coeffients into percentages and adapt CI accordingly - only with log outcome
        if outcome == "trip_number":
                df['Coefficient'] = df['Coefficient'] * 100
                df['Lower CI'] = df['Lower CI'] * 100
                df['Upper CI'] = df['Upper CI'] * 100

        # Extract Temperature and Coefficient values
        temperature = df['Temperature']
        coefficient = df['Coefficient']

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

        # Create the plot

        ax1.scatter(temperature, coefficient, color='blue', label='Data')
        if outcome == "trip_number":    
                ax1.set_ylabel(f' Trip number response in %')
        else:
                ax1.set_ylabel(f'{outcome} response in $')
        
        # ax1.set_title(f' Log-transformed {outcome} response by {temp_bin_size}°C bin- {city}- 2019-2023 - Community Zone and Year fixed effects')

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
        ax1.axhline(y=0, color='blue', linestyle='--')
        ax1.set_ylim(-10, 10)
        # Add a legend
        # ax1.legend()


        sequence_bins = np.arange(-10, 41, temp_bin_size)
        temp_bins = pd.cut(taxi_data_cut['tmax_obs'], bins=sequence_bins, include_lowest=True, ordered = True)
        taxi_data_cut['temp_bins'] = temp_bins
        # Create a new DataFrame with unique days
        unique_days = taxi_data_cut[['date_pickup', 'temp_bins']].drop_duplicates()
        temp_bin_counts = unique_days['temp_bins'].value_counts().sort_index()
        temp_bin_counts = temp_bin_counts[temp_bin_counts > 1]
        # Find the index of the maximum count
        # max_days_bin = temp_bin_counts.idxmax()

        # Create a color array with 'grey' for all bars and 'red' for the omitted bin
        color_bin= '(17.0, 20.0]'
        colors = ['grey' if str(bin) != color_bin else 'red' for bin in temp_bin_counts.index]

        # Ensure the plot respects the categorical order
        temp_bin_counts.plot(kind='bar', color=colors)
        # ax2.set_title(f' Days in each {temp_bin_size}°C bin- {city} 2019')
        ax2.set_xlabel('Temperature Bins')
        ax2.set_ylabel('Number of Days')
        ax2.tick_params(axis='x', rotation=45)
        print(results)