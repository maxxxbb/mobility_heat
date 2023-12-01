import pandas as pd
import numpy as np
import statsmodels.api as sm




def run_zone_level_regression(data, PULocationID):
    """
    Run the regression for a given PULocationID, including time fixed effects,
    and return the results with heteroskedasticity-robust standard errors.
    
    Parameters:
    data (DataFrame): The panel data.
    PULocationID (int): The unique identifier for the location.
    
    Returns:
    dict: A dictionary with regression results for the given PULocationID.
    """
    # Subset the data for the current PULocationID
    temp_data = data[data['PULocationID'] == PULocationID]
    if temp_data.empty:
        print(f"The DataFrame is empty after filtering for {PULocationID}.")
    for col in ["tmax_obs", "log_total", "pr_obs"]:
        if temp_data[col].nunique() <= 1:
            print(f"{col} has no variation for {PULocationID}.")
    
    
    
    # if over half of "dynamic_tourism" is na then adapt formula:

    if temp_data["dynamic_tourism"].isna().sum() > temp_data.shape[0]/2:

        model_formula = 'log_total ~ 1 + tmax_obs + pr_obs + windspeed_obs + Weekday_index + holiday + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + C(Year_fact)'
        tourism = False


    else:
        # Define your model formula
        model_formula = 'log_total ~ 1 + tmax_obs + pr_obs + windspeed_obs + Weekday_index + dynamic_tourism + holiday + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + C(Year_fact)'
        tourism = True

    # Fit the model from formula
    model = sm.formula.ols(formula=model_formula, data=temp_data)
    results = model.fit(cov_type='HC3')  
    # Extract results and return them in a dictionary
    return {
        'PULocationID': PULocationID,
        'Coefficient_tmax_obs': results.params.get('tmax_obs', float('nan')),
        'CI_lower': results.conf_int().loc['tmax_obs'][0] if 'tmax_obs' in results.params else float('nan'),
        'CI_upper': results.conf_int().loc['tmax_obs'][1] if 'tmax_obs' in results.params else float('nan'),
        'p_value_tmax_obs': results.pvalues.get('tmax_obs', float('nan')),
        'num_observations': results.nobs,
        'tourism_control' : tourism
    }


def get_zone_level_regression_results(dataset,temp_cutoff : int,weekend_indicator : str, PU_or_DO : str):
    """
    Returns zone-level regression results for the given dataset.

    Args:
        dataset (DataFrame): The dataset to use.
        temp_cutoff (int): The temperature cutoff to use.
        weekdays (str): None for all days, "weekdays" for weekdays only, "weekends" for weekends only.
        PU_or_DO (str): "PU" for regression at pickup level , "DO" for regression at dropoff level

    """
    
    panel_data = dataset
    
    if weekend_indicator == "weekdays":
        panel_data = panel_data[panel_data["Weekday_index"] <= 4]
    if weekend_indicator == "weekends":
        panel_data = panel_data[panel_data["Weekday_index"] >= 5]
    
    # temperatur cutoff for linear model
    panel_data = panel_data[panel_data["tmax_obs"] >= temp_cutoff]

    # Get unique PULocationIDs and apply the regression function to each
    unique_ids = panel_data['PULocationID'].unique()
    results = [run_zone_level_regression(panel_data, id) for id in unique_ids]

    # Convert the results to a DataFrame
    results_df = pd.DataFrame(results)

    # Save the results to a new CSV file
    return results_df

