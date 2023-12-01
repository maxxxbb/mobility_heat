import pandas as pd
import statsmodels.api as sm

def run_regression(data, PULocationID):
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
        model_formula = 'log_total ~ 1 + tmax_obs + pr_obs + windspeed_obs + Weekday_index + dynamic_tourism + holiday + cheby_1 + cheby_2 + cheby_3 + cheby_4 + cheby_5 + C(Year_fact)'
        tourism = True

    # Fit the model from formula
    model = sm.formula.ols(formula=model_formula, data=temp_data)
    results = model.fit(cov_type='HC3')  
    # return results in a dictionary
    return {
        'PULocationID': PULocationID,
        'Coefficient_tmax_obs': results.params.get('tmax_obs', float('nan')),
        'CI_lower': results.conf_int().loc['tmax_obs'][0] if 'tmax_obs' in results.params else float('nan'),
        'CI_upper': results.conf_int().loc['tmax_obs'][1] if 'tmax_obs' in results.params else float('nan'),
        'p_value_tmax_obs': results.pvalues.get('tmax_obs', float('nan')),
        'num_observations': results.nobs,
        'tourism_control' : tourism
    }

def run_zcta_regression(data):

    # restrict data to observations
    temp_data = data[data["tmax_obs"] >= 20]
    
    unique_ids = temp_data['PULocationID'].unique()
    
    results = [run_regression(temp_data, id) for id in unique_ids]
    

    results_df = pd.DataFrame(results)

    # Save the results to a new CSV file
    results_df.to_csv('regression_results_by_zone.csv', index=False)





data = pd.read_csv("Pooled_data/data_regression_PU.csv")

run_zcta_regression(data)