# Mobility and Heat:
### Author

- Max Boehringer (University of Bonn, s6mxboeh@uni-bonn.de)

### About

This repository contains code to replicate the main estimates of .... .


### Requires
In order to run this project on your local machine you need to have installed Python, an Anaconda distribution and LaTex distribution in order to compile .tex documents.

The project was created on Windows 10 using

- Anaconda 4.11.0
- Python 3.11
- R 4.3

1. All necessary python dependencies are contained in environment.yml . To install the virtual environment in a terminal move to the root folder of the repository and type `$ conda env create -f environment.yml` and to activate type  `$ conda activate dl_intro`.

2. Trip record data is too large to upload and can be made available upon request. Aggregated weather data (source: NOAA) and census data (source: ACS 2019) are in the Data Folder.

### Data 

Trip record data is not contained in this repository due to its size. The aggregated datasets will be made available upon request:

1. **Trip Records**:
   - 1.1 NYC Trip Records are provided by the NYC Taxi and Limousine Comission [(TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
   - 1.2 Chicago Trip Records are provided by the [City of Chicago](https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2018-2022-)

NYC monthly trip records (.parquet) from 2015-2019 were downloaded from the TLC homepage and then processed with the scripts `prepocess_data.py`,  `pool_taxi_data.py` and `prepare_for_regression`. The preprocessed trip records for the respective subsets (medallion taxis or ridesharing companies) are saved into `Pooled_data\level\final\final_data_subset_level` which are then used for the subsequent analysis.

Chicago ridesharing trip records were preaggregated on the Chicago Open Data Portal as defined [here](https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2018-2022-/m6dm-c72p/explore/query/SELECT%0A%20%20%60trip_start_timestamp%60%2C%0A%20%20%60pickup_community_area%60%2C%0A%20%20count%28%60trip_id%60%29%20AS%20%60count_trips%60%2C%0A%20%20avg%28%60fare%60%29%20AS%20%60avg_fare%60%2C%0A%20%20avg%28%60trip_miles%60%29%20AS%20%60avg_trip_distance%60%2C%0A%20%20avg%28%60tip%60%29%20AS%20%60avg_tip%60%0AGROUP%20BY%20%60trip_start_timestamp%60%2C%20%60pickup_community_area%60%0AORDER%20BY%20%60pickup_community_area%60%20ASC%20NULL%20LAST/page/aggregate). The aggregation was done for the 2018-2022 and 2023 ridesharing trip records.

2. **Weather Data**:

Weather data was sourced from the [NOAA](https://www.ncei.noaa.gov/cdo-web/search) (Central Park Weather Station for NYC and O'Hare Station for CHI) and are contained in the folder 'Data/NYC_weather' and `Data/Chicago_data`. The 'NYC_weather' folder additionally contains raster-data on the mean summertime surface temperature for NYC sourced from the [New York City Council Data Team](https://github.com/NewYorkCityCouncil/heat_map).


3. **Neighborhood Characteristics**

   - 3.1 ACS (2015-2019) U.S. Census Bureau 5-year estimates : is downloaded in the script `process_census_data_taxi_5_year_estimates`.
   - 3.2 Park and Beach Areas: Data on [Park](https://nycopendata.socrata.com/Recreation/Parks-Properties/enfh-gkve/about_data) and [Beach](https://data.cityofnewyork.us/dataset/Beaches/ijwa-mn2v/about_data) Areas in NYC are sourced from the Department of Parks and Recreation (DPR)

Final resulting dataset containing all neighborhood characteristics used in the analysis and matched to the taxi-zone level  are in `Data\ACS_data\taxi_zones_ACS_parks_beaches_deviation.csv`

4. **Shapefiles**

Contains shapefiles of Taxi zones and ZCTAs from the TLC website and the NYC Open Data Portal.

### Which scripts are contained?

#### Data Preprocessing:

- **Trip_record_preprocessing.pdf**: Pdf describing the steps to process the trip records more detailed.
- **preprocess_data.py**: Script to aggregate raw trip records (.parquet) on the day-by-zone level
- **pool_taxi_data.py**: Script to pool Yellow and Green and FHV and HVFHV datasets
- **prepare_for_regression**: Merges aggregated trip records with weather data and prepares those for usage in the main analysis.
- **weight_socioeconomic_data.py**: Script to match ACS_data to the taxi zone level and adding park and beach areas.
- **test_weighting.py**: Contains unit test for weighting function.
- **add_satellite_tenmperature.py**: Maps Landsat-8 raster data to the taxi zones and creates averages to get neighborhood-level heat proxy.
- **process_census_data_taxi_5_year_estimates.R**: Script to download Census data
- **chicago_ridesharing_functions.py**: Contains all datapreprocessing steps for Chicago subset.

#### Analysis:
- **binned_regression.py**: Contains all relevant functions to estimate binned panel model and plots.
- **main_reg_notebook.ipynb**: Main Model with different specifications are estimated here. Draws on functions from `binned_regression.py` and Data folder.
- **mobility_response_by_neighborhood.py**: Contains all functions used to estimate the neighborhood-level response.





#### Plots:
- **Plot_neighborhood_characteristics.ipynb**: Notebook to create vizualisations of neighborhood characteristics on the taxi zone level
- **Plot_pickup_maps.ipynb**: Notebook to create visualizations of taxi and ridesharing rides per taxi zone.
- **Plot_time_series**: Notebook containing plot of ridership distribution over time.


- **paper** : final pdf
- **tables** : tables of estimates and summary statistics


### Sources:
