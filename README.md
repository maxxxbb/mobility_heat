# Mobility and Heat:
### Author

- Max Boehringer (University of Bonn, s6mxboeh@uni-bonn.de)

### About

This repository contains code to replicate the main estimates of my masteers thesis


### Requires
In order to run this project on your local machine you need to have installed Python, an Anaconda distribution and LaTex distribution in order to compile .tex documents.

The project was created on Windows 10 using

- Anaconda 4.11.0
- Python 3.11

1. All necessary python dependencies are contained in environment.yml . To install the virtual environment in a terminal move to the root folder of the repository and type `$ conda env create -f environment.yml` and to activate type  `$ conda activate dl_intro`.

2. Trip record data is too large to upload and can be made available upon request. Aggregated weather data (source: NOAA) and census data (source: ACS 2019) are in the Data Folder.

### Data 

Trip record data is not contained in this repository due to its size. The aggregated datasets will be made available upon request:

1. **Trip Records**:
   1.1 NYC Trip Records are sourced from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
   1.2 Chicago Trip Records are sourced from https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2018-2022-

NYC monthly trip records (.parquet) from 2015-2019 were downloaded from the TLC homepage and then processed with the scripts 'prepocess_data.py',  'pool_taxi_data.py' and 'prepare_for_regression'. The preprocessed trip records for the respective subsets (medallion taxis or ridesharing companies) are saved into 'Pooled_data\level\final\final_data_subset_level' which are then used for the subsequent analysis.

Chicago ridesharing trip records were preaggregated on the Chicago Open Data Portal: (https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2018-2022-/m6dm-c72p/explore/query/SELECT%0A%20%20%60trip_start_timestamp%60%2C%0A%20%20%60pickup_community_area%60%2C%0A%20%20count%28%60trip_id%60%29%20AS%20%60count_trips%60%2C%0A%20%20avg%28%60fare%60%29%20AS%20%60avg_fare%60%2C%0A%20%20avg%28%60trip_miles%60%29%20AS%20%60avg_trip_distance%60%2C%0A%20%20avg%28%60tip%60%29%20AS%20%60avg_tip%60%0AGROUP%20BY%20%60trip_start_timestamp%60%2C%20%60pickup_community_area%60%0AORDER%20BY%20%60pickup_community_area%60%20ASC%20NULL%20LAST/page/aggregate). The aggregation was done for the 2018-2022 and 2023 ridesharing trip records.

2. **Weather Data**:

Weather data was sourced from the NOAA (https://www.ncei.noaa.gov/cdo-web/search) (Central Park Weather Station for NYC and O'Hare Station for CHI) and are contained in the folder 'Data/NYC_weather' and 'Data/Chicago_data'. The 'NYC_weather' folder additionally contains raster-data on the mean summertime surface temperature for NYC sourced from https://github.com/NewYorkCityCouncil/heat_map.


3. **Neighborhood Characteristics**

   3.1 ACS (2015-2019) 5-year estimates :

### Which scripts are contained?



- **data**: prepared data
- **documentation**: documentation on the code in /src
- **estimation**: estimation results for generating tables and graphs.
- **figures** :  plots
- **paper** : final pdf
- **tables** : tables of estimates and summary statistics


### Sources:
