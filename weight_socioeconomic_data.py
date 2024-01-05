import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.wkt import loads

#### FUNCTIONS

def calculate_weighted_socioeconomic_data(demographics_csv, taxi_zones_csv):
    """
    Process demographic and taxi zone data to create a weighted socioeconomic dataset.

    This function reads demographic data and taxi zone geometries from CSV files, 
    converts them into GeoDataFrames, and calculates weighted averages for socioeconomic 
    variables based on the overlap of taxi zones with ZCTAs (Zip Code Tabulation Areas).

    Parameters:
    demographics_csv (str): File path to the CSV containing demographic data by ZCTA.
    taxi_zones_csv (str): File path to the CSV containing taxi zone geometries.

    Returns:
    pandas.DataFrame: A DataFrame containing the weighted socioeconomic data by taxi zone.
    """
    # Load the datasets
    demographics_df = pd.read_csv(demographics_csv)
    # with new demographics: downloaded in R : rename GEOID into zcta
    demographics_df = demographics_df.rename(columns={"GEOID": "zcta"})

    taxi_zones_df = pd.read_csv(taxi_zones_csv)

    # Convert the 'geometry' columns from WKT to actual geometric objects
    demographics_gdf = gpd.GeoDataFrame(
        demographics_df, 
        geometry=gpd.GeoSeries.from_wkt(demographics_df['geometry'])
    )
    taxi_zones_gdf = gpd.GeoDataFrame(
        taxi_zones_df, 
        geometry=gpd.GeoSeries.from_wkt(taxi_zones_df['geometry'])
    )

    # Set the coordinate reference system to WGS84
    demographics_gdf.set_crs(epsg=4326, inplace=True)
    taxi_zones_gdf.set_crs(epsg=4326, inplace=True)

    # List of socioeconomic variables to check for numeric types : , "heatdays" , 'HVI', 'age65_prop', 'heat_jobs
    socioeconomic_variables = ["medincome", "total_pop1", "fpl_100", "fpl_100to150", "median_rent",
                           "total_hholds1", 'hholds_snap', 'over16total_industry1', 'ag_industry',
                           'construct_industry', 'transpo_and_utilities_industry', 'total_commute1',
                           'drove_commute', 'pubtrans_bus_commute', 'pubtrans_subway_commute',
                           'pubtrans_ferry_commute', 'taxi_commute', 'bicycle_commute', 'walked_commute',
                           'workhome_commute', 'unemployed', 'under19_noinsurance', 'age19_34_noinsurance',
                           'age35_64_noinsurance', 'age65plus_noinsurance', 'hisplat_raceethnic',
                           'nonhispLat_white_raceethnic', 'nonhispLat_black_raceethnic',
                           'nonhispLat_amerindian_raceethnic', 'nonhispLat_asian_raceethnic', 'age65_plus',
                           'fpl_150', 'not_insured']

    # Filter for numeric socioeconomic variables only
    socioeconomic_variables = [
        var for var in socioeconomic_variables 
        if pd.api.types.is_numeric_dtype(demographics_gdf[var])
    ]

    # Compute weighted averages for each taxi zone
    taxi_zone_data = _compute_weighted_averages(
        taxi_zones_gdf, 
        demographics_gdf, 
        socioeconomic_variables
    )

    taxi_zones_socioeconomics = pd.DataFrame(taxi_zone_data)
    
    # set covariates to N.A.N if the sum of intersection shares is less than 0.2
    
    taxi_zones_socioeconomics['sum_of_shares'] = taxi_zones_socioeconomics['intersection_shares'].apply(convert_to_floats_and_sum)


    exclude_columns = ['LocationID', 'Zone', 'ZCTA_IDS' , 'sum_of_shares']


    columns_to_nan = taxi_zones_socioeconomics.columns.difference(exclude_columns)

    taxi_zones_socioeconomics.loc[taxi_zones_socioeconomics['sum_of_shares'] < 0.2, columns_to_nan] = np.nan
    
    final_df = taxi_zones_socioeconomics
    

    output_path = "ACS_data/taxi_zones_ACS.csv"
    final_df.to_csv(output_path, index=False)



def convert_to_floats_and_sum(string):
    """
    Helper function to detect taxi zones which do not correspond to a single Zip Code
    Area from the ACS data. The covariates of taxi zones which intersect with the zones 
    less than 20 percent are set then later set to N.A.N
    """
    if pd.isna(string) or string in ['NaN', 'nan', '']:
        return 0
    try:
        # Splitting the string (containing intersection shares of zctas corresponding to one taxi zones.) by comma and converting each part to a float
        numbers = [float(num) for num in string.split(',')]
        
        return sum(numbers)
    except ValueError:
        return 0


def calculate_park_area_in_taxizone(taxizone, parks):
    """
    Helper Function which returns the sum of park areas (and beach areas in a given taxizone,
    by calculating the intersection of each park geometry with the given taxizone geometry.

    Function will be applied to each row of the taxizone GeoDataFrame

    Parameters:
    taxizone (GeoDataFrame): a single row of the taxizone GeoDataFrame
    parks (GeoDataFrame): the parks GeoDataFrame

    Returns:
    float: the sum of park areas in the taxizone

    """
    
    intersections = parks['multipolygon'].intersection(taxizone['geometry'])
    return intersections.area.sum()



def _compute_weighted_averages(taxi_zones_gdf, demographics_gdf, socioeconomic_variables):
    """
    Helper function to compute weighted averages for socioeconomic variables.

    Parameters:
    taxi_zones_gdf (geopandas.GeoDataFrame): GeoDataFrame containing taxi zone geometries.
    demographics_gdf (geopandas.GeoDataFrame): GeoDataFrame containing demographic data by ZCTA.
    socioeconomic_variables (list): List of socioeconomic variable names to compute weighted averages for.

    Returns:
    taxi_zone_data: List of dictionaries containing weighted socioeconomic data for each taxi zone and zone
    """
    
    
    taxi_zone_data = []
    # Loop through each taxi zone
    for taxi_zone in taxi_zones_gdf.itertuples():
        taxi_zone_geom = taxi_zone.geometry
        taxi_zone_area = taxi_zone_geom.area
        taxi_zone_socioeconomic_data = {'LocationID': taxi_zone.location_i, 'Zone': taxi_zone.zone}
        intersecting_zctas = []
        zcta_ids = []
        intersection_shares = []

        # Loop through each ZCTA
        for zcta in demographics_gdf.itertuples():
            zcta_geom = zcta.geometry

            if taxi_zone_geom.intersects(zcta_geom):
                intersection_area = taxi_zone_geom.intersection(zcta_geom).area
                area_proportion = intersection_area / taxi_zone_area
                population_weight = zcta.total_pop1
                weighted_proportion = area_proportion * population_weight
                intersecting_zctas.append((zcta, weighted_proportion))
                zcta_ids.append(zcta.zcta)
                # intersection_shares.append((zcta.zcta, area_proportion)) 
                intersection_shares.append((area_proportion)) 
        # Calculate real weighted averages
        for var in socioeconomic_variables:
            
            if len(intersecting_zctas) > 0:
                weighted_sum = sum(getattr(zcta, var) * proportion for zcta, proportion in intersecting_zctas)
                total_weight = sum(proportion for _, proportion in intersecting_zctas)
                taxi_zone_socioeconomic_data[f'{var}'] = weighted_sum / total_weight if total_weight != 0 else 0

        taxi_zone_socioeconomic_data['ZCTA_IDs'] = ', '.join(map(str, zcta_ids))
        taxi_zone_socioeconomic_data['intersection_shares'] = ', '.join(map(str, intersection_shares))
        
        taxi_zone_data.append(taxi_zone_socioeconomic_data)

    return taxi_zone_data



def add_parks_and_beaches(parks_csv, beaches_csv ,taxi_zones_geometry , taxi_zones_ACS):
    """
    Add parks and beach coverage to the taxi zones dataset.

    This function reads parks data and taxi zone geometries from CSV files, 
    converts them into GeoDataFrames, and adds the park coverage to ACS covariates.

    Parameters:
    parks_csv (str): File path to the CSV containing parks data.
    beaches_csv (str): File path to the CSV containing beaches data.
    taxi_zones_csv (str): File path to the CSV containing taxi zone geometries.
    taxi_zones_ACS (str): File path to the CSV containing taxi zone ACS covariates.


    Returns:
    pandas.DataFrame: A DataFrame containing the weighted socioeconomic data by taxi zone.
    """
    taxi_zones_ACS = pd.read_csv(taxi_zones_ACS)
    df_taxizones = pd.read_csv(taxi_zones_geometry)
    df_parks = pd.read_csv(parks_csv)
    df_beaches = pd.read_csv(beaches_csv)

    # convert string columns to vector geometries
    df_beaches['multipolygon'] = df_beaches['multipolygon'].apply(loads)
    df_parks['multipolygon'] = df_parks['multipolygon'].apply(loads)
    df_taxizones['geometry'] = df_taxizones['geometry'].apply(loads)

    gdf_parks = gpd.GeoDataFrame(df_parks, geometry='multipolygon')
    gdf_taxizones = gpd.GeoDataFrame(df_taxizones, geometry='geometry')
    gdf_beaches = gpd.GeoDataFrame(df_beaches, geometry='multipolygon')

    # set coordinate reference system for both such that they coincide
    gdf_parks.set_crs(epsg=4326, inplace=True)
    gdf_taxizones.set_crs(epsg=4326, inplace=True)
    gdf_beaches.set_crs(epsg=4326, inplace=True)
    
    # Apply helper function to each row in taxi zones df
    gdf_taxizones['park_area'] = gdf_taxizones.apply(lambda x: calculate_park_area_in_taxizone(x, gdf_parks), axis=1)
    gdf_taxizones['park_coverage'] = gdf_taxizones['park_area'] / gdf_taxizones['shape_area'] * 100

    gdf_taxizones['beach_area'] = gdf_taxizones.apply(lambda x: calculate_park_area_in_taxizone(x, gdf_beaches), axis=1)
    gdf_taxizones['beach_coverage'] = gdf_taxizones['beach_area'] / gdf_taxizones['shape_area'] * 100

    park_coverage = pd.DataFrame(gdf_taxizones[['park_coverage','beach_coverage' ,'location_i']])

    taxizone_ACS_parks = pd.merge(taxi_zones_ACS, park_coverage, left_on = 'LocationID',right_on='location_i', how='left').drop(columns=['location_i'])


    # Save the dataset
    output_path = "ACS_data/taxi_zones_ACS_parks_beaches.csv"
    taxizone_ACS_parks.to_csv(output_path, index=False)


#### PATHS

demographics_csv = "ACS_data/census_data_zcta.csv"
taxi_zones_csv = "Shapefiles/taxi_zones_geometry.csv"
parks_csv = "Heat_Vulnerability/Parks_Properties_20231208.csv"
taxi_zones_ACS = "ACS_data/taxi_zones_ACS.csv"
taxi_zones_geometry = "Shapefiles/taxi_zones_geometry.csv"
parks_csv = "Heat_Vulnerability/Parks_Properties_20231208.csv"
beaches_csv = "Heat_Vulnerability/Beaches_20240105.csv"


#### RUN

calculate_weighted_socioeconomic_data(demographics_csv, taxi_zones_csv)
add_parks_and_beaches(parks_csv, beaches_csv ,taxi_zones_geometry , taxi_zones_ACS)