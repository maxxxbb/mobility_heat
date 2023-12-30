import pandas as pd
import geopandas as gpd

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

    final_df = pd.DataFrame(taxi_zone_data)

    output_path = "taxi_zones_with_socioeconomics_popweighted.csv"
    final_df.to_csv(output_path, index=False)




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
                intersection_shares.append((zcta.zcta, area_proportion)) 

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


demographics_csv = "ACS_data/census_data_zcta.csv"
taxi_zones_csv = "Shapefiles/taxi_zones_geometry.csv"

calculate_weighted_socioeconomic_data(demographics_csv, taxi_zones_csv)