import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import unittest


def _compute_weighted_averages(taxi_zones_gdf, demographics_gdf, socioeconomic_variables):
    """
    Weighted average function for testing
    """
    
    
    taxi_zone_data = []
    # Loop through each taxi zone
    for taxi_zone in taxi_zones_gdf.itertuples():
        taxi_zone_geom = taxi_zone.geometry
        taxi_zone_area = taxi_zone_geom.area
        # initialize list
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






class TestSocioeconomicCalculations(unittest.TestCase):

    def test_weighted_averages(self):
        # Test data setup
        demographics_data = {
            'zcta': ['ZCTA1', 'ZCTA2', 'ZCTA3'],
            'total_pop1': [100, 200, 100],
            'medincome': [50000, 40000, 60000],
            'geometry': [Polygon([(0, 0), (0.33, 0), (0.33, 1), (0, 1)]),
                         Polygon([(0.33, 0), (0.66, 0), (0.66, 1), (0.33, 1)]),
                         Polygon([(0.66, 0), (1, 0), (1, 1), (0.66, 1)])]
        }
        taxi_zones_data = {
            'location_i': ['1', '2', '3'],
            'zone': ['Zone A', 'Zone B' , 'Zone C'],
            'geometry': [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                         Polygon([(0.66, 0), (1, 0), (1, 1), (0.66, 1)]),
                         Polygon([(0.9, 0), (1, 0), (1, 1), (0.9, 1)])]
        }

        # Create GeoDataFrames
        demographics_gdf = gpd.GeoDataFrame(demographics_data, geometry='geometry')
        taxi_zones_gdf = gpd.GeoDataFrame(taxi_zones_data,geometry='geometry')
         
        demographics_gdf.set_crs(epsg=4326, inplace=True)
        taxi_zones_gdf.set_crs(epsg=4326, inplace=True)


        # Define the socioeconomic variables to include in the weighted average calculation
        socioeconomic_variables = ["medincome"]

        # Calculate the weighted socioeconomic data
        weighted_data = _compute_weighted_averages(taxi_zones_gdf,demographics_gdf,socioeconomic_variables)

        # Expected results
        expected_results = {
            '1': 47593.98 , # Equal weights for each ZCTA in Zone A
            '2': 60000,   # Zone B consists only of ZCTA 3
            '3': 60000    # Zone C consists only of ZCTA 3 but not fully -> still assign same value
        }

        print(weighted_data)
        # Assert calculations
        for row in weighted_data:
            # Check if medincome is within a 1000 range of the expected value
            self.assertTrue(
                expected_results[row['LocationID']] - 10 <= row['medincome'] <= expected_results[row['LocationID']] + 10,
                f"medincome for {row['LocationID']} is not within the expected range"
            )

if __name__ == '__main__':
    unittest.main()
