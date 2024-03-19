library(tidyverse) 
library(tidycensus) 
library(tigris) 
library(sf) 
library(censusapi)
library(stringr)
library(ggplot2)
library(timeDate)

# when install = TRUE, you don't have to keep using this line in your scripts
# census_api_key("b13c8d0f1a3905bae5672737732248a13386333f", install = TRUE)

#get yearly income data for nyc on demographics

modzcta_zcta <- read.csv("Data/ACS_data/Modified_Zip_Code_Tabulation_Areas__MODZCTA__20231113.csv")

ZCTAs_raw <- modzcta_zcta$ZCTA

# Split the strings by commas and unlist to get individual ZCTAs
ZCTAs_split <- unlist(strsplit(ZCTAs_raw, split = ", "))

# Get unique ZCTAs
ZCTAs_in_NYC <- unique(ZCTAs_split)



acs.main <- function(admin_unit = c("zcta"), state_unit = c(NULL, "NY"), sf_shapes = c(TRUE)) {
  ACS_Data <- get_acs(geography = admin_unit,
                      state = state_unit,
                      geometry = sf_shapes,
                      variables = c(medincome = "B19013_001",
                                    total_pop1 = "B01003_001",
                                    fpl_100 = "B06012_002", 
                                    fpl_100to150 = "B06012_003",
                                    median_rent = "B25031_001",
                                    total_hholds1 = "B22003_001",
                                    hholds_snap = "B22003_002",
                                    over16total_industry1 = "C24050_001",
                                    ag_industry = "C24050_002",
                                    construct_industry = "C24050_003",
                                    manufact_industry = "C24050_004",
                                    wholesaletrade_industry = "C24050_005",
                                    retail_industry = "C24050_006",
                                    transpo_and_utilities_industry = "C24050_007",
                                    information_industry = "C24050_008",
                                    finance_and_realestate_industry = "C24050_009",
                                    science_mngmt_admin_industry = "C24050_010",
                                    edu_health_socasst_industry = "C24050_011",
                                    arts_entertain_rec_accomodate_industry = "C24050_012",
                                    othsvcs_industry = "C24050_013",
                                    publicadmin_industry = "C24050_014",
                                    total_commute1 = "B08301_001",
                                    drove_commute = "B08301_002",
                                    pubtrans_total_commute = "B08301_010",
                                    pubtrans_bus_commute = "B08301_011",
                                    pubtrans_subway_commute = "B08301_012",
                                    pubtrans_railroad_commute = "B08301_013",
                                    pubtrans_ferry_commute = "B08301_015",
                                    taxi_commute = "B08301_016",
                                    bicycle_commute = "B08301_018",
                                    walked_commute = "B08301_019",
                                    workhome_commute = "B08301_021",
                                    unemployed = "B23025_005",
                                    under19_noinsurance = "B27010_017",
                                    age19_34_noinsurance = "B27010_033",
                                    age35_64_noinsurance = "B27010_050",
                                    age65plus_noinsurance = "B27010_066",
                                    hisplat_raceethnic = "B03002_012",
                                    nonhispLat_white_raceethnic = "B03002_003",
                                    nonhispLat_black_raceethnic = "B03002_004",
                                    nonhispLat_amerindian_raceethnic = "B03002_005",
                                    nonhispLat_asian_raceethnic = "B03002_006",
                                    age65_plus  = "B08101_008",
                                    no_vehicles = "B08203_002",
                                    time_to_work = "B08135_001",
                                    median_age = "B01002_001"),
                      year = 2019,
                      output = "wide",
                      survey = "acs5")
  
  if(admin_unit=="zcta"){
    ACS_Data <- ACS_Data %>% #only pull out the estimates and cleaning variable names
      mutate(GEOID = str_sub(GEOID, -5, -1)) %>%
      filter(GEOID %in% ZCTAs_in_NYC) %>%
      dplyr::select(-NAME)  %>%
      dplyr::select(GEOID, !ends_with("M")) %>%
      rename_at(vars(ends_with("E")), .funs = list(~str_sub(., end = -2)))
  }
  return(ACS_Data)
}

##extract data on tract level 
census_data=acs.main('zcta','NY',TRUE)





# proportion of households on snap
census_data$hholds_snap <- round((census_data$hholds_snap / census_data$total_hholds1) * 100, 2)

#proportion 150% or less of FPL
census_data$fpl_150 = round(((census_data$fpl_100+census_data$fpl_100to150)/census_data$total_pop1)*100, 2)
census_data$ag_industry = round((census_data$ag_industry/census_data$over16total_industry1)*100, 2)
census_data$construct_industry = round((census_data$construct_industry/census_data$over16total_industry1)*100, 2)
census_data$transpo_and_utilities_industry = round((census_data$transpo_and_utilities_industry/census_data$over16total_industry1)*100, 2)
census_data$manufact_industry = round((census_data$manufact_industry/census_data$over16total_industry1)*100, 2)
census_data$time_to_work = round ((census_data$time_to_work/census_data$over16total_industry1),2)
census_data$no_vehicles = round((census_data$no_vehicles/census_data$total_hholds1)*100, 2)
#drop the other industries for now 
census_data = census_data[ , -which(names(census_data) %in% c("manufact_industry","wholesaletrade_industry","retail_industry","information_industry","finance_and_realestate_industry","science_mngmt_admin_industry","edu_health_socasst_industry","arts_entertain_rec_accomodate_industry","othsvcs_industry","publicadmin_industry"))]
#proportion unemployed
census_data$unemployed = round((census_data$unemployed/census_data$over16total_industry1)*100, 2) 

#proportion of people relying on a given mode of transit
census_data=census_data%>%mutate_at(vars(ends_with("_commute")), ~round((./total_commute1)*100, 2)) 

#total proportion of uninsured people in each neighborhood  
census_data = census_data %>%mutate(not_insured = round((under19_noinsurance + 
                                                           age19_34_noinsurance + age35_64_noinsurance + age65plus_noinsurance) / total_pop1*100, 2))
#proportion of ppl reporting a given race/ethncity            
census_data = census_data %>%mutate_at(vars(ends_with("_raceethnic")), ~round((./total_pop1)*100, 2)) 

#proportion elderly 
census_data$age65_plus_share = round((census_data$age65_plus/census_data$total_pop1)*100, 2) 

# convert NANs resulting from zero values to NAs


for (col_name in names(census_data)) {
  # Check if the column is numeric
  if (is.numeric(census_data[[col_name]])) {
    # Replace NaN with NA in the numeric column
    census_data[[col_name]][is.nan(census_data[[col_name]])] <- NA
  }
}

# convert geometry column to wkt
census_data$geometry <- st_as_text(census_data$geometry)


write.csv(census_data, 'Data/ACS_data/census_data_zcta.csv', row.names = FALSE)





