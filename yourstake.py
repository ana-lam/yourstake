#!/usr/bin/python

import pandas as pd
import requests
import os
import csv
import io

# Python API Query using Envirofacts
# (https://www.epa.gov/enviro/envirofacts-data-service-api#metadata)

base_URL = "https://enviro.epa.gov/enviro/efservice/"
tables = ["tri_facility", "V_TRI_FORM_R_EXT_EZ"]  # TRI tables with facility information and releases information
reporting_year = 2019
output_fileformat = "CSV"

def search_query_URL(base_URL, tables, reporting_year, output_fileformat):
    """
    Construct search query URL from inputs.
    Arguments:
        tables: String. Name of tables with relevant data we want to pull.
        reporting_year: Int. Desired year of report.
        output_fileformat: String. Desired output file format.

    Note: The URL output limit is 100000 rows at a time, would need to create a for loop
    to pull all rows. We can generate the count for the table by changing output_fileformat to "COUNT"
    and pulling rows in 100000 chunks with for loop.
    """
    URL = base_URL + "/".join(tables) + "/" + "reporting_year" + "/" + str(reporting_year) + "/" + output_fileformat
    return URL

def content_to_pandas(URL):
    """
    Fetch content from URL and convert to pandas dataframe.
    """
    r = requests.get(URL).content
    df = pd.read_csv(io.StringIO(r.decode('utf-8')), engine="python",
                    encoding='utf-8', error_bad_lines=False)
    if len(df.columns) <= 1:
        print("Error in EPA URL.")
    else:
        return df

EPA_df = content_to_pandas(search_query_URL(base_URL,
                    tables, reporting_year, output_fileformat))

# Indicating and filtering columns of interest
columns_of_interest = ["tri_facility.FACILITY_NAME", "tri_facility.PARENT_CO_NAME",
                        "tri_facility.PREF_LATITUDE", "tri_facility.PREF_LONGITUDE",
                        "V_TRI_FORM_R_EXT_EZ.CHEM_NAME", "V_TRI_FORM_R_EXT_EZ.AIR_TOTAL_RELEASE",
                        "V_TRI_FORM_R_EXT_EZ.INDUSTRY_CODE"]

EPA_df = EPA_df[columns_of_interest]

# Renaming columns for later concatenation
EPA_df = EPA_df.rename(columns={"tri_facility.FACILITY_NAME":"facility_name",
                        "tri_facility.PARENT_CO_NAME":"parent_company_name",
                        "tri_facility.PREF_LATITUDE":"latitude",
                        "tri_facility.PREF_LONGITUDE":"longitude",
                        "V_TRI_FORM_R_EXT_EZ.CHEM_NAME":"chemical",
                        "V_TRI_FORM_R_EXT_EZ.AIR_TOTAL_RELEASE":"total_air_pollutants",
                        "V_TRI_FORM_R_EXT_EZ.INDUSTRY_CODE":"industry_code(EPA)"})

# Unit conversion (lbs to kg)
EPA_df["total_air_pollutants"] = EPA_df["total_air_pollutants"] * 0.453592
EPA_df = EPA_df.rename(columns={"total_air_pollutants":"total_air_pollutants_kg"})

# NAICS industry code to industry activity conversion (would use API in future to pull by year definitions)
naics_df = pd.read_csv("naic_codes_2-6_digit_2017.csv") # load 2017 NAICS code definitions file
naics_dict = dict(zip(naics_df["2017 NAICS US Code"], naics_df["2017 NAICS US Title"]))

EPA_df["industry_code(EPA)"] = EPA_df["industry_code(EPA)"].astype(str).map(naics_dict)
EPA_df = EPA_df.rename(columns={"industry_code(EPA)":"industry"})



## EEA, no API (would either do more research on potential resources or have selenium web bot download data)

"""
## With Selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Chrome('/Users/alam/Desktop/chromedriver')

driver.get(
    'https://www.eea.europa.eu/data-and-maps/data/industrial-reporting-under-the-industrial-3')
table_link = driver.find_element_by_id('1d6eee54f9fc43729e208c9e72c63237') # link to EEA data download page

driver.execute_script("arguments[0].click();", table_link) # trigger download link
"""

EEA_df = pd.read_csv("E-PRTR_Pollutant_Releases.csv", low_memory=False)
EEA_df = EEA_df.loc[(EEA_df['reportingYear'] == 2019) & (EEA_df['medium'] == "AIR")]


columns_of_interest = ["parentCompanyName", "nameOfFeature", "mainActivityName",
                        "pointGeometryLon", "pointGeometryLat", "pollutantName",
                        "totalPollutantQuantityKg"]

EEA_df = EEA_df[columns_of_interest]

EEA_df = EEA_df.rename(columns={"nameOfFeature":"facility_name",
                        "parentCompanyName":"parent_company_name",
                        "pointGeometryLat":"latitude",
                        "pointGeometryLon":"longitude",
                        "pollutantName":"chemical",
                        "totalPollutantQuantityKg":"total_air_pollutants_kg",
                        "mainActivityName":"industry"})

combined_df = pd.concat([EPA_df, EEA_df], axis=0)
combined_df.reset_index(drop=True, inplace=True)


combined_df.to_csv("EPA_EEA_table.csv")


if __name__ == "__main__":
    print("EPA and EEA combined CSV written at \yourstake\EPA_EEA_table.csv")
