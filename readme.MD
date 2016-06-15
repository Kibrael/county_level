## An investingation of how different counties experienced the boom, bust, and recovery from 2000 to 2014.

#### This code requires HMDA LAR data from 2000 to 2014 loaded to tables on a PostgreSQL server. This repository contains code that will produce tables of the HMDA data aggregated to the county level (one table per year) and a CSV for each county containing all 15 years in the /data/[state]/[county] directory path.

The following fields are available:
- FIXME add data fields

#### Method:
The analysis aggregates the count, dollar value, and income variables for applications and originations in the HMDA dataset from 2000 to 2014. These data vectors are available for the entire population and by each race listed in the HMDA data.

Focus will be on counties that experienced high growth in average loan value, count of applications or originations during 2000-2007, a significant price drop from 2007-2009, and a minimal recovery post 2009.


#### Loan Products:
Data were filtered to allow valid comparison between properties and loans.
Single-family: property type 1
purchase or refinance: loan purpose 1 or 3
Income and loan amount were filtered to remove NA fields prior to aggregation

#### Data Handling:
Rows with incorrectly formatted data were not used. This removed between 15 and 30 rows per year (predominantly from agency code 7).

To calculate a multiple of loan amount to income, loan amounts were taken from conventional products and multiplied by 0.80.

#### Notes:
Income Multiple - average loan amount has been divided by 0.80. All loans in the analysis were conventional loans.

#### File Descriptions:

Load_agg.py: creates aggregate tables by year and county for applications and originations

to load HMDA data please go to [this repository](https://github.com/Kibrael/HMDA_load)