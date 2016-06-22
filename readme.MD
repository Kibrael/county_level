## This repository includes a set of scripts and functions that produce county-level aggregate outputs of HMDA application and origination activity.

##Dependancies
PostgreSQL with HMDA LAR data loaded (link repo for load), (link archives)
Psycopg2
Python 3.5 or higher
Pandas
Jupyter (for notebooks)
Bokeh (for visualization)


Running the Aggregation(link) script will produce SQL tables and CSVs with the following fields:
- FIXME add data fields

Running split_by_county.py will produce CSVs for each valid county that contain the following fields:
- FIXME add data fields

#### Method:
FIXME definition of method for calculated variables
To calculate a multiple of loan amount to income, loan amounts were taken from conventional products and multiplied by 0.80.

#### Loan Products:
When using HMDA LAR data is is often important to filter for specific loan products.
This repository has functions that aggregate single-family homes that were purchased or refinanced (loan purpose codes 1 and 3 respectively).

To make this analysis more robust lien status should be added, but this variable does not exist until 2004.


#### Data Handling:
To handle Pandas dataframe issues, rows with income or amount values that contained NA or na were omitted.

Rows with incorrectly formatted data were removed from the HMDA LAR SQL tables. This removed between 15 and 30 rows per year (predominantly from agency code 7).



#### Notes:
Income Multiple - average loan amount has been divided by 0.80. All loans in the analysis were conventional loans.

#### File Descriptions:


to load HMDA data please go to [this repository](https://github.com/Kibrael/HMDA_load)