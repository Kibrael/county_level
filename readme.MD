# An investingation of how different counties experienced the boom, bust, and recovery from 2000 to 2014.

### Analysis will be done on a CSV containing state and county geographic identifiers. For each geography the following data fields will be available:


#### Method:
The analysis aggregates the count, dollar value, income, and rate spread variables for applications and originations in the HMDA dataset from 2000 to 2014.

Focus will be on counties that experienced high growth in average loan value, count of applications or originations during 2000-2007, a significant price drop from 2007-2009, and a minimal recovery post 2009.

Rate spread is used as a proxy for potentially harmful loan products. Rate spread is examined as a ratio of loans with a rate spread to loans without a rate spread. For the time in question, the APOR supplied by Freddie Mac's PMMS had a standard deviation of 1.30. This is roughly approximate to the first-lien rate spread reporting threshold of 1.5. Only first lien loans were examined.

#### Loan Products:
Data were filtered to allow valid comparison between properties and loans.
Single-family: property_type 1
first-lien: lien status 1
purchase or refinance: loan purpose 1 or 3

### Data Handling:
Rows with incorrectly formatted data were not used. This removed between 15 and 30 rows per year (predominantly from agency code 7).

Rows with empty strings, or NA for income or loan amount were not used.

To calculate a multiple of loan amount to income, loan amounts were taken from conventional products and multiplied by 0.80.

### Notes:
Income Multiple - average loan amount has been divided by 0.80. All loans in the analysis were conventional loans.