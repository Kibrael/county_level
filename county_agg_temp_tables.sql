 CREATE TEMPORARY TABLE
  county_apps AS (
  SELECT year, state, county,
	ROUND(avg(amount::INTEGER),2) AS loan_average_app,
        ROUND(avg(income::INTEGER),2) AS income_average_app,
        COUNT(concat(agency, rid)) AS count_app,
        SUM(amount::INTEGER) AS value_app,
        --ROUND(((amount::NUMERIC / 0.80) / income::NUMERIC),2) AS income_mult_app,
	CONCAT(state, county) AS fips

        FROM hmdalar2000

        WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND income NOT LIKE '%NA%'
        AND income NOT LIKE '%na% '
        AND amount NOT LIKE '%NA%'
        AND amount NOT LIKE '%na%'

        GROUP BY state, county, year, amount, income);

 Select * from county_apps;

 CREATE TEMPORARY TABLE
 county_orig AS (
 SELECT year, state, county,
        ROUND(avg(amount::INTEGER),2) AS loan_average_orig,
        ROUND(avg(income::INTEGER),2) AS income_average_orig,
        COUNT(concat(agency, rid)) AS count_orig,
        SUM(amount::INTEGER) AS value_orig,
        --ROUND(((amount::NUMERIC / 0.80) / income::NUMERIC),2) AS income_mult_orig,
        CONCAT(state, county) AS fips

        FROM hmdalar2000

        WHERE loan_type = '1'
        AND action = '1'
        AND loan_purpose in ('1', '3')
        AND income NOT LIKE '%NA%'
        AND income NOT LIKE '%na% '
        AND amount NOT LIKE '%NA%'
        AND amount NOT LIKE '%na%'

        GROUP BY state, county, year, amount, income)

