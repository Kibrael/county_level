 CREATE TABLE
  county_apps_2002 AS (
  SELECT year, state, county,
	ROUND(avg(amount::INTEGER),2) AS loan_average_app,
        ROUND(avg(income::INTEGER),2) AS income_average_app,
        COUNT(concat(agency, rid)) AS count_app,
        SUM(amount::INTEGER) AS value_app,
        --ROUND(((amount::NUMERIC / 0.80) / income::NUMERIC),2) AS income_mult_app,
	CONCAT(state, county) AS fips

        FROM hmdalar2002

        WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND income NOT LIKE '%NA%'
        AND income NOT LIKE '%na% '
        AND amount NOT LIKE '%NA%'
        AND amount NOT LIKE '%na%'

        GROUP BY state, county, year, amount, income);

commit;

 CREATE TABLE
  county_orig_2001 AS (
  SELECT year, state, county,
	ROUND(avg(amount::INTEGER),2) AS loan_average_app,
        ROUND(avg(income::INTEGER),2) AS income_average_app,
        COUNT(concat(agency, rid)) AS count_app,
        SUM(amount::INTEGER) AS value_app,
        --ROUND(((amount::NUMERIC / 0.80) / income::NUMERIC),2) AS income_mult_app,
	CONCAT(state, county) AS fips

        FROM hmdalar2001

        WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND action = '1'
        AND income NOT LIKE '%NA%'
        AND income NOT LIKE '%na% '
        AND amount NOT LIKE '%NA%'
        AND amount NOT LIKE '%na%'

        GROUP BY state, county, year, amount, income); 

        commit;
