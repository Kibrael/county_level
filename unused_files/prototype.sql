WITH rates AS (
    SELECT year, state, county,
	CONCAT(state, county) AS fips,
	COUNT(rate_spread) AS has_rs,
	AVG(rate_spread::NUMERIC) AS avg_rate_spread
    
    FROM hmdalar2004
    
    WHERE loan_type = '1'
        AND loan_purpose IN ('1', '3')
        AND property_type = '1'
        AND action = '1'
        AND lien = '1'
        AND rate_spread != 'NA   '
        AND rate_spread != '     '
        
    GROUP BY year, state, county),

rates2 as (
    SELECT year, state, county,
    CONCAT(state, county) AS fips,
    COUNT(rate_spread) AS no_rs
    
    FROM hmdalar2004
    
    WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND property_type = '1'
        AND action = '1'
        AND lien = '1'
        AND rate_spread = 'NA   '
        
GROUP BY year, state, county),

county_agg_orig AS (
    SELECT year, state, county,
	CONCAT(state, county) AS fips,
        ROUND(avg(amount::INTEGER),2) AS loan_average,
        ROUND(avg(income::INTEGER),2) AS income_average,
        COUNT(concat(agency, rid)) AS count_orig,
        SUM(amount::INTEGER) AS value_orig,
        SUM(income::INTEGER) AS total_income_orig
	
    FROM hmdalar2004
    
    WHERE loan_type = '1'
        AND loan_purpose IN ('1', '3')
        AND property_type = '1'
        AND action = '1'
        AND lien = '1'
        AND rate_spread != '     '
        AND income NOT LIKE '%NA%'
        AND income NOT LIKE '%na% '
        AND income != '    '
        AND amount != '     '
        AND amount NOT LIKE '%NA%'
        AND amount NOT LIKE '%na%'
        
        GROUP BY state, county, year ),

county_agg_apps AS (
SELECT year, state, county,
	CONCAT(state, county) AS fips,
        ROUND(AVG(amount::INTEGER),2) AS loan_average,
        ROUND(AVG(income::INTEGER),2) AS income_average,
        COUNT(CONCAT(agency, rid)) AS count_app,
        SUM(amount::INTEGER) AS value_app,
        SUM(income::INTEGER) AS total_income_app
        
        FROM hmdalar2004
        
        WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND property_type = '1'
        AND action != '1'
        AND lien = '1'
        AND rate_spread != '     '
        AND income not like '%NA%'
        AND income not like '%na% '
        AND income != '    '
        AND amount != '     '
        AND amount not like '%NA%'
        AND amount not like '%na%'
        
        GROUP BY state, county, year )
        
SELECT rates.year, rates.state, rates.county, rates.fips, rates.has_rs, rates2.no_rs, 
ROUND(rates.avg_rate_spread, 2) AS avg_rs, 
ROUND(((rates.has_rs::NUMERIC / (rates2.no_rs::NUMERIC + rates.has_rs::NUMERIC))*100),2) AS rs_ratio,
county_agg_orig.loan_average, 
county_agg_orig.income_average, 
county_agg_orig.count_orig, 
county_agg_orig.value_orig,
ROUND((value_orig::NUMERIC / total_income_orig::NUMERIC),2) AS total_mult_orig,
ROUND(((value_app::NUMERIC / 0.80) / total_income_app::NUMERIC),2) AS total_mult_app,
ROUND(((county_agg_apps.loan_average::NUMERIC / 0.80) / county_agg_apps.income_average::NUMERIC),2) AS income_multiple_app,
ROUND(((county_agg_orig.loan_average::NUMERIC /0.80) / county_agg_orig.income_average::NUMERIC),2) AS income_multiple_orig,

county_agg_apps.loan_average AS app_loan_avg,
county_agg_apps.income_average AS app_income_avg,
county_agg_apps.count_app AS count_apps,
county_agg_apps.value_app,
ROUND(((county_agg_orig.count_orig::NUMERIC / (county_agg_orig.count_orig::NUMERIC + county_agg_apps.count_app::NUMERIC))*100),2) AS orig_rate


FROM rates
JOIN rates2
ON concat(rates.state, rates.county) = concat(rates2.state, rates2.county)
JOIN county_agg_orig
ON concat(rates.state, rates.county) = concat(county_agg_orig.state, county_agg_orig.county)
JOIN county_agg_apps
ON concat(rates.state, rates.county) = concat(county_agg_apps.state, county_agg_apps.county);