WITH rates as (
SELECT year, state, county,
	count(rate_spread) as has_rs,
	avg(rate_spread::numeric) as avg_rate_spread
FROM hmdalar2004
WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND property_type = '1'
        AND action = '1'
        AND lien = '1'
        AND rate_spread != 'NA   '
GROUP BY year, state, county),

rates2 as (
SELECT year, state, county,
	count(rate_spread) as no_rs
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
        round(avg(amount::integer),2) as loan_average,
        round(avg(income::integer),2) as income_average,
        count(concat(agency, rid)) as count_orig,
        sum(amount::integer) as orig_value
        FROM hmdalar2004
        WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND property_type = '1'
        AND action = '1'
        AND lien = '1'
        AND income not like '%NA%'
        AND income not like '%na% '
        AND income != '    '
        AND amount != '     '
        AND amount not like '%NA%'
        AND amount not like '%na%'
        GROUP BY state, county, year ),

county_agg_apps AS (SELECT year, state, county,
        round(avg(amount::integer),2) as loan_average,
        round(avg(income::integer),2) as income_average,
        count(concat(agency, rid)) as count_apps,
        sum(amount::integer) as app_value
        FROM hmdalar2004
        WHERE loan_type = '1'
        AND loan_purpose in ('1', '3')
        AND property_type = '1'
        AND action != '1'
        AND lien = '1'
        AND income not like '%NA%'
        AND income not like '%na% '
	AND income != '    '
        AND amount != '     '
        AND amount not like '%NA%'
        AND amount not like '%na%'
        GROUP BY state, county, year )
        
SELECT rates.year, rates.state, rates.county, rates.has_rs, rates2.no_rs, 
round(rates.avg_rate_spread, 2) as avg_rs, 
round(((rates.has_rs::numeric / (rates2.no_rs::numeric + rates.has_rs::numeric))*100),2) AS rs_ratio,
county_agg_orig.loan_average, 
county_agg_orig.income_average, 
county_agg_orig.count_orig, 
county_agg_orig.orig_value,
round(((county_agg_orig.loan_average::numeric/.8) / county_agg_orig.income_average::numeric),2) AS income_multiple,
county_agg_apps.loan_average AS app_loan_avg,
county_agg_apps.income_average AS app_income_avg,
county_agg_apps.count_apps AS count_apps,
county_agg_apps.app_value,
ROUND(((county_agg_orig.count_orig::numeric / (county_agg_orig.count_orig::numeric + county_agg_apps.count_apps::numeric))*100),2) AS orig_rate


FROM rates
JOIN rates2
ON concat(rates.state, rates.county) = concat(rates2.state, rates2.county)
JOIN county_agg_orig
ON concat(rates.state, rates.county) = concat(county_agg_orig.state, county_agg_orig.county)
JOIN county_agg_apps
ON concat(rates.state, rates.county) = concat(county_agg_apps.state, county_agg_apps.county)
;
