#creates annual tables with aggregate applications and originations by county
#rows with data not parsing correctly were deleted ~30 per year from 2000-2006

import psycopg2

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()

#List output tables
hmda_app_tables = ['county_apps_2000','county_apps_2001','county_apps_2002','county_apps_2003','county_apps_2004','county_apps_2005',
				  'county_apps_2006','county_apps_2007','county_apps_2008','county_apps_2009','county_apps_2010','county_apps_2011',
				  'county_apps_2012','county_apps_2013','county_apps_2014']

hmda_orig_tables = ['county_orig_2000','county_orig_2001','county_orig_2002','county_orig_2003','county_orig_2004','county_orig_2005',
				  'county_orig_2006','county_orig_2007','county_orig_2008','county_orig_2009','county_orig_2010','county_orig_2011',
				  'county_orig_2012','county_orig_2013','county_orig_2014']

#initialize source table variable name
table_start = 'hmdalar'
year = 2000

#create application tables: county level aggregates by year
for num in range(15):
	source_table= table_start + str(year + num) #increment source table
	result_table=hmda_app_tables[num] #increment result table
	print result_table, source_table #display progress in UI
	SQL1 = """DROP TABLE IF EXISTS {drop_table};
		commit;
		"""
	SQL2 = """CREATE TABLE {result_table} AS

		SELECT year, state, county, CONCAT(state, county) AS fips
		ROUND(AVG(amount::INTEGER),2) AS loan_average_app,
		ROUND(AVG(income::INTEGER),2) AS income_average_app,
		COUNT(concat(agency, rid)) AS count_app,
		SUM(amount::INTEGER) AS value_app

		FROM {source_table}

		WHERE loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

    		GROUP BY year, state, county;commit;
		"""
	SQL=SQL1.format(drop_table=result_table) + SQL2.format(source_table=source_table, result_table =result_table)
	print SQL
	cur.execute(SQL)

#create origination tables: county level aggregates by year
for num in range(15):
	source_table= table_start + str(year + num)
	result_table=hmda_orig_tables[num]
	print result_table, source_table
	SQL1 = """DROP TABLE IF EXISTS {drop_table};
		commit;
		"""
	SQL2 = """CREATE TABLE {result_table} AS

		SELECT year, state, county,
		ROUND(AVG(amount::INTEGER),2) AS loan_average_orig,
		ROUND(AVG(income::INTEGER),2) AS income_average_orig,
		COUNT(concat(agency, rid)) AS count_orig,
		SUM(amount::INTEGER) AS value_orig,
		CONCAT(state, county) AS fips

		FROM {source_table}

		WHERE
		loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND action = '1'
		AND amount not like '%NA%'
		AND income not like '%NA%'
 		AND amount not like '%na%'
 		AND income not like '%na%'

    		GROUP BY year, state, county;
		"""
	SQL=SQL1.format(drop_table=result_table) + SQL2.format(source_table=source_table, result_table =result_table)
	print SQL
	cur.execute(SQL)

print "done"

