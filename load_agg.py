
import psycopg2

#connects to database
#creates annual tables with aggregate applications and originations by county
#rows with data not parsing correctly were deleted


conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()

hmda_app_tables = ['county_apps_2000','county_apps_2001','county_apps_2002','county_apps_2003','county_apps_2004','county_apps_2005',
				  'county_apps_2006','county_apps_2007','county_apps_2008','county_apps_2009','county_apps_2010','county_apps_2011',
				  'county_apps_2012','county_apps_2013','county_apps_2014']

hmda_orig_tables = ['county_orig_2000','county_orig_2001','county_orig_2002','county_orig_2003','county_orig_2004','county_orig_2005',
				  'county_orig_2006','county_orig_2007','county_orig_2008','county_orig_2009','county_orig_2010','county_orig_2011',
				  'county_orig_2012','county_orig_2013','county_orig_2014']

table_start = 'hmdalar'
year = 2000

for num in range(0,15):
	source_table= table_start + str(year + num)
	result_table=hmda_app_tables[num]
	print result_table, source_table
	SQL1 = """DROP TABLE IF EXISTS {drop_table};
		commit;
		"""
	SQL2 = """CREATE TABLE {result_table} AS

		SELECT year, state, county,
		ROUND(avg(amount::INTEGER),2) AS loan_average_app,
		ROUND(avg(income::INTEGER),2) AS income_average_app,
		COUNT(concat(agency, rid)) AS count_app,
		SUM(amount::INTEGER) AS value_app,
		CONCAT(state, county) AS fips

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



for num in range(0,15):
	source_table= table_start + str(year + num)
	result_table=hmda_orig_tables[num]
	print result_table, source_table
	SQL1 = """DROP TABLE IF EXISTS {drop_table};
		commit;
		"""
	SQL2 = """CREATE TABLE {result_table} AS

		SELECT year, state, county,
		ROUND(avg(amount::INTEGER),2) AS loan_average_app,
		ROUND(avg(income::INTEGER),2) AS income_average_app,
		COUNT(concat(agency, rid)) AS count_app,
		SUM(amount::INTEGER) AS value_app,
		CONCAT(state, county) AS fips

		FROM {source_table}

		WHERE loan_type = '1'
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

