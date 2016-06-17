#combines load_agg.py and get_demographic_aggregates.py to produce CSV and SQL tables of applications aggregated by county
#creates annual tables with aggregate applications and originations by county
#rows with data not parsing correctly were deleted ~30 per year from 2000-2006

import os
import pandas as pd
import psycopg2

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()



#create application tables: county level aggregates by year
def app_agg_SQL(source_table):
	"""returns SQL_base with source table formatted into the query text
		Used to aggregate LAR data to the county level for a single year LAR table """
	SQL_base ="""SELECT
		year
		,state
		,county
		,CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS loan_average_app
		,ROUND(AVG(income::INTEGER),2) AS income_average_app
		,COUNT(concat(agency, rid)) AS count_app
		,SUM(amount::INTEGER) AS value_app
		FROM {source_table}
		WHERE
		          loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

		GROUP BY year, state, county;"""
	return SQL_base.format(source_table=source_table)

def app_agg_demo_SQL(source_table, race_code, race_name):
	"""returns SQL_base with source table, race code and race name formatted into the query text
		Used to aggregate LAR data to the county level for a single year LAR table for the selected race"""
	SQL_base = """SELECT
		 CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS {race_name}_loan_average_app
		,ROUND(AVG(income::INTEGER),2) AS {race_name}_income_average_app
		,COUNT(concat(agency, rid)) AS {race_name}_count_app
		,SUM(amount::INTEGER) AS {race_name}_value_app
		FROM {source_table}
		WHERE
		          race = '{race_code}'
		AND loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

		GROUP BY CONCAT(state, county);
		"""
	return SQL_base.format(source_table=source_table, race_code=race_code, race_name=race_name)

def drop_table(table):
	"""returns SQL text formatted to drop the selected table if it exists"""
	drop_SQL = """DROP TABLE IF EXISTS {drop_table}; COMMIT;"""
	return drop_SQL.format(drop_table=table)

def race_agg_df(race_code, race_name):
	"""Queries a PostgreSQL table and aggregates LAR data to the county level for the selected race
		uses app_agg_demo_SQL and race to produce the formatted query text"""

	demo_SQL = app_agg_demo_SQL(source_table, race_code=race_code, race_name=race_name)

	print("\n\nexecuting race aggregation on {table} for race {race}\n".format(table=source_table, race=race_list[race]))
	try:
		return pd.read_sql_query(demo_SQL, conn) #load query results to dataframe and return it
	except psycopg2.ProgrammingError as e: #catch empty dataframe errors
		print("no results to fetch for {race} from {table}",format(table=source_table, race=race_list[race]))
		#do I need an empty df return?

def agg_df(source_table):
	""" """
	app_SQL = app_agg_SQL(source_table) #format SQL query with data source table
	try: #read results of SQL query into data frame
		return pd.read_sql_query(app_SQL, conn) #query LAR database and load county level aggregates to a dataframe
	except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
		print("no results to fetch for table {table}".format(table=source_table), e)

def check_path(path):
	if not os.path.exists(path):
		os.makedirs(path)


race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}

table_start = 'hmdalar' #initialize source table variable name
app_table_start = 'county_apps_'
path = 'data/holding/applications/' #set path for CSV output
year = 2000
for num in range(1):
	source_table = table_start + str(year + num) #increment source table
	app_table = app_table_start + str(year + num) #increment applications aggregate table
	print("\n\n aggregating {source} data into {target} ".format(source=source_table, target=app_table))

	base_counties_df = agg_df(source_table) #set initial dataframe for county aggregates of 1 year LAR data

	for race in race_list.keys(): #iterate over race codes
		demo_df = race_agg_df(race, race_list[race])
		base_counties_df = base_counties_df.merge(demo_df, on='fips', how='outer') #merge dataframes on fips column

	check_path(path)
	base_counties_df.to_csv(path_or_buf=path+app_table+".csv", index=False)
		#base_counties_df2.to_csv(path_or_buf=path+app_table+"2.csv", index=False)
	load_to_sql_csv = base_counties_df.to_csv(index=False) #load this file to SQL
	#FIXME: create SQL table, copy CSV to table





	#FIXME create origination tables: county level aggregates by year
print("done")

