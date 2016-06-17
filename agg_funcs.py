#create application tables: county level aggregates by year
import os
import psycopg2
import pandas as pd
def agg_SQL(source_table, action):
	#FIXME change action to a passed format variable
	"""returns SQL_base with source table formatted into the query text
		Used to aggregate LAR data to the county level for a single year LAR table
		action is used to determine if applications or originations are selected """
	if action == 'app':
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
		AND action != '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'
		GROUP BY year, state, county;"""

	elif action == 'orig':
		SQL_base ="""SELECT
		year
		,state
		,county
		,CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS loan_average_orig
		,ROUND(AVG(income::INTEGER),2) AS income_average_orig
		,COUNT(concat(agency, rid)) AS count_orig
		,SUM(amount::INTEGER) AS value_orig
		FROM {source_table}
		WHERE
		          loan_type = '1'
		AND action = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'
		GROUP BY year, state, county;"""

	else:
		print("invalid action code")

	return SQL_base.format(source_table=source_table)

def agg_demo_SQL(source_table, race_code, race_name, action):
	"""returns SQL_base with source table, race code and race name formatted into the query text
		Used to aggregate LAR data to the county level for a single year LAR table for the selected race"""
	if action == 'app':
		SQL_base = """SELECT
			 CONCAT(state, county) AS fips
			,ROUND(AVG(amount::INTEGER),2) AS {race_name}_loan_average_app
			,ROUND(AVG(income::INTEGER),2) AS {race_name}_income_average_app
			,COUNT(concat(agency, rid)) AS {race_name}_count_app
			,SUM(amount::INTEGER) AS {race_name}_value_app
			FROM {source_table}
			WHERE
			          race = '{race_code}'
			AND action != '1'
			AND loan_type = '1'
			AND loan_purpose in ('1', '3')
			AND amount not like '%NA%'
			AND income not like '%NA%'
			AND amount not like '%na%'
			AND income not like '%na%'
			GROUP BY CONCAT(state, county);"""

	elif action == 'orig':
		SQL_base = """SELECT
			 CONCAT(state, county) AS fips
			,ROUND(AVG(amount::INTEGER),2) AS {race_name}_loan_average_orig
			,ROUND(AVG(income::INTEGER),2) AS {race_name}_income_average_orig
			,COUNT(concat(agency, rid)) AS {race_name}_count_orig
			,SUM(amount::INTEGER) AS {race_name}_value_orig
			FROM {source_table}
			WHERE
			          race = '{race_code}'
			AND action != '1'
			AND loan_type = '1'
			AND loan_purpose in ('1', '3')
			AND amount not like '%NA%'
			AND income not like '%NA%'
			AND amount not like '%na%'
			AND income not like '%na%'
			GROUP BY CONCAT(state, county);
			"""
	else:
		print("invalid action") #raise valueError

	return SQL_base.format(source_table=source_table, race_code=race_code, race_name=race_name)

def drop_table(table):
	"""returns SQL text formatted to drop the selected table if it exists"""
	drop_SQL = """DROP TABLE IF EXISTS {drop_table}; COMMIT;"""
	return drop_SQL.format(drop_table=table)

def race_agg_df(source_table, action, race_code, race_name, conn):
	"""Queries a PostgreSQL table and aggregates LAR data to the county level for the selected race
		uses app_agg_demo_SQL and race to produce the formatted query text"""

	demo_SQL = agg_demo_SQL(source_table, action='app', race_code=race_code, race_name=race_name)

	print("\n\nexecuting race {action} aggregation on {table} for {race}\n".format(action=action, table=source_table, race=race_name))
	try:
		return pd.read_sql_query(demo_SQL, conn) #load query results to dataframe and return it
	except psycopg2.ProgrammingError as e: #catch empty dataframe errors
		print("no results to fetch for {race} from {table}",format(table=source_table, race=race_name))
		#do I need an empty df return?

def agg_df(source_table, action, conn):
	""" """
	app_SQL = agg_SQL(source_table, action) #format SQL query with data source table
	try: #read results of SQL query into data frame
		print("executing {action} aggregation on {table}".format(action=action, table=source_table))
		return pd.read_sql_query(app_SQL, conn) #query LAR database and load county level aggregates to a dataframe
	except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
		print("no results to fetch for table {table}".format(table=source_table), e)

def check_path(path):
	if not os.path.exists(path):
		os.makedirs(path)