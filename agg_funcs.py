#create application tables: county level aggregates by year
import os
import psycopg2
import pandas as pd

race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}
race_dict = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white'}
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

def format_load_SQL(table, data):
	SQL = """COPY {table} FROM '{path}' DELIMITER ',' CSV HEADER; COMMIT;"""
	return SQL.format(table=table, path=data)

def create_aggregate_table_SQL(table, action):
	SQL = """CREATE TABLE {create_table} (
		year real,
		state varchar(2),
		county varchar(5),
		fips varchar(5),
		loan_average_{action} real,
		income_average_{action} real,
		count_{action} real,
		value_{action} real,
		native_loan_average_{action} real,
		native_income_average_{action} real,
		native_count_{action} real,
		native_value_{action} real,
		black_loan_average_{action} real,
		black_income_average_{action} real,
		black_count_{action} real,
		black_value_{action} real,
		asian_loan_average_{action} real,
		asian_income_average_{action} real,
		asian_count_{action} real,
		asian_value_{action} real,
		white_loan_average_{action} real,
		white_income_average_{action} real,
		white_count_{action} real,
		white_value_{action} real,
		hawaiian_loan_average_{action} real,
		hawaiian_income_average_{action} real,
		hawaiian_count_{action} real,
		hawaiian_value_{action} real,
		not_applicable_loan_average_{action} real,
		not_applicable_income_average_{action} real,
		not_applicable_count_{action} real,
		not_applicable_value_{action} real,
		not_provided_loan_average_{action} real,
		not_provided_income_average_{action} real,
		not_provided_count_{action} real,
		not_provided_value_{action} real,
		no_co_app_loan_average_{action} real,
		no_co_app_income_average_{action} real,
		no_co_app_count_{action} real,
		no_co_app_value_{action} real);
		COMMIT;"""
	return SQL.format(create_table=table, action=action)

def county_years_SQL(app_table, orig_table, fips):
	SQL = """SELECT
		app.year
		,app.state
		,app.county
		,app.fips
		,loan_average_orig
		,income_average_orig
		,count_orig
		,value_orig
		,native_loan_average_orig
		,native_income_average_orig
		,native_count_orig
		,native_value_orig
		,black_loan_average_orig
		,black_income_average_orig
		,black_count_orig
		,black_value_orig
		,asian_loan_average_orig
		,asian_income_average_orig
		,asian_count_orig
		,asian_value_orig
		,white_loan_average_orig
		,white_income_average_orig
		,white_count_orig
		,white_value_orig
		,hawaiian_loan_average_orig
		,hawaiian_income_average_orig
		,hawaiian_count_orig
		,hawaiian_value_orig
		,loan_average_app
		,income_average_app
		,count_app
		,value_app
		,native_loan_average_app
		,native_income_average_app
		,native_count_app
		,native_value_app
		,black_loan_average_app
		,black_income_average_app
		,black_count_app
		,black_value_app
		,asian_loan_average_app
		,asian_income_average_app
		,asian_count_app
		,asian_value_app
		,white_loan_average_app
		,white_income_average_app
		,white_count_app
		,white_value_app
		,hawaiian_loan_average_app
		,hawaiian_income_average_app
		,hawaiian_count_app
		,hawaiian_value_app
		FROM {app_table} AS app
		FULL OUTER JOIN {orig_table} AS orig
		ON app.fips = orig.fips
		WHERE app.fips = cast({fips} AS VARCHAR(5))"""
	return SQL.format(app_table=app_table, orig_table=orig_table, fips=fips)

def get_CBSA_df(CBSA_file, seperator):
	cbsa_df = pd.read_csv(CBSA_file, sep=seperator)
	cbsa_df.county = cbsa_df.county.map(lambda x: str(x).zfill(5)) #left pad with 0's to make valid FIPS codes
	return cbsa_df

