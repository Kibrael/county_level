##############################
#06/16/2016 K. David Roell CFPB
#Contains functions that format SQL statements for use in aggregating HMDA LAR data
##############################
def add_column(table='county_apps_2000', column='new_col', data_type='varchar(10)'):
	"""formats a SQL statement to add a new column to an existing SQL table"""

	SQL = """ALTER TABLE {table} ADD COLUMN {column} {data_type}"""
	return SQL.format(table=table, column=column, data_type=data_type)


def agg_new_col(source_table='hmdalar2000', pg_func='sum', column='income', action_taken="action != '1'"):
	"""Returns a SQL statement that aggregates one column of data to the county level for a single year of HMDA data
	the default values return SQL for calculating the sum of incomes by county"""

	metric_list = ['sum', 'count', 'stddev_samp', 'stddev_pop', 'max', 'min'] #list of implemented PostgreSQL aggregate functions

	if pg_func in metric_list:
		metric_text = pg_func + "(" + column + "::real)"
		SQL = """SELECT
			year,
			state,
			county,
			concat(state,county) AS fips,
			{metric_text}
			FROM {source_table}
			WHERE
			          loan_type = '1'
			AND {action_taken}
			AND loan_purpose in ('1', '3')
			AND amount not like '%NA%'
			AND income not like '%NA%'
			AND amount not like '%na%'
			AND income not like '%na%'
			GROUP BY year, state, county;
		"""
		return SQL.format(source_table=source_table, metric_text=metric_text, action_taken=action_taken)
	else:
		print('the SQL function you selected is not currently supported')
		raise NotImplementedError


def agg_SQL(source_table, action):
	#FIXME change action to a passed format variable
	"""returns SQL_base with source table formatted into the query text
	    Used to aggregate LAR data to the county level for a single year LAR table
	    action is used to determine if applications or originations are selected """

	if action == 'app':
		action_taken = "action != '1' " #all other action codes
	elif action == 'orig':
		action_taken = "action = '1' "#origination action code
	else:
		print('invalid action selection')
		#FIXME raise valueerror?

	SQL_base ="""SELECT
	year
	,state
	,county
	,CONCAT(state, county) AS fips
	,ROUND(AVG(amount::INTEGER),2) AS loan_average_{action}
	,ROUND(AVG(income::INTEGER),2) AS income_average_{action}
	,COUNT(concat(agency, rid)) AS count_{action}
	,SUM(amount::INTEGER) AS value_{action}
	FROM {source_table}
	WHERE
	          loan_type = '1'
	AND {action_taken}
	AND loan_purpose in ('1', '3')
	AND amount not like '%NA%'
	AND income not like '%NA%'
	AND amount not like '%na%'
	AND income not like '%na%'
	GROUP BY year, state, county;"""

	return SQL_base.format(source_table=source_table, action_taken=action_taken)

def agg_demo_SQL(source_table, race_code, race_name, action):
	"""returns SQL_base with source table, race code and race name formatted into the query text
	    Used to aggregate LAR data to the county level for a single year LAR table for the selected race"""
	if action == 'app':
		action_taken = "action != '1' "
	elif action == 'orig':
		action_takne = "action = '1' "
	else:
		print("invalid action")
		#FIXME raise valueError

	SQL_base = """SELECT
		 CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS {race_name}_loan_average_app
		,ROUND(AVG(income::INTEGER),2) AS {race_name}_income_average_app
		,COUNT(concat(agency, rid)) AS {race_name}_count_app
		,SUM(amount::INTEGER) AS {race_name}_value_app
		FROM {source_table}
		WHERE
		          race = '{race_code}'
		AND {action_taken}
		AND loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'
		GROUP BY CONCAT(state, county);"""

	return SQL_base.format(source_table=source_table, race_code=race_code, race_name=race_name, action_taken=action_taken)

def create_aggregate_table_SQL(table, action):
	"""Returns a formatted SQL statement to create a table holding a single year's aggregated application or origination data for one county"""
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
		no_co_app_value_{action} real)
		PRIMARY KEY(fips);
		COMMIT;"""
	return SQL.format(create_table=table, action=action)

def county_years_SQL(app_table, orig_table, fips):
	"""Returns a SQL select statement that combines columns from tables containing application and origination data for a specified county"""
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



def drop_table(table):
	"""Returns a SQL statement that drops the passed table if it exists"""
	drop_SQL = """DROP TABLE IF EXISTS {drop_table}; COMMIT;"""
	return drop_SQL.format(drop_table=table)

def format_load_SQL(table, data):
	"""Returns a formatted SQL statement to load a CSV file into the specified table """
	SQL = """COPY {table} FROM '{path}' DELIMITER ',' CSV HEADER; COMMIT;"""
	return SQL.format(table=table, path=data)

def insert_metric(table, data, merge_key):
	"""Formats a SQL statement to Insert a single column into an existing table by merging on the merge_key"""
	SQL = """INSERT
	"""
