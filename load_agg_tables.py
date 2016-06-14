#prototype code to clean pandas dataframes from CSVs and load county aggregate tables to postgres
import os
import pandas as pd
import psycopg2

#establish DB connection
conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor() #establish psycopg2 cursor object

def create_table_SQL(table, data, action):
	SQL = """DROP TABLE IF EXISTS {drop_table}; COMMIT;
		CREATE TABLE {create_table} (
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
	return SQL.format(drop_table=table, create_table=table, action=action)

def format_load_SQL(table, data):
	SQL = """COPY {table} FROM '{path}' DELIMITER ',' CSV HEADER; COMMIT;"""
	return SQL.format(table=table, path=data)


app_path = "/Users/roellk/Desktop/HMDA/data_analysis/data/holding/applications/" #psycopg2 requires an absolute path from which to copy files
orig_path = '/Users/roellk/Desktop/HMDA/data_analysis/data/holding/originations/'
year = 2000 #set start year for table naming and CSV file reading
app_start = 'county_apps_' #set start point for application table and data file names
orig_start = 'county_orig_' #set start point for origination table and data file names

for num in range(15): #loop over 15 years to create tables and load county aggregate data
	app_data = app_path + app_start + str(year + num) + '.csv' #set application aggregate data file
	orig_data = orig_path + orig_start + str(year + num) + '.csv' #set origination data file source
	app_table = app_start + str(year + num) #set application table name
	orig_table = orig_start + str(year +num) #set origination table name

	#format SQL for dropping and creating tables
	create_app_table_SQL = create_table_SQL(app_table, app_data, 'app')
	create_orig_table_SQL = create_table_SQL(orig_table, orig_data, 'orig')

	#execute create table SQL commands
	cur.execute(create_app_table_SQL,)
	cur.execute(create_orig_table_SQL,)

	#format data copy SQL statements
	load_app_SQL = format_load_SQL(app_table, app_data)
	load_orig_SQL = format_load_SQL(orig_table, orig_data)

	#Copy data from CSV to table
	cur.execute(load_app_SQL,)
	cur.execute(load_orig_SQL,)

print('finished')