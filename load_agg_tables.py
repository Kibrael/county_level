#prototype code to clean pandas dataframes from CSVs and load county aggregate tables to postgres
import os
import pandas as pd
import psycopg2

from agg_funcs import create_aggregate_table_SQL
from agg_funcs import format_load_SQL
from agg_funcs import drop_table

#establish DB connection
conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor() #establish psycopg2 cursor object

app_data_path = "/Users/roellk/Desktop/HMDA/data_analysis/data/holding/applications/" #psycopg2 requires an absolute path from which to copy files
orig_data_path = '/Users/roellk/Desktop/HMDA/data_analysis/data/holding/originations/'
year = 2000 #set start year for table naming and CSV file reading
app_start = 'county_apps_' #set start point for application table and data file names
orig_start = 'county_orig_' #set start point for origination table and data file names

for num in range(15): #loop over 15 years to create tables and load county aggregate data
	app_data = app_data_path + 'hmdalar' + str(year + num) + '_applications.csv' #set application aggregate data file
	orig_data = orig_data_path + 'hmdalar' + str(year + num) + '_originations.csv' #set origination data file source
	app_table = app_start + str(year + num) #set application table name
	orig_table = orig_start + str(year +num) #set origination table name

	#drop old tables
	cur.execute(drop_table(app_table),)
	cur.execute(drop_table(orig_table),)

	#format SQL for  creating tables
	create_app_table_SQL = create_aggregate_table_SQL(app_table, 'app')
	create_orig_table_SQL = create_aggregate_table_SQL(orig_table, 'orig')

	#execute create table SQL commands
	cur.execute(create_app_table_SQL,)
	print("creating {table}".format(table=app_table))
	cur.execute(create_orig_table_SQL,)
	print("creating {table}".format(table=orig_table))



	load_app_SQL = format_load_SQL(app_table, app_data)
	load_orig_SQL = format_load_SQL(orig_table, orig_data)

	#Copy data from CSV to table
	cur.execute(load_app_SQL,)
	print("loading data into {app_table}".format(app_table=app_table))
	cur.execute(load_orig_SQL,)
	print("loading data into {orig_table}".format(orig_table=orig_table))
print('finished')