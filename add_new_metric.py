##########################
#06/24/2016 K. David Roell CFPB
#Aggregates a new metric from base data
#Adds metric column to data tables and county-level CSV files
#
###########################
import os
import pandas as pd
import psycopg2

from lib.sql_text import add_column, agg_new_col, select_agg_table, empty_table, format_load_SQL
from lib.agg_funcs import *

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()#instantiate cursor object to use in SQL queries

#FIXME create config section/file for new metric creation
metric_col = 'income'
agg_function='sum'
new_col_name = 'test_col'
app = "action != '1'"
orig = "action = '1'"
dtype='real'
#format SQL for new metric
for table in source_tables:
	app_agg_table = "county_apps_" + table[-4:]
	orig_agg_table = "county_orig_"+ table[-4:]
	app_metric_SQL = agg_new_col(source_table=table, pg_func=agg_function, action_taken=app, column=metric_col, new_col=new_col_name)
	orig_metric_SQL = agg_new_col(source_table=table, pg_func=agg_function, action_taken=orig, column=metric_col, new_col=new_col_name)

	#query source table for new aggregate metric
	app_metric_df = pd.read_sql_query(app_metric_SQL, conn)
	print('aggregating applications data')
	orig_metric_df = pd.read_sql_query(orig_metric_SQL, conn)
	print('aggregating originations data')
	#read data for all counties for a single year
	holding_app_df = pd.read_csv(app_path + table+'_applications.csv')
	holding_orig_df = pd.read_csv(orig_path + table+'_originations.csv')

	#merge new metric with previously aggregated data
	new_app_holding_df = holding_app_df.merge(app_metric_df, on='fips', how='outer')
	new_orig_holding_df = holding_orig_df.merge(orig_metric_df, on='fips', how='outer')

	#write new aggregate CSV to holding
	new_app_holding_df.to_csv(app_path+table+'_applications.csv', index=None)
	new_orig_holding_df.to_csv(orig_path+table+'_originations.csv', index=None)

	#add additional column to table schema
	new_col_sql = add_column(table=app_agg_table, column=new_col_name, data_type=dtype) #format SQL statement
	cur.execute(new_col_sql,) #execute SQL statement
	new_col_sql = add_column(table=orig_agg_table, column=new_col_name, data_type=dtype) #format SQL statement
	cur.execute(new_col_sql,) #execute SQL statement

	#delete table data
	del_data_sql = empty_table(table=app_agg_table)#format SQL statement to drop table data
	cur.execute(del_data_sql) #drop table data
	del_data_sql = empty_table(table=orig_agg_table)#format SQL statement to drop table data
	cur.execute(del_data_sql) #drop table data

	#load new data to aggregate table
	load_app_SQL = format_load_SQL(app_agg_table, app_data_path+table+ '_applications.csv')
	cur.execute(load_app_SQL,)
	load_app_SQL = format_load_SQL(app_agg_table, app_data_path+table+ '_originations.csv')
	cur.execute(load_app_SQL,)
