################################
#06/16/2016 K. David Roell CFPB
#Aggregates HMDA LAR data to the county level for each source table in the source_tables list FIXME: is this clear?
#Writes the aggregated data to a CSV at the specified path for applications or originations
#FIXME Creates SQL tables to hold annual data for applications and originations
#FIXME Writes data from dataframes (CSV?) to annual county-level aggregate tables for applications or originations
#FIXME standardize order of columns in CSV and Tables
################################

import os
import pandas as pd
import psycopg2

from lib.agg_funcs import agg_df, check_path, race_agg_df, race_list, app_data_path, orig_data_path, source_tables, app_path, orig_path
from lib.sql_text import *

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()#instantiate cursor object to use in SQL queries
cbsa_df = pd.read_csv('tract_to_cbsa_2010.csv', sep='|')
fips_list = list(set(cbsa_df.county.ravel()))

for source_table in source_tables:
	counties_apps_df = agg_df(source_table=source_table, action='app', conn=conn) #set initial application dataframe for 1 year of aggregate county data
	counties_origs_df = agg_df(source_table=source_table, action='orig', conn=conn) #set initial origination dataframe for 1 year of aggregate county data

	for race in race_list.keys(): #iterate over race codes to aggregate county HMDA activity by race
		demo_app_df = race_agg_df(source_table, 'app', race, race_list[race], conn) #create a dataframe of county level aggregates for a race
		demo_orig_df = race_agg_df(source_table, 'orig', race, race_list[race], conn)

		counties_apps_df = counties_apps_df.merge(demo_app_df, on='fips', how='outer') #merge race dataframes on fips column
		counties_origs_df = counties_origs_df.merge(demo_orig_df, on='fips', how='outer') #merge race dataframes on fips column

	check_path(app_path) #check if file path exists, if not then create it
	check_path(orig_path) #check if file path exists, if not then create it

	#drop rows with invalid fips

	# for fips in list(counties_apps_df.fips.ravel()):
	# 	print(list(counties_apps_df.fips.ravel()))
	# 	if fips not in fips_list:
	# 		print(fips + " not in list")
	# 		#print(fips_list)
	# 		counties_apps_df.drop(fips, axis=1, inplace=True)
	# for fips in counties_origs_df.fips:
	# 	if fips not in fips_list:
	# 		counties_origs_df.drop(fips, axis=0, inplace=True)
	#counties_apps_df = counties_apps_df.sort(axis=1, columns=sorted_app_cols)
	#FIXME ensure columns are dtyped appropriately IE state as object or varchar 2
	print('writing aggregate files for {year}'.format(year=source_table[-4:]))
	counties_apps_df.to_csv(path_or_buf=app_path+source_table+"_applications.csv", index=False) #write 1 year of aggregated data to CSV (all counties)
	counties_origs_df.to_csv(path_or_buf=orig_path+source_table+"_originations.csv", index=False) #write 1 year of aggregated data to CSV (all counties)

	#set table names for data load
	app_table =  'county_apps_' + source_table[-4:]#county_apps_yyyy
	orig_table =  'county_orig_' + source_table[-4:] #county_orig_yyyy

	#drop old tables annual aggregate tables for originations and applications
	print('dropping old aggregate tables and creating new for year {year}'.format(year=source_table[-4:]))
	cur.execute(drop_table(app_table),)
	cur.execute(drop_table(orig_table),)

	#format SQL for  creating annual county-level aggregate tables for applicaiton and origination
	create_app_table_SQL = create_aggregate_table_SQL(app_table, 'app')
	create_orig_table_SQL = create_aggregate_table_SQL(orig_table, 'orig')

	#execute create table SQL commands to create annual county-level aggregate tables for application and origination
	cur.execute(create_app_table_SQL,)
	print("creating {table}".format(table=app_table))
	cur.execute(create_orig_table_SQL,)
	print("creating {table}".format(table=orig_table))

	#set data to load
	app_data = app_data_path + 'hmdalar' + source_table[-4:] + '_applications.csv' #set application aggregate data file
	orig_data = orig_data_path + 'hmdalar' + source_table[-4:] + '_originations.csv' #set origination aggregate data file

	#format SQL statements to load data to annual county-level aggregate tables
	load_app_SQL = format_load_SQL(app_table, app_data)
	load_orig_SQL = format_load_SQL(orig_table, orig_data)

	#Copy data from CSV to table
	cur.execute(load_app_SQL,)
	print("loading data into {app_table}".format(app_table=app_table))
	cur.execute(load_orig_SQL,)
	print("loading data into {orig_table}".format(orig_table=orig_table))




#create aggregate tables for origination and application
#write data to tables by year
#make CSV files with all years per county and write to path