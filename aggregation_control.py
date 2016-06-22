import os
import pandas as pd
import psycopg2

from lib.agg_funcs import agg_df, check_path, race_agg_df, race_list, app_data_path, orig_data_path, source_tables
from lib.sql_text import *

################################
#Aggregates HMDA LAR data to the county level for each source table in the source_tables list
#Writes the aggregated data to a CSV at the specified path for applicaitons or originations
#FIXME Creates SQL tables to hold annual data for applications and originations
#FIXME Writes data from dataframes (CSV?) to annual county-level aggregate tables for applications or originations
################################

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()#instantiate cursor object to use in SQL queries


app_path = 'data/holding/applications/' #set path for CSV output
orig_path = 'data/holding/originations/'
count = 2000

for source_table in source_tables:

	counties_apps_df = agg_df(source_table, 'app', conn) #set initial application dataframe for 1 year of aggregate county data
	counties_origs_df = agg_df(source_table, 'orig', conn) #set initial origination dataframe for 1 year of aggregate county data

	for race in race_list.keys(): #iterate over race codes to aggregate county HMDA activity by race
	 	demo_app_df = race_agg_df(source_table, 'app', race, race_list[race], conn) #create a dataframe of county level aggregates for a race
	 	demo_orig_df = race_agg_df(source_table, 'orig', race, race_list[race], conn)

	 	counties_apps_df = counties_apps_df.merge(demo_app_df, on='fips', how='outer') #merge race dataframes on fips column
	 	counties_origs_df = counties_origs_df.merge(demo_orig_df, on='fips', how='outer') #merge race dataframes on fips column

 	check_path(app_path) #check if file path exists, if not then create it
 	check_path(orig_path) #check if file path exists, if not then create it
 	print('writing aggregate files for {year}'.format(year=str(count)))
	counties_apps_df.to_csv(path_or_buf=app_path+source_table+"_applications.csv", index=False) #write 1 year of aggregated data to CSV (all counties)
	counties_origs_df.to_csv(path_or_buf=orig_path+source_table+"_originations.csv", index=False) #write 1 year of aggregated data to CSV (all counties)
#/Users/roellk/Desktop/HMDA/data_analysis/data/holding/applications
#/Users/roellk/Desktop/HMDA/data_analysis/data/holding/originations/hmdalar2000_applications.csv
 	#set table names for data load
 	app_table =  'county_apps_' + str(count)#county_apps_yyyy
 	orig_table =  'county_orig_' + str(count) #county_orig_yyyy

 	#drop old tables annual aggregate tables for originations and applications
 	print('dropping old aggregate tables and creating new for year {year}'.format(year=count))
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
 	app_data = app_data_path + 'hmdalar' + str(count) + '_applications.csv' #set application aggregate data file
 	orig_data = orig_data_path + 'hmdalar' + str(count) + '_originations.csv' #set origination aggregate data file

 	#format SQL statements to load data to annual county-level aggregate tables
 	load_app_SQL = format_load_SQL(app_table, app_data)
 	load_orig_SQL = format_load_SQL(orig_table, orig_data)

 	#Copy data from CSV to table
 	cur.execute(load_app_SQL,)
 	print("loading data into {app_table}".format(app_table=app_table))
 	cur.execute(load_orig_SQL,)
 	print("loading data into {orig_table}".format(orig_table=orig_table))
 	count += 1
 	# 	#FIXME: create SQL table, copy CSV to table


#create aggregate tables for origination and application
#write data to tables by year
#make CSV files with all years per county and write to path