import os
import pandas as pd
import psycopg2
from agg_funcs import *

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()#instantiate cursor object to use in SQL queries

race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}

source_tables = ['hmdalar2000', 'hmdalar2001', 'hmdalar2002', 'hmdalar2003', 'hmdalar2004', 'hmdalar2005', 'hmdalar2006',
		'hmdalar2007', 'hmdalar2008', 'hmdalar2009', 'hmdalar2010', 'hmdalar2011', 'hmdalar2012', 'hmdalar2013', 'hmdalar2014']

app_path = 'data/holding/applications/' #set path for CSV output
orig_path = 'data/holding/originations/'
app_data_path = "/Users/roellk/Desktop/HMDA/data_analysis/data/holding/applications/" #psycopg2 requires an absolute path from which to copy files
orig_data_path = '/Users/roellk/Desktop/HMDA/data_analysis/data/holding/originations/'

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
	counties_apps_df.to_csv(path_or_buf=app_path+source_table+"_applications.csv", index=False) #write 1 year of aggregated data to CSV (all counties)
	counties_origs_df.to_csv(path_or_buf=orig_path+source_table+"_originations.csv", index=False) #write 1 year of aggregated data to CSV (all counties)
 	# 	load_to_sql_csv = base_counties_df.to_csv(index=False) #load this file to SQL
 	# 	#FIXME: create SQL table, copy CSV to table


#create aggregate tables for origination and application
#write data to tables by year
#make CSV files with all years per county and write to path