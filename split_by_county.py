
##########################
#06/16/2016 K. David Roell CFPB
#selects application and origination information for single family homes for a single county
#loads to data frame and writes to csv in state/county directory structure
#writes a CSV to the base directory with all counties and all states
#this process takes ~15 minutes to run
##########################

import numpy as np
import os
import pandas as pd
import psycopg2

from lib.agg_funcs import *
from lib.sql_text import county_years_SQL


conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor() #establish SQL cursor object
#FIXME is there a better way to do this?
sorted_list = ['year', 'state', 'county', 'fips', 'loan_average_app', 'loan_average_orig', 'income_average_app', 'income_average_orig', \
	'income_multiple_app', 'income_multiple_orig', 'count_app', 'count_orig', 'value_app', 'value_orig', 'loan_average_app_delta', \
	'loan_average_orig_delta', 'income_average_app_delta', 'income_average_orig_delta', 'income_multiple_app_delta', 'income_multiple_orig_delta', \
	'count_app_delta', 'count_orig_delta', 'value_app_delta', 'value_orig_delta', 'native_loan_average_app', 'native_loan_average_orig', 'native_income_average_app', \
	'native_income_average_orig', 'native_income_multiple_app', 'native_income_multiple_orig', 'native_count_app', 'native_count_orig', 'native_value_app', \
	'native_value_orig', 'native_loan_average_app_delta', 'native_loan_average_orig_delta', 'native_income_average_orig_delta', 'native_income_average_app_delta', \
	'native_income_multiple_app_delta', 'native_income_multiple_orig_delta', 'native_count_app_delta', 'native_count_orig_delta', 'native_value_app_delta', \
	'native_value_orig_delta', 'asian_loan_average_app', 'asian_loan_average_orig', 'asian_income_average_app', 'asian_income_average_orig', \
	'asian_income_multiple_app', 'asian_income_multiple_orig', 'asian_count_app', 'asian_count_orig', 'asian_value_app', 'asian_value_orig', \
	'asian_loan_average_app_delta', 'asian_loan_average_orig_delta', 'asian_income_average_app_delta', 'asian_income_average_orig_delta', 'asian_income_multiple_app_delta',\
	'asian_income_multiple_orig_delta', 'asian_count_app_delta', 'asian_count_orig_delta', 'asian_value_app_delta', 'asian_value_orig_delta',  'black_loan_average_app', \
	'black_loan_average_orig', 'black_income_average_app', 'black_income_average_orig', 'black_income_multiple_app', 'black_income_multiple_orig', \
	'black_count_app', 'black_count_orig', 'black_value_app', 'black_value_orig', 'black_loan_average_app_delta', 'black_loan_average_orig_delta', \
	'black_income_average_app_delta', 'black_income_average_orig_delta', 'black_income_multiple_app_delta', 'black_income_multiple_orig_delta','black_count_app_delta', \
	'black_count_orig_delta', 'black_value_app_delta', 'black_value_orig_delta', 'hawaiian_loan_average_app', 'hawaiian_loan_average_orig', 'hawaiian_income_average_app', \
	'hawaiian_income_average_orig', 'hawaiian_income_multiple_app', 'hawaiian_income_multiple_orig', 'hawaiian_count_app', 'hawaiian_count_orig', 'hawaiian_value_app', \
	'hawaiian_value_orig', 'hawaiian_loan_average_app_delta', 'hawaiian_loan_average_orig_delta', 'hawaiian_income_average_app_delta', 'hawaiian_income_average_orig_delta', \
	'hawaiian_income_multiple_app_delta', 'hawaiian_income_multiple_orig_delta', 'hawaiian_count_app_delta', 'hawaiian_count_orig_delta', 'hawaiian_value_app_delta', \
	'hawaiian_value_orig_delta', 'white_loan_average_app', 'white_loan_average_orig', 'white_income_multiple_app', 'white_income_multiple_orig', 'white_income_average_app', \
	'white_income_average_orig', 'white_count_app', 'white_count_orig', 'white_value_app', 'white_value_orig', 'white_loan_average_app_delta', 'white_loan_average_orig_delta', \
	'white_income_average_app_delta', 'white_income_average_orig_delta', 'white_income_multiple_app_delta', 'white_income_multiple_orig_delta', 'white_count_app_delta', \
	'white_count_orig_delta', 'white_value_app_delta', 'white_value_orig_delta']
#load tract to CBSA file to get valid counties
cbsa_df = get_CBSA_df('tract_to_cbsa_2010.csv', '|') #loads CBSA data and left pads 0's
fips_list = set(cbsa_df.county.ravel()) #remove duplicates

#select data for years in source_tables list
for fips in list(set(fips_list)):
	first = True
	state = fips[:2]
	county = fips[2:]
	path = "data/"+ state+"/"+fips + "/" #geographic hierarchy path for storing county data

	for table in source_tables:
		app_table = "county_apps_"+ table[-4:] #the year must be the last 4 characters of the table name
		orig_table = "county_orig_" + table[-4:]
		SQL = county_years_SQL(app_table, orig_table, fips) #format SQL statement text
		df = pd.read_sql_query(SQL, conn) #load query results to dataframe
		if first == True:
			out_file = df #establish outfile with first year containing data
			print("initial df for {year} and {fips}".format(year=table[-4:], fips=fips))
			first = False
		else:
			out_file = pd.concat([out_file, df], axis=0) #append a year to a county dataframe
			print("concat {year}".format(year=table[-4:]))

	#create income multiples for entire county
	income_multiple(df=out_file, action='app', numerator='loan_average_app', denominator='income_average_app')
	income_multiple(df=out_file, action='orig', numerator='loan_average_orig', denominator='income_average_orig')

	#create deltas for pattern building
	percent_change(df=out_file, col_list=app_delta_cols)
	percent_change(df=out_file, col_list=orig_delta_cols)

	for race in race_dict.keys():
		numerator_text = race_dict[race] + '_loan_average_'
		denominator_text = race_dict[race] + '_income_average_'
		#create income multiple for each race
		income_multiple(df=out_file, race_name=race_dict[race], action='app', numerator=numerator_text + 'app', denominator=denominator_text+'app')
		income_multiple(df=out_file, race_name=race_dict[race], action='orig', numerator=numerator_text + 'orig', denominator=denominator_text+'orig')

		race_app_delta_cols = [race_dict[race] + '_' + col for col in app_delta_cols] #set column name list for race percent change columns
		race_orig_delta_cols = [race_dict[race] + '_' + col for col in orig_delta_cols] #set column name list for race percent change columns

		percent_change(df=out_file, col_list=race_app_delta_cols)#add percent change columns for applications
		percent_change(df=out_file, col_list=race_orig_delta_cols)# add percent change columns for originations

	check_path(path)
	print('writing data for {path}'.format(path=path))

	#FIXME append new_metric columns
	#print out_file.head()
	out_file = out_file[sorted_list]
	out_file.to_csv(path+"data.csv", index=None) #write dataframe to CSV for one county in state/county/file directory structure

