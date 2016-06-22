
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

from lib.agg_funcs import get_CBSA_df, race_dict, income_multiple, percent_change, source_tables, app_delta_cols, orig_delta_cols, check_path
from lib.sql_text import county_years_SQL


conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor() #establish SQL cursor object

#load tract to CBSA file to get valid counties
cbsa_df = get_CBSA_df('tract_to_cbsa_2010.csv', '|') #loads CBSA data and left pads 0's
fips_list = set(cbsa_df.county.ravel()) #remove duplicates

#select data for 2000-2014
for fips in list(set(fips_list)):
	first = True
	state = fips[:2]
	county = fips[2:]
	path = "data/"+ state+"/"+fips + "/"
	year = 2000
	app_start = "county_apps_"
	orig_start = "county_orig_"

	#FIXME match this range to the LAR table list? range(len(source_tables))
	for num in range(len(source_tables)):
		app_table = app_start + str(year+num)
		orig_table = orig_start + str(year+num)
		SQL = county_years_SQL(app_table, orig_table, fips)
		df = pd.read_sql_query(SQL, conn) #load query results to dataframe

		if first == True and df.empty == False:
			out_file = df #establish outfile with first year containing data
			print("initial df for {year} and {fips}".format(year=str(year+num), fips=fips))
			first = False
		else:
			out_file = pd.concat([out_file, df]) #append a year to a county dataframe
			print("concat {year}".format(year=str(year+num)))

		#create income multiples for entire county
		income_multiple(df=out_file, action='app', numerator='loan_average_app', denominator='income_average_app')
		income_multiple(df=out_file, action='orig', numerator='loan_average_orig', denominator='income_average_orig')

		#create deltas for pattern building
		percent_change(df=out_file, col_list=app_delta_cols)
		percent_change(df=out_file, col_list=orig_delta_cols)

		for race in race_dict.keys():
			numerator_text = race_dict[race] + '_loan_average_'
			denominator_text = race_dict[race] + '_income_average_'

			income_multiple(df=out_file, race_name=race_dict[race], action='app', numerator=numerator_text + 'app', denominator=denominator_text+'app')
			income_multiple(df=out_file, race_name=race_dict[race], action='orig', numerator=numerator_text + 'orig', denominator=denominator_text+'orig')

			race_app_delta_cols = [race_dict[race] + '_' + col for col in app_delta_cols] #set column name list for race percent change columns
			race_orig_delta_cols = [race_dict[race] + '_' + col for col in orig_delta_cols] #set column name list for race percent change columns

			percent_change(df=out_file, col_list=race_app_delta_cols)#add percent change columns for applications
			percent_change(df=out_file, col_list=race_orig_delta_cols)# add percent change columns for originations

	check_path(path)
	print('writing data for {path}'.format(path=path))
	out_file.to_csv(path+"data.csv", index=None) #write dataframe to CSV for one county in state/county/file directory structure

