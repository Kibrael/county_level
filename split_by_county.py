
import numpy as np
import os
import pandas as pd
import psycopg2

from agg_funcs import county_years_SQL
from agg_funcs import get_CBSA_df
from agg_funcs import race_dict

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor() #establish SQL cursor object

#load tract to CBSA file to get valid counties
cbsa_df = get_CBSA_df('tract_to_cbsa_2010.csv', '|')
fips_list = set(cbsa_df.county.ravel()) #remove duplicates

##########################
#selects application and origination information for single family homes for a single county
#loads to data frame and writes to csv in state/county directory structure
#writes a CSV to the base directory with all counties and all states
#this process takes ~15 minutes to run
##########################

def income_multiple(df=None, race_name=None, action='app', numerator=None, denominator=1):
	"""calculates the income multiple with the assumption that LTV is 80% at origination"""
	if race_name is None:
		col_name = 'income_multiple_' + action
	else:
		col_name = race_name + '_' + income_multiple + action
	df[col_name] = (df[numerator] / 0.80) / df[denominator]


def percent_change(df=None, col_list=None):
	for col in col_list
		out_col = col + '_delta'
		df[out_col] = df[col].pct_change() * 100

#select data for 2000-2014
for fips in list(set(fips_list)):
	first = True
	state = fips[:2]
	county = fips[2:]
	path = "data/"+ state+"/"+fips + "/"
	year = 2000
	app_start = "county_apps_"
	orig_start = "county_orig_"

	for num in range(15):
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
		delta_cols = ['loan_avg_app', 'income_average_app', 'count_app', 'value_app', 'income_mult']
		percent_change(df=out_file, col_list=delta_cols)
		#out_file['loan_avg_app_delta'] = out_file.loan_average_app.pct_change()*100
		#out_file['income_average_app_delta'] = out_file.income_average_app.pct_change() *100
		#out_file['count_app_delta'] = out_file.count_app.pct_change() *100
		#out_file['value_app_delta'] = out_file.value_app.pct_change() * 100
		#out_file['income_mult_delta'] = out_file.income_multiple_app.pct_change()*100
		for race in race_dict.keys():
			numerator = race_dict[race] + '_loan_average_'
			denominator = race_dict[race] + '_income_average_'
			income_multiple(df=out_file, action='app', numerator=numerator + 'app', denominator=denominator+'app')
			income_multiple(df=out_file, action='orig', numerator=numerator + 'orig', denominator=denominator+'orig')
		#FIXME add demographic deltas here
	if not os.path.exists(path): #check to see if path for a county exists
			os.makedirs(path) #create path if it is not present
	print('writing data for {path}'.format(path=path))
	for col in out_file.columns.ravel():
		print col
	out_file.to_csv(path+"data.csv", index=None) #write dataframe to CSV for one county in state/county/file directory structure

