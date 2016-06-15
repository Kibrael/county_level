import matplotlib
import numpy as np
import os
import pandas as pd
import psycopg2

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor() #establish SQL cursor object

#load tract to CBSA file to get valid counties
cbsa_df = pd.read_csv('tract_to_CBSA_2010.csv', sep='|')
cbsa_df.county = cbsa_df.county.map(lambda x: str(x).zfill(5)) #left pad with 0's to make valid FIPS codes
fips_list = set(cbsa_df.county.ravel()) #remove duplicates

##########################
#selects application and origination information for single family homes for a single county
#loads to data frame and writes to csv in state/county directory structure
#writes a CSV to the base directory with all counties and all states
#this process takes ~15 minutes to run
##########################

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
			WHERE app.fips = cast({fips} AS VARCHAR(5))

			"""
		SQL = SQL.format(app_table=app_table, orig_table=orig_table, fips=fips)
		df = pd.read_sql_query(SQL, conn) #load query results to dataframe

		if first:
			out_file = df #establish outfile with first year
			first = False
		else:
			try:
				out_file = pd.concat([out_file, df]) #append a year to a county dataframe
			except ValueError as e:
				print('big trouble!! ', e)
		#create income multiple for single year
		out_file['income_multiple'] = (out_file.loan_average_app / 0.80) / out_file.income_average_app
		#create deltas for pattern building
		out_file['loan_avg_app_delta'] = out_file.loan_average_app.pct_change()*100
		out_file['income_average_app_delta'] = out_file.income_average_app.pct_change() *100
		out_file['count_app_delta'] = out_file.count_app.pct_change() *100
		out_file['value_app_delta'] = out_file.value_app.pct_change()
		out_file['income_mult_delta'] = out_file.income_multiple.pct_change()*100

	if not os.path.exists(path): #check to see if path for a county exists
			os.makedirs(path) #create path if it is not present
	print('writing data for {path}'.format(path=path))
	out_file.to_csv(path+"data.csv", index=None) #write dataframe to CSV for one county in state/county/file directory structure

