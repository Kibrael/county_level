#create application tables: county level aggregates by year
############################
#K. David Roell CFPB
#Contains variables and functions to manipulate Pandas dataframes and other files
############################
from collections import OrderedDict
import os
import psycopg2
import pandas as pd

from lib.sql_text import agg_SQL, agg_demo_SQL, agg_new_col
#full dictionary of races used in the HMDA data file
race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}
#subset of the full race list that does not include codes indicating that no information was provided
race_dict = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white'}
#HMDA LAR tables from which to draw data for aggregation
source_tables = ['hmdalar2000', 'hmdalar2001', 'hmdalar2002', 'hmdalar2003', 'hmdalar2004', 'hmdalar2005', 'hmdalar2006',
		'hmdalar2007', 'hmdalar2008', 'hmdalar2009', 'hmdalar2010', 'hmdalar2011', 'hmdalar2012', 'hmdalar2013', 'hmdalar2014']

#set column names to pass to percent_change to create year over year change values
app_delta_cols = ['loan_average_app', 'income_average_app', 'count_app', 'value_app', 'income_multiple_app']
orig_delta_cols = ['loan_average_orig', 'income_average_orig', 'count_orig', 'value_orig', 'income_multiple_orig']

#relative data paths for CSV outputs
app_path = 'data/holding/applications/' #set path for CSV output
orig_path = 'data/holding/originations/'

#data paths for CSVs containing aggregate data for all counties for each year
app_data_path = "/Users/roellk/Desktop/HMDA/data_analysis/data/holding/applications/" #psycopg2 requires an absolute path from which to copy files
orig_data_path = '/Users/roellk/Desktop/HMDA/data_analysis/data/holding/originations/'

#FIXME remove positional arguments from functions

def agg_df(source_table, action, conn):
	"""Calls agg_SQL to create a SQL query and execute it to return a dataframe"""
	app_SQL = agg_SQL(source_table=source_table, action=action) #format SQL query with data source table
	try: #read results of SQL query into data frame
		print("executing {action} aggregation on {table}".format(action=action, table=source_table))
		return pd.read_sql_query(app_SQL, conn) #query LAR database and load county level aggregates to a dataframe
	except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
		print("no results to fetch for table {table}".format(table=source_table), e)

def check_path(path):
	"""Takes a directory path and creates it if it does not exist """
	if not os.path.exists(path):
		os.makedirs(path)


def get_CBSA_df(CBSA_file, seperator):
	"""Loads a CBSA file and the utilized delimiter and returns a dataframe with FIPS codes appropriately padded with 0s """
	cbsa_df = pd.read_csv(CBSA_file, sep=seperator)
	cbsa_df.county = cbsa_df.county.map(lambda x: str(x).zfill(5)) #left pad with 0's to make valid FIPS codes
	return cbsa_df

def income_multiple(df=None, race_name=None, action='app', numerator=None, denominator=1):
	"""Calculates the income multiple for a dataframe with the assumption that LTV is 80% at origination.
	     Requires specification of numerator and denominator columns"""
	#FIXME can this be abstracted to a generic multiple function?
	#FIXME can column names be moved to function call instead of action variable?
	if race_name is None:
		col_name = 'income_multiple_' + action
	else:
		col_name = race_name + '_' + 'income_multiple_' + action
	df[col_name] = (df[numerator] / 0.80) / df[denominator]

def percent_change(df=None, col_list=None):
	"""Applies the Pandas percent change function to a passed list of columns in a data frame"""
	for col in col_list:
		out_col = col + '_delta'
		try:
			df[out_col] = df[col].pct_change() * 100
		except:
			print("unable to compute all values for {column}".format(column=col))
			df[out_col] = 'NaN'

def race_agg_df(source_table, action, race_code, race_name, conn):
	"""Queries a PostgreSQL table and aggregates LAR data to the county level for the selected race
	     uses app_agg_demo_SQL and race to produce the formatted query text"""

	demo_SQL = agg_demo_SQL(source_table, action='app', race_code=race_code, race_name=race_name)
	print("\n\nexecuting race {action} aggregation on {table} for {race}\n".format(action=action, table=source_table, race=race_name))
	try:
		return pd.read_sql_query(demo_SQL, conn) #load query results to dataframe and return it
	except psycopg2.ProgrammingError as e: #catch empty dataframe errors
		print("no results to fetch for {race} from {table}",format(table=source_table, race=race_name))
		#FIXME do I need an empty df return?