##########################
#06/24/2016 K. David Roell CFPB
#prototype for creating dynamic SQL schemas
#
#
##################################

from collections import OrderedDict
import psycopg2
import os
import pandas as pd

from lib.agg_funcs import *
from lib.sql_text import *

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()#instantiate cursor object to use in SQL queries

app_path = '/data/holding/applications/' #set path for applications CSV output
orig_path = 'data/holding/originations/' #set path for originations CSV output

#load CSV of county-level aggregates for a single year
holding_df = pd.read_csv(app_data_path+'test_new.csv')
#make dictionary to convert from pandas to PostgreSQL data types
dtype_dict = {'float64':'real', 'object':'varchar(10)', 'int64':'float'}
schema_cols = OrderedDict({}) #dictionary for column headers and data types

#makes a dictionary of column headers and data types
#FIXME change to list comprehension
for col in holding_df.columns:
	schema_cols[col] = str(holding_df[col].dtype)

SQL = """CREATE TABLE {out_table} (""".format(out_table='derp2')
#loop over columns in dictionary to add them to the create table statement
for column, dtype in schema_cols.iteritems():
	col_add = """{column} {dtype}, """.format(column=column, dtype=dtype_dict[dtype])
	SQL=SQL+col_add + '\n'
SQL = SQL[:-3] + ');' + 'COMMIT;' #remove trailing comma and finalize SQL with COMMIT
#FIXME needs column order to be standard
#FIXME add primary key fips
cur.execute(drop_table('derp2'))#drop previous instance of table, if any
cur.execute(SQL,) #execute table creation
load_SQL = format_load_SQL('derp2', app_data_path+'test_new.csv') #format data load SQL statement
print load_SQL
cur.execute(load_SQL,) #execute data load statement