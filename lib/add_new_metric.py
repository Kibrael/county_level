##########################
#06/24/2016 K. David Roell CFPB
#
#
##########################\
import os
import pandas as pd
import psycopg2

#from lib.sql_text.py import add_column, agg_new_col
from lib.agg_funcs import *

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()#instantiate cursor object to use in SQL queries

app_path = 'data/holding/applications/' #set path for applications CSV output
orig_path = 'data/holding/originations/' #set path for originations CSV output
#add additional column to table

#query source table for new metric
new_metric_df = agg_new_col() #source_table='hmdalar2000', pg_func='sum', column='income', action_taken="action != '1'"
#query aggregate table for previously aggregated data
#join in pandas
#write new aggregate CSV to holding

#load CSV to table
