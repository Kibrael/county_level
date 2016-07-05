##########################
#06/24/2016 K. David Roell CFPB
#Aggregates a new metric from base data
#Adds metric column to data tables and county-level CSV files
#
###########################
import os
import pandas as pd
import psycopg2

from lib.sql_text import add_column, agg_new_col, select_agg_table
from lib.agg_funcs import *

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()#instantiate cursor object to use in SQL queries

app_path = 'data/holding/applications/' #set path for applications CSV output
orig_path = 'data/holding/originations/' #set path for originations CSV output

#FIXME create config section/file for new metric creation

#format SQL for new metric
app_metric_SQL = agg_new_col(source_table='hmdalar2000', pg_func='sum', action_taken="action != '1'", column='income', new_col='graaaar')
orig_metric_SQL = agg_new_col(source_table='hmdalar2000', pg_func='sum', action_taken="action = '1'", column='income', new_col='graaaar')

#query source table for new aggregate metric
app_metric_df = agg_new_metric(conn, app_metric_SQL)
orig_metric_df = agg_new_metric(conn, orig_metric_SQL)

#query aggregate table for previously aggregated data
#aggregate_table_SQL = select_agg_table(table='county_apps_2000')
#agg_df = pd.read_sql_query(aggregate_table_SQL, conn)

#read data for all counties for a single year
holding_app_df = pd.read_csv(app_path + 'hmdalar2000_applications.csv')
holding_orig_df = pd.read_csv(orig_path+'hmdalar2000_originations.csv')

#merge new metric with previously aggregated data
new_app_holding_df = holding_app_df.merge(app_metric_df, on='fips', how='outer')
new_orig_holding_df = holding_orig_df.merge(orig_metric_df, on='fips', how='outer')

#write new aggregate CSV to holding
new_app_holding_df.to_csv(app_path+'test_new.csv', index=None)
new_orig_holding_df.to_csv(orig_path+'test_new.csv', index=None)

#FIXME: create dynamic SQL for making schema
#forl col in
#add additional column to table schema
#new_col_sql = add_column()#table='county_apps_2000', column='new_col', data_type='varchar(10)'
#cur.execute(new_col_sql,)



#load CSV to table
#PG export table structure command? use to re create table with new agg metrics
