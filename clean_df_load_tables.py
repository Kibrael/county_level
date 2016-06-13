#prototype code to clean pandas dataframes from CSVs and load county aggregate tables to postgres
import os
import pandas as pd
import psycopg2

#application data

#load CSV from path
path = "data/holding/"
initial_df = pd.read_csv(path+'data.csv')
#clean dataframe

#create SQL table

#load SQL table