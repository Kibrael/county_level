import pandas as pd
import psycopg2
import matplotlib
import numpy as np
from connector import connect_DB #import custom connection script
#get county level data
#count of loans
#average and median dollar value of loans
#dollar volume of loan amount
connector = connect_DB() #instantiate connector class
cur = connector.connect() #connect and return connection

#list of HMDA years to use
#property type appears in 2004
#FIXME reload and clean 2004
#FIXME: add property type into 2004 and beyond?
hmda_tables = ['hmdalar2000', 'hmdalar2001', 'hmdalar2002', 'hmdalar2003', 'hmdalar2004', 'hmdalar2005', 'hmdalar2006',
		'hmdalar2007', 'hmdalar2008', 'hmdalar2009', 'hmdalar2010', 'hmdalar2011', 'hmdalar2012', 'hmdalar2013',
		 'hmdalar2014']

#selects application information for single family homes
first = True


for table in hmda_tables[]:

	SQL = """
		SELECT
		 year
		,state
		,county
		,round(avg(amount::integer),2) as loan_average
		,round(avg(income::integer),2) as income_average,
		count(concat(agency, rid)) as count_of_apps
		,sum(amount::integer) as app_volume

		FROM {table}

		WHERE
		          loan_type = {type}
		AND loan_purpose in ('1', '3')
		AND property_type = '{type}'
		AND income not like '%NA%'
		AND income not like '%na% '
		AND amount not like '%NA%'
		AND amount not like '%na%'

		GROUP BY state, county, year """

	SQL = SQL.format(table=table, type='1')
	#print SQL
	cur.execute(SQL)
	data = cur.fetchall()

	for row in data:
		print row
	#print table, "\n", SQL, "\n\n"
#selects originated loan information for single family homes
for table in hmda_tables:
	SQL = """
		SELECT
		 year
		 ,state
		 ,county
		 ,round(avg(amount::integer),2) as loan_average
		 ,round(avg(income::integer),2) as income_average
		 ,count(concat(agency, rid)) as count_of_apps
		 ,sum(amout::integer) as app_volume

		FROM {table}

		WHERE
	  	          loan_type = {type}
		AND loan_purpose in ('1', '3')
		AND property_type = '{type}''
		AND income not like '%NA%'
		AND income not like '%na% '
		AND amount not like '%NA%'
		AND amount not like '%na%'
		AND action = '1'

		GROUP BY state, county, year """

	SQL = SQL.format(table=table, type='1')
#drop table if exists
#create table to hold data
#insert data

