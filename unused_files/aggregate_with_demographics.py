#combines load_agg.py and get_demographic_aggregates.py
#creates annual tables with aggregate applications and originations by county
#rows with data not parsing correctly were deleted ~30 per year from 2000-2006
import os
import pandas as pd
import psycopg2

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()

#List output tables
hmda_app_tables = ['county_apps_2000','county_apps_2001','county_apps_2002','county_apps_2003','county_apps_2004','county_apps_2005',
				  'county_apps_2006','county_apps_2007','county_apps_2008','county_apps_2009','county_apps_2010','county_apps_2011',
				  'county_apps_2012','county_apps_2013','county_apps_2014']

hmda_orig_tables = ['county_orig_2000','county_orig_2001','county_orig_2002','county_orig_2003','county_orig_2004','county_orig_2005',
				  'county_orig_2006','county_orig_2007','county_orig_2008','county_orig_2009','county_orig_2010','county_orig_2011',
				  'county_orig_2012','county_orig_2013','county_orig_2014']

race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}

#initialize source table variable name
table_start = 'hmdalar'
app_table_start = 'county_apps_'
orig_table_start = 'county_orig_'
year = 2000

#create application tables: county level aggregates by year
for num in range(1):
	source_table = table_start + str(year + num) #increment source table
	app_table = app_table_start + str(year + num) #increment applications aggregate table
	orig_table = orig_table_start + str(year + num) #increment originations aggregate table
	print("\n\nsource table:", source_table, "app table:", app_table, "orig table:", orig_table)
	#SQL1 = """DROP TABLE IF EXISTS {drop_table}; COMMIT;"""

	SQL_base ="""SELECT
		year
		,state
		,county
		,CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS loan_average_app
		,ROUND(AVG(income::INTEGER),2) AS income_average_app
		,COUNT(concat(agency, rid)) AS count_app
		,SUM(amount::INTEGER) AS value_app
		FROM {source_table}
		WHERE
		          loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

		GROUP BY year, state, county;"""

	SQL_base = SQL_base.format(source_table=source_table)

	#print("SQL text:\n\n",SQL_base) #check query text
	try: #read results of SQL query into data frame
		base_counties_df = pd.read_sql_query(SQL_base, conn) #query LAR database and load county level aggregates to a dataframe
		#print df.head()
	except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
		print("no results to fetch for table {table}",format(table=source_table))

	for race in race_list.keys():
		SQL_demo_app ="""SELECT
		 CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS {race_name}_loan_average_app
		,ROUND(AVG(income::INTEGER),2) AS {race_name}_income_average_app
		,COUNT(concat(agency, rid)) AS {race_name}_count_app
		,SUM(amount::INTEGER) AS {race_name}_value_app
		FROM {source_table}
		WHERE
		          race = '{race}'
		AND loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

		GROUP BY CONCAT(state, county);
		"""
		print('test', source_table, race)
		print("aggregating demographics:", race_list[race])
		#print("executing race aggregation on {table} for race {race}").format(table=source_table, race=race_list[race])
		try: #read results of SQL query into data frame
			SQL_demo_app = SQL_demo_app.format(source_table=source_table, race=race, race_name=race_list[race]) #format SQL query
			df = pd.read_sql_query(SQL_demo_app, conn) #load query results to dataframe
			#print df.head()

		except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
			print("no results to fetch for table {table}",format(table=source_table))


		base_counties_df = pd.concat([base_counties_df, df], axis=1, join='outer') #add currrent results to all race aggregate dataframe
		#base_counties_df2 = base_counties_df.merge(df, on='fips', how='outer') #test alternate format of merge
		#print("all races:\n", all_race_aggs.head())

		path = 'data/holding/applications/' #set path for CSV output
		if not os.path.exists(path):
			os.makedirs(path)
		base_counties_df.to_csv(path_or_buf=path+app_table+".csv", index=False)
		#base_counties_df2.to_csv(path_or_buf=path+app_table+"2.csv", index=False)

	#FIXME: create SQL table, copy CSV to table


#create origination tables: county level aggregates by year
for num in range(15):
	source_table= table_start + str(year + num)
	result_table=hmda_orig_tables[num]


	#FIXME move creation to end of year loop
	#FIXME hold single year, all county applications or originations in memory (as DF?)

	SQL1 = """DROP TABLE IF EXISTS {drop_table};
		commit;
		"""
	SQL2 = """CREATE TABLE {result_table} AS

		SELECT year, state, county,
		ROUND(AVG(amount::INTEGER),2) AS loan_average_orig,
		ROUND(AVG(income::INTEGER),2) AS income_average_orig,
		COUNT(concat(agency, rid)) AS count_orig,
		SUM(amount::INTEGER) AS value_orig,
		CONCAT(state, county) AS fips

		FROM {source_table}

		WHERE
		loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND action = '1'
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

			GROUP BY year, state, county;
		"""
	SQL=SQL1.format(drop_table=result_table) + SQL2.format(source_table=source_table, result_table =result_table)
	print("orig SQL", SQL)
	#cur.execute(SQL) #convert to pandas sql execution

	#Demographic SQL loop
	#concat all demographics and county aggregates
	#write to CSV
	#create table, copy CSV to table
print("done")

