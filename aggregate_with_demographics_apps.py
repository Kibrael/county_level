#combines load_agg.py and get_demographic_aggregates.py to produce CSV and SQL tables of applications aggregated by county
#creates annual tables with aggregate applications and originations by county
#rows with data not parsing correctly were deleted ~30 per year from 2000-2006
import os
import pandas as pd
import psycopg2

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()

race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}

#initialize source table variable name
table_start = 'hmdalar'
app_table_start = 'county_apps_'
year = 2000

#create application tables: county level aggregates by year
for num in range(15):
	source_table = table_start + str(year + num) #increment source table
	app_table = app_table_start + str(year + num) #increment applications aggregate table
	print("\n\n source table: "+ source_table + ", app table: " + app_table +"\n\n")
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
		print base_counties_df.head()
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

		print("\n\nexecuting race aggregation on {table} for race {race}\n".format(table=source_table, race=race_list[race]))
		try: #read results of SQL query into data frame
			SQL_demo_app = SQL_demo_app.format(source_table=source_table, race=race, race_name=race_list[race]) #format SQL query
			demo_df = pd.read_sql_query(SQL_demo_app, conn) #load query results to dataframe
			print demo_df.head()

		except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
			print("no results to fetch for table {table}",format(table=source_table))

		base_counties_df = base_counties_df.merge(demo_df, on='fips', how='outer') #test alternate format of merge
		path = 'data/holding/applications/' #set path for CSV output
		if not os.path.exists(path):
			os.makedirs(path)
		base_counties_df.to_csv(path_or_buf=path+app_table+".csv", index=False)
		#base_counties_df2.to_csv(path_or_buf=path+app_table+"2.csv", index=False)

	#FIXME: create SQL table, copy CSV to table


	#FIXME create origination tables: county level aggregates by year
print("done")

