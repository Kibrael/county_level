#combines load_agg.py and get_demographic_aggregates.py to produce CSV and SQL tables of originations aggregated by county
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
orig_table_start = 'county_orig_'
year = 2000

#aggregate origination data: county level aggregates by year
for num in range(15):
	source_table = table_start + str(year + num) #increment source table
	orig_table = orig_table_start + str(year + num) #increment originations aggregate table
	print("\n\n source table: " + source_table + ",  aggregate table: " + orig_table +"\n\n")
	#SQL1 = """DROP TABLE IF EXISTS {drop_table}; COMMIT;"""

	SQL_base ="""SELECT
		year
		,state
		,county
		,CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS loan_average_orig
		,ROUND(AVG(income::INTEGER),2) AS income_average_orig
		,COUNT(concat(agency, rid)) AS count_orig
		,SUM(amount::INTEGER) AS value_orig
		FROM {source_table}
		WHERE
		          loan_type = '1'
		AND action = '1'
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
		SQL_demo_orig ="""SELECT
		 CONCAT(state, county) AS fips
		,ROUND(AVG(amount::INTEGER),2) AS {race_name}_loan_average_orig
		,ROUND(AVG(income::INTEGER),2) AS {race_name}_income_average_orig
		,COUNT(concat(agency, rid)) AS {race_name}_count_orig
		,SUM(amount::INTEGER) AS {race_name}_value_orig
		FROM {source_table}
		WHERE
		          race = '{race}'
		AND action = '1'
		AND loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

		GROUP BY CONCAT(state, county);
		"""

		print("aggregating demographics for {table} and {demo}".format(table=source_table, demo=race_list[race])
		try: #read results of SQL query into data frame
			SQL_demo_orig = SQL_demo_orig.format(source_table=source_table, race=race, race_name=race_list[race]) #format SQL query
			demo_df = pd.read_sql_query(SQL_demo_orig, conn) #load query results to dataframe
			#print df.head()

		except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
			print("no results to fetch for table {table}",format(table=source_table))
		base_counties_df = base_counties_df.merge(demo_df, on='fips', how='outer') #test alternate format of merge
		#base_counties_df = pd.concat([base_counties_df, df], axis=1, join='outer') #add currrent results to all race aggregate dataframe
		path = 'data/holding/originations/' #set path for CSV output
		if not os.path.exists(path):
			os.makedirs(path)
		base_counties_df.to_csv(path_or_buf=path+orig_table+".csv", index=False)


	#FIXME: create SQL table, copy CSV to table
	#FIXME create origination tables: county level aggregates by year

print("done")

