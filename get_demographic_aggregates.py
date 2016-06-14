#python3

import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()

#modify application table to include demographic factors
race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}

for num in range(15): #set to 1 for testing, return to 15 when done
	source_table = "hmdalar"+ str(2000+num) #set table to aggregate race data from
	update_table = "county_apps_"+str(2000+num) #set table on which to store race aggregate


	read_SQL = """SELECT * FROM {table}""".format(table=update_table) #set query text to get county aggregate data from table
	print("selecting data from {table}".format(table=update_table))

	county_agg_data = pd.read_sql_query(read_SQL, conn) #execute query and convert results to dataframe
	print(county_agg_data.head()) #check output
	all_race_aggs = []
	first = True
	for race in race_list.keys(): #loop over all races and select aggregate date for each
		SQL = """
		SELECT
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
		print("executing race aggregation")#, SQL.format(source_table=source_table, race=race, race_name=race_list[race])

		try: #read results of SQL query into data frame
			df = pd.read_sql_query(SQL.format(source_table=source_table, race=race, race_name=race_list[race]), conn) #load query results to dataframe
			#print df.head()

		except psycopg2.ProgrammingError as e: #catch empty dataframe errors, this may need to populate an emtpy dataframe
			print("no results to fetch for table {table}",format(table=source_table))

		if first:
			all_race_aggs = df #establish dataframe for all race aggregates
			first = False
		else:
			all_race_aggs = pd.concat([all_race_aggs, df], axis=1, join='outer') #add currrent results to all race aggregate dataframe
		#print("all races:\n", all_race_aggs.head())
	merged_df = pd.concat([county_agg_data, all_race_aggs], axis=1, join='outer') #merge race aggregates with county aggregates
	merged_df2= pd.merge([county_agg_data, all_race_aggs], axis=1, on='fips')
	#engine = create_engine('posgresql://roellk@localhost:5432/hmdamaster')
	#merged_df.to_sql(update_table, engine)
	path = 'data/holding/'
	if not os.path.exists(path):
		os.makedirs(path)
	merged_df.to_csv(path_or_buf=path+update_table+".csv", index=False)
	merged_df2.to_csv(path_or_buf=path+update_table"2.csv", index=False)
	#print merged_df.head()
		#Write DF to CSV in data/state/county path
