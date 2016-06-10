
import psycopg2

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()

#modify application table to include demographic factors
race_list = {'1': 'native', '2': 'asian', '3': 'black', '4': 'hawaiian', '5': 'white', '6': 'not_provided', '7': 'not_applicable', '8': 'no_co_app'}

for num in range(15):
	source_table = "hmdalar"+ str(2000+num) #set table to aggregate race data from
	update_table = "county_apps_"+str(2000+num) #set table on which to store race aggregate
	for race in race_list.keys():
		SQL = """
		DROP TABLE IF EXISTS {race_name}_count; COMMIT;
		CREATE TEMP TABLE {race_name}_count AS (
		SELECT
			 CONCAT(state, county) AS fips
			,COUNT(sequence) AS rcount
		FROM {source_table}
		WHERE
		          race = '{race}'
		AND loan_type = '1'
		AND loan_purpose in ('1', '3')
		AND amount not like '%NA%'
		AND income not like '%NA%'
		AND amount not like '%na%'
		AND income not like '%na%'

		GROUP BY state, county
			); COMMIT;
		"""
		print "executing:", SQL.format(source_table=source_table, race=race, race_name=race_list[race])
		cur.execute(SQL.format(source_table=source_table, race=race, race_name=race_list[race]),)

		SQL2 = """
		ALTER TABLE {result_table} ADD count_{race_name} INTEGER;
		UPDATE {result_table}
		SET count_{race_name} = race_count.rcount
		FROM race_count
		WHERE CONCAT({result_table}.state, {result_table}.county) = race_count.fips;
		COMMIT;
		DROP TABLE IF EXISTS race_count; COMMIT;
		"""

		#print "executing:", SQL2.format(result_table=update_table, race_name=race_list[race])
		#cur.execute(SQL2.format(result_table=update_table, race_name=race_list[race]),)