import psycopg2

conn = psycopg2.connect("dbname=hmdamaster user=roellk") #connect and return connection
cur = conn.cursor()

table_start = 'hmdalar'
year = 2004

for num in range(11):
    table = table_start+str(year+num)

    print "renaming {table} column".format(table=table)
    SQL = """
            ALTER TABLE {table}
            RENAME COLUMN race1 TO race; commit;"""

    SQL = SQL.format(table=table)
    cur.execute(SQL,)
