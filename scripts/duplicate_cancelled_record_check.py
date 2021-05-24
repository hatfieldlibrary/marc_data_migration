import datetime

from processors.oclc_update.db_connector import DatabaseConnector

dt = datetime.datetime.now()

writer = open('../output/audit/duplicate-cancelled-oclc-' + str(dt) + '.csv', 'w')

db_connect = DatabaseConnector()
conn = db_connect.get_connection('pnca', 'Sibale2')
print("Database opened successfully")
cursor = conn.cursor()


with open('../output/audit/cancelled-oclc-2021-04-22 11:46:08.067860.csv', 'r') as fh:
    line = fh.readline()
    while line:
        values = line.split('\t')
        primary = values[1].replace('\n', '')
        cursor.execute('''SELECT id from oclc where id=%s''', [values[0]])
        tuples = cursor.fetchall()
        for record in tuples:
            writer.write(values[0] + '\t' + values[1])
        line = fh.readline()

cursor.close()
conn.close()