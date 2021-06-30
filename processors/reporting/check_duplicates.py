import datetime

from pymarc import MARCReader

from processors.db_connector import DatabaseConnector
from processors import utils


class CheckDuplicates:

    dt = datetime.datetime.now()

    @staticmethod
    def __write_dups(tuples, identifier, writer):
        if len(tuples) > 1:
            writer.write(identifier + '\t' + str(len(tuples)) + '\n')

    @staticmethod
    def __query_db(cursor, field):
        cursor.execute('''SELECT id from oclc where id=%s''', [field])
        return cursor.fetchall()

    def check_duplicates(self, input_records, database_name, password, writer):
        db_connect = DatabaseConnector()
        conn = db_connect.get_connection(database_name, password)
        print("Database opened successfully")
        cursor = conn.cursor()
        with open(input_records, 'rb') as fh:

            reader = MARCReader(fh, permissive=True, utf8_handling='ignore')

            for record in reader:
                if record:
                    field_001 = None
                    field_035 = None
                    try:
                        if len(record.get_fields('001')) == 1:
                            field_001 = utils.get_oclc_001_value(record['001'], record['003'])
                        elif len(record.get_fields('035')) > 0:
                            fields = record.get_fields('035')
                            for field in fields:
                                subfields = field.get_subfields('a')
                                if len(subfields) > 1:
                                    print('duplicate 035a')
                                elif len(subfields) == 1:
                                    field_035 = utils.get_035(subfields[0])

                    except Exception as err:
                        print('error reading fields from input record.')
                        print(err)

                    if field_001:
                        tuples = self.__query_db(cursor, field_001)
                        self.__write_dups(tuples, field_001, writer)
                    elif field_035:
                        tuples = self.__query_db(cursor, field_035)
                        self.__write_dups(tuples, field_035, writer)
                else:
                    print(reader.current_exception)
                    print(reader.current_chunk)

            cursor.close()
            conn.close()
