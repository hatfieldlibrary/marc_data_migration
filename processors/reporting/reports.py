import datetime

from pymarc import TextWriter

from processors.db_connector import DatabaseConnector
from processors.read_marc import MarcReader


class ReportProcessor:

    dt = datetime.datetime.now()

    def analyze_duplicate_control_fields(self, file, password):
        """
        Reports on records in set that have duplicate 001/003 combinations
        Uses a postgres database for analysis.  This is awkward
        but effective. Database name: 'duplicatetest,' table name
        'recs.'  Table columns 'field001' and 'field003.' Column types
        'varchar.'
        :param file: the file containing records
        :param password: the database password
        :return:
        """
        print('Loading database.  This will take a few minutes.')
        self.__load_database(file, password)
        print('Database is loaded. Creating report.')
        database = DatabaseConnector()
        conn = database.get_connection('duplicatetest', password)
        cursor = conn.cursor()
        cursor.execute('SELECT field001, field003, count(*) FROM recs GROUP BY field001, field003 HAVING count(*) > 1')
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    @staticmethod
    def __load_database(file, password):
        """
        Loads the database after first assuring that the 'recs'
        table is empty.
        :param file: the file containing records
        :param password:
        :return:
        """
        wrapper = MarcReader()
        reader = wrapper.get_reader(file)
        database = DatabaseConnector()
        conn = database.get_connection('duplicatetest', password)
        cursor = conn.cursor()
        # delete existing
        cursor.execute('DELETE FROM recs')
        conn.commit()
        for record in reader:
            if record:
                field001arr = record.get_fields('001')
                if len(field001arr) == 0:
                    field001 = ''
                else:
                    field001 = field001arr[0].value()
                field003arr = record.get_fields('003')
                if len(field003arr) == 0:
                    field003 = ''
                else:
                    field003 = field003arr[0].value()

                try:
                    cursor.execute('INSERT INTO recs (field001, field003)  VALUES (%s, %s)',
                                   (field001, field003))
                    conn.commit()
                except Exception as err:
                    print(err)
                    cursor.close()
                    conn.close()
                    break

        cursor.close()
        conn.close()

    def report_dup_245(self, file):
        """
        Checks for duplicate 245 fields. Writes result to audit file.
        :param file: the file containing records.
        :return:
        """
        title_writer = TextWriter(open('output/audit/duplicate_title_fields-' + str(self.dt) + '.txt', 'w'))
        wrapper = MarcReader()
        reader = wrapper.get_reader(file)
        counter = 0
        for record in reader:
            if record:
                arr_245 = record.get_fields('245')
                if len(arr_245) > 1:
                    title_writer.write(record)
                    counter += 1
        print(str(counter) + ' duplicates found.')
        print('See: output/audit/duplicate_title_fields-' + str(self.dt) + '.txt')

    def report_dup_main(self, file):
        """
        Checks for duplicate 1xx fields.  Writes result to audit file.
        :param file: the file containing records
        :return:
        """
        dup_writer = TextWriter(open('output/audit/duplicate_100_fields-' + str(self.dt) + '.txt', 'w'))
        wrapper = MarcReader()
        reader = wrapper.get_reader(file)
        counter = 0
        for record in reader:
            if record:
                arr_100 = record.get_fields('100')
                arr_110 = record.get_fields('110')
                arr_111 = record.get_fields('111')
                arr_130 = record.get_fields('130')
                # array of arrays to be filtered
                fields = [arr_100, arr_110, arr_111, arr_130]
                result = filter(lambda a: len(a) > 0, fields)
                # convert filter result to list an check length
                if len(list(result)) > 1:
                    dup_writer.write(record)
                    counter += 1
                elif len(arr_100) > 1:
                    dup_writer.write(record)
                    counter += 1
                elif len(arr_110) > 1:
                    dup_writer.write(record)
                    counter += 1
                elif len(arr_130) > 1:
                    dup_writer.write(record)
                    counter += 1

        print(str(counter) + ' duplicates found')
        print('See: output/audit/duplicate_100_fields-' + str(self.dt) + '.txt')

    def decode(self, file):
        wrapper = MarcReader()
        reader = wrapper.get_reader(file, 'replace', False)
        for record in reader:
            if record is None:
                print(
                    "Current chunk: ",
                    reader.current_chunk,
                    " was ignored because the following exception raised: ",
                    reader.current_exception)
            else:
                title = record.title()
                #decoded = title.encode('utf-8', 'replace')
                #print(decoded)
                #print(bytes.decode(decoded, 'utf-8', 'replace'))
                rec_245 = record.get_fields('245')
                if len(rec_245) > 1:
                    print(title)
