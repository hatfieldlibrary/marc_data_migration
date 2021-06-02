import datetime

from pymarc import TextWriter

from processors.db_connector import DatabaseConnector
from processors.read_marc import MarcReader


class ReportProcessor:

    dt = datetime.datetime.now()

    def analyze_duplicate_control_fields(self, password):
        database = DatabaseConnector()
        conn = database.get_connection('duplicatetest', password)
        cursor = conn.cursor()

    @staticmethod
    def load_database(file, password):
        wrapper = MarcReader()
        reader = wrapper.get_reader(file)
        database = DatabaseConnector()
        conn = database.get_connection('duplicatetest', password)
        cursor = conn.cursor()
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
                    cursor.execute('INSERT INTO recs (field001, field003, record)  VALUES (%s, %s, %s)',
                                (field001, field003, record.as_json()))
                    conn.commit()
                except Exception as err:
                    print(err)
                    cursor.close()
                    conn.close()
                    break

        cursor.close()
        conn.close()

    def report_dup_245(self, file):
        title_writer = TextWriter(open('output/audit/duplicate_title_fields-' + str(self.dt) + '.txt', 'w'))
        wrapper = MarcReader()
        reader = wrapper.get_reader(file)
        for record in reader:
            if record:
                arr_245 = record.get_fields('245')
                if len(arr_245) > 1:
                    title_writer.write(record)

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
