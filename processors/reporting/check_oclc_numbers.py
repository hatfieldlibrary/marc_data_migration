from urllib.error import HTTPError

from pymarc import MARCReader
import processors.utils as utils
from processors.db_connector import DatabaseConnector
from processors.oclc_update.oclc_connector import OclcConnector
import xml.etree.ElementTree as ET


class CompareOclcNumbers:

    ns = {'': 'http://www.loc.gov/MARC21/slim'}

    connector = OclcConnector()

    def __write_to_file(self, control_field, oclc_response, writer):
        field_001 = oclc_response.find('.//*[@tag="001"]', self.ns)
        if field_001 is not None:
            num = field_001.text
            if control_field != num:
                if num:
                    writer.write("'" + control_field + "\t'" + num + "\n")
                else:
                    writer.write("'" + control_field + "'\tfound no identifier in oclc record?\n")

    def compare_oclc_numbers(self, file, writer, database_name, password):

        conn = None
        cursor = None
        if database_name:
            # If database provided, initialize the connection and get cursor. If not
            # provided the OCLC API will be used.
            db_connect = DatabaseConnector()
            conn = db_connect.get_connection(database_name, password)
            print("Database opened successfully")
            cursor = conn.cursor()

        with open('/Users/mspalti/oclc_worldcat_my_key.txt', 'r') as fh:
            oclc_developer_key = fh.readline().strip()

        with open(file, 'rb') as fh:
            reader = MARCReader(fh, permissive=True, utf8_handling='ignore')
            for record in reader:
                if record:
                    field_001 = None
                    field_035 = None
                    try:
                        # Get values for 001 value, 035 value, and title.
                        # These are used to request and verify data from OCLC.
                        if len(record.get_fields('001')) == 1:
                            field_001 = utils.get_oclc_001_value(record['001'], record['003'])
                        if len(record.get_fields('035')) > 0:
                            field_035 = utils.get_oclc_035(record)

                    except Exception as err:
                        print('error reading fields from input record.')
                        print(err)

                    try:
                        # Use 001 by default. Try 035 if the 001 is not available.
                        if field_001:
                            if cursor is not None:
                                cursor.execute("""SELECT oclc FROM oclc where id=%s""", [field_001])
                                row = cursor.fetchone()
                                if row:
                                    oclc_response = ET.fromstring(row[0])
                            else:
                                oclc_response = self.connector.get_oclc_response(field_001, oclc_developer_key, False)

                            self.__write_to_file(field_001, oclc_response, writer)

                        elif field_035:
                            if cursor is not None:
                                cursor.execute("""SELECT oclc FROM oclc where id=%s""", [field_035])
                                row = cursor.fetchone()
                                if row:
                                    oclc_response = ET.fromstring(row[0])
                            else:
                                oclc_response = self.connector.get_oclc_response(field_035, oclc_developer_key, False)

                            self.__write_to_file(field_035, oclc_response, writer)

                        # time.sleep(0.5)

                    except HTTPError as err:
                        print(err)
                    except UnicodeEncodeError as err:
                        print(err)
                    except Exception as err:
                        print(err)

        if cursor is not None:
            cursor.close()
            conn.close()
