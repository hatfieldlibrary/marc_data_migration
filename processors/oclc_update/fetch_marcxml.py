from urllib.error import HTTPError
import re
from pymarc import MARCReader
import processors.utils as utils
from processors.oclc_update.oclc_connector import OclcConnector


class FetchMarcXMLRecs:

    connector = OclcConnector()

    def fetch_marcxml(self, file, oclc_xml_writer, oclc_developer_key):
        count = 0
        if oclc_xml_writer is not None:
            oclc_xml_writer.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>')
            oclc_xml_writer.write('<collection xmlns="http://www.loc.gov/MARC21/slim" '
                                  'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                                  'xsi:schemaLocation="http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">')

        with open(file, 'rb') as fh:
            reader = MARCReader(fh, permissive=True, utf8_handling='ignore')
            for record in reader:
                if record:
                    field_001 = None
                    field_035 = None
                    try:
                        if len(record.get_fields('001')) == 1:
                            field_001 = utils.get_oclc_001_value(record['001'], record['003'])

                        if len(record.get_fields('035')) > 0:
                            field_035 = utils.get_035(record)

                    except Exception as err:
                        print('error reading fields from input record.')
                        print(err)

                    try:
                        regex = re.compile('<\?xml.*?\?>\\n')
                        # Use 001 by default. Try 035 if the 001 is not available.
                        if field_001:
                            oclc_response = self.connector.get_oclc_response(field_001, oclc_developer_key, True)
                            oclc_response = re.sub(regex, '', oclc_response)
                            if oclc_xml_writer is not None:
                                oclc_xml_writer.write(oclc_response)
                                count += 1
                        elif field_035:
                            oclc_response = self.connector.get_oclc_response(field_035, oclc_developer_key, True)
                            oclc_response = re.sub(regex, '', oclc_response)
                            if oclc_xml_writer is not None:
                                oclc_xml_writer.write(oclc_response)
                                count += 1
                        # time.sleep(0.5)
                    except HTTPError as err:
                        print(err)
                    except UnicodeEncodeError as err:
                        print(err)
                    except Exception as err:
                        print(err)

        if oclc_xml_writer is not None:
            oclc_xml_writer.write('</collection>')

        print('Downloaded record count: ' + str(count))
