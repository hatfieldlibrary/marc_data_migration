from urllib.error import HTTPError
import xml.etree.ElementTree as ET
from pymarc import MARCReader
import modules.utils as utils


class FetchOclcRecs:


def update_fields_using_oclc(self, file, oclc_xml_writer, oclc_developer_key):
    if oclc_xml_writer is not None:
        oclc_xml_writer.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>')
        oclc_xml_writer.write('<collection xmlns="http://www.loc.gov/MARC21/slim" '
                              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                              'xsi:schemaLocation="http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">')

    with open(file, 'rb') as fh:
        reader = MARCReader(fh, permissive=True, utf8_handling='ignore')
        for record in reader:
            if record:
                oh_one_value = None
                oclc_number = None
                title = ''
                try:
                    if record['245'] and record['245']['a']:
                        title = record['245']['a']
                    # Get values for 001 value, 035 value, and title.
                    # These are used to request and verify data from OCLC.
                    if len(record.get_fields('001')) == 1:
                        oh_one_value = utils.get_oclc_001_value(record['001'], record['003'])

                    if record['035'] and record['035']['a']:
                        oclc_number = utils.get_oclc_035_value(record['035']['a'])
                except:
                    print('error reading fields from input record: ' + title)

                try:
                    # Use 001 by default. Try 035 if the 001 is not available.
                    if oh_one_value:
                        oclc_response = self.connector.get_oclc_response(oh_one_value, oclc_developer_key)
                        oclc_response.replace('<?xml version=\'1.0\' encoding=\'utf8\'?>\n', '')
                        if oclc_xml_writer is not None:
                            oclc_xml_writer.write(ET.tostring(oclc_response, encoding='utf8', method='xml'))
                    elif oclc_number:
                        oclc_response = self.connector.get_oclc_response(oclc_number, oclc_developer_key)
                        oclc_response.replace('<?xml version=\'1.0\' encoding=\'utf8\'?>\n', '')
                        if oclc_xml_writer is not None:
                            oclc_xml_writer.write(ET.tostring(oclc_response, encoding='utf8', method='xml'))

                except HTTPError as err:
                    print(err)
                except UnicodeEncodeError as err:
                    print(err)

    if oclc_xml_writer is not None:
        oclc_xml_writer.write('</collection>')
