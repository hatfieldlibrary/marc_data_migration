
import xml.etree.ElementTree as ET


class DatabaseUpdate:

    def add_response(self, local_field, oclc_response, oclc_field, title, cursor):

        ET.register_namespace('', 'http://www.loc.gov/MARC21/slim')

        xml = ET.tostring(oclc_response,
                          encoding='unicode',
                          method='xml')

        cursor.execute('INSERT INTO oclc (id, oclc_id, title, oclc)  VALUES (%s, %s, %s, %s)',
                       (local_field, oclc_field, title, xml))
