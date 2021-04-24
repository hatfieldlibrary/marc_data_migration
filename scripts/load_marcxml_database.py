import lxml.etree as ET
from db_connector import DatabaseConnector

db_connect = DatabaseConnector()
conn = db_connect.get_connection('pnca', 'Sibale2')
print("Database opened successfully")
cursor = conn.cursor()
path = '/Users/mspalti/IdeaProjects/marc_data_migration/output/xml/oclc-2021-04-20 11:02:47.023345.xml'

count = 0
count_001 = 0
count_035 = 0
for event, elem in ET.iterparse(path, events=('start', 'end'), tag='{http://www.loc.gov/MARC21/slim}record'):
    if event == 'end':
        count += 1
        field_001 = None
        field_035 = None
        subfield_a = None
        if elem.tag == '{http://www.loc.gov/MARC21/slim}record':
            for child in list(elem):
                tag = child.get('tag')
                if tag is not None:
                    if tag == '001':
                        field_001 = child.text
                    if tag == '003':
                        field_003 = child.text
                    if tag == '035':
                        field_035 = child.text
                    if tag == '245':
                        for subfield in child:
                            at = subfield.attrib.get("code")
                            if at == 'a':
                                subfield_a = subfield.text


        xml_output = ET.tounicode(elem, method='xml', pretty_print=False, with_tail=True, doctype=None)

        if field_001 and subfield_a:
            count_001 += 1
            cursor.execute('INSERT INTO oclc (id, title, oclc)  VALUES (%s, %s, %s)',
                           (field_001, subfield_a, xml_output))
        elif field_035 and subfield_a:
            count_035 += 1
            cursor.execute('INSERT INTO oclc (id, title, oclc) VALUES (%s, %s, %s)',
                           (field_003, subfield_a, xml_output))
        else:
            print('could not read data from input')

        conn.commit()

        print('record inserted.')
        elem.clear()

print('total: ' + str(count))
print('001: ' + str(count_001))
print('035: ' + str(count_035))

conn.close()
