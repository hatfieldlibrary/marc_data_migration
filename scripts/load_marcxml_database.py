import lxml.etree as ET
import psycopg2


conn = psycopg2.connect(database="pnca", user="postgres",  host="127.0.0.1", port="5432")
print("Database opened successfully")
cursor = conn.cursor()
path = '/Users/michaelspalti/willamette/pnca-marc-records/output/xml/oclc-2021-04-10 14:50:36.088895.xml'

ns = {'': 'http://www.loc.gov/MARC21/slim'}

for event, elem in ET.iterparse(path, events=('start', 'end'), tag='{http://www.loc.gov/MARC21/slim}record'):
    if event == 'end':
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

        xml_output = ET.tounicode(elem, method='xml', pretty_print=False, with_tail=True, doctype=None)

        if field_001 is not None:
            cursor.execute('INSERT INTO oclc (id, oclc)  VALUES (%s, %s)', (field_001, xml_output))
        elif field_035 is not None:
            cursor.execute('INSERT INTO oclc (id, oclc) VALUES (%s, $s)', (field_003, xml_output))

        conn.commit()

        print('record inserted.')
        elem.clear()

conn.close()
