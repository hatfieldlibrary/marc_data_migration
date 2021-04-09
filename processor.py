from pymarc import TextWriter
from modules.records_modifier import RecordsModifier

# Get developer key
with open('/Users/michaelspalti/oclc_worldcat_my_key.txt', 'r') as fh:
    oclc_developer_key = fh.readline().strip()

updated_records_writer = TextWriter(open('output/updated-records/updated-records-pretty.txt', 'w'))
unmodified_records_writer = TextWriter(open('output/updated-records/unmodified-records-pretty.txt', 'w'))
title_log_writer = open('output/logs/title_fuzzy_match.txt', 'w')
oclc_xml_writer = open('output/xml/oclc.xml', 'w')
field_subtitution_audit_writer = open('output/audit/fields_audit.txt', 'w')
# Write to binary.
bad_records_writer = open('output/updated-records/bad-records-pretty.txt', 'wb')

modifier = RecordsModifier()

fields_array = [
    '006',
    '007',
    '008',
    '024',
    '028',
    '041',
    '043',
    '082',
    '084',
    '100',
    '110',
    '111',
    '130',
    '222',
    '240',
    '245',
    '246',
    '247',
    '250',
    '264',
    '300',
    '337',
    '340',
    '362',
    '386',
    '490',
    '505',
    '510',
    '511',
    '520',
    '521',
    '526',
    '533',
    '538',
    '541',
    '550',
    '600',
    '610',
    '611',
    '630',
    '650',
    '651',
    '655',
    '700',
    '710',
    '730',
    '740',
    '752',
    '760',
    '765',
    '780'
]

title_log_writer.write('original\toclc\tratio\n\n')

modifier.update_fields_using_oclc('data/bib/full-export-orig.txt',
                                  fields_array,
                                  updated_records_writer,
                                  unmodified_records_writer,
                                  bad_records_writer,
                                  title_log_writer,
                                  oclc_xml_writer,
                                  field_subtitution_audit_writer,
                                  oclc_developer_key,
                                  True,
                                  True)

title_log_writer.close()
bad_records_writer.close()
field_subtitution_audit_writer.close()
