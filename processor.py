from pymarc import TextWriter
from modules.records_modifier import RecordsModifier

# Get developer key
with open('/Users/michaelspalti/oclc_worldcat_my_key.txt', 'r') as fh:
    oclc_developer_key = fh.readline().strip()

updated_records_writer = TextWriter(open('output/updated-records/updated-records-pretty.txt', 'wt'))
unmodified_records_writer = TextWriter(open('output/updated-records/unmodified-records-pretty.txt', 'wt'))
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
modifier.update_fields_using_oclc('data/bib/full-export-orig.txt',
                                  fields_array,
                                  updated_records_writer,
                                  unmodified_records_writer,
                                  bad_records_writer,
                                  oclc_developer_key)


