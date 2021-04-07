from pymarc import TextWriter
from modules.records_modifier import RecordsModifier

# Get developer key
with open('/Users/michaelspalti/oclc_worldcat_my_key.txt', 'r') as fh:
    oclc_developer_key = fh.readline().strip()

updated_records_writer = TextWriter(open('output/updated-records/updated-records-pretty.txt', 'wt'))
modifier = RecordsModifier()
modifier.update_fields('data/bib/full-export-orig.txt', ['245'], updated_records_writer, oclc_developer_key)



