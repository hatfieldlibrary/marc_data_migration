from pymarc import TextWriter
from modules.records_modifier import RecordsModifier
from modules.fetch_marcxml import FetchMarcXMLRecs
import argparse
import datetime

database_name = 'pnca'

parser = argparse.ArgumentParser(description='Process marc records.')

parser.add_argument('source', metavar='source', type=str,
                    help='path to the marc file that will be processed')
parser.add_argument('-db', '--use-database', metavar='database name', type=str,
                    help='postgres database name to be used instead of OCLC API')
parser.add_argument('-nt', '--no-title-check', action='store_false',
                    help='skip the fuzzy title match on 245a')
parser.add_argument('-r', '--replace-fields', action='store_true',
                    help='replace fields with OCLC data')
parser.add_argument("-t", "--track-fields", action="store_true",
                    help="Create an audit log of modifed fields.")
parser.add_argument("-m", "--track-title-matches", action="store_true",
                    help="Create a log of fuzzy title matches.")
parser.add_argument("-so", "--save-oclc", action="store_true",
                    help="Save records from OCLC to local xml file for reuse.")
parser.add_argument('-oc', '--oclc-records', action='store_true',
                    help='just download the OCLC marcxml records')

args = parser.parse_args()

dt = datetime.datetime.now()

source = args.source
if not source:
    raise AssertionError("You must provide a source file.")

# optional database name to use instead of OCLC API
database_name = args.use_database

if args.oclc_records:

    # Get developer key. Change path as needed!
    with open('/Users/michaelspalti/oclc_worldcat_my_key.txt', 'r') as fh:
        oclc_developer_key = fh.readline().strip()

    oclc_xml_writer = open('output/xml/oclc-' + str(dt) + '.xml', 'w')

    fetch_recs = FetchMarcXMLRecs()
    fetch_recs.fetch_marcxml(source, oclc_xml_writer, oclc_developer_key)

if args.replace_fields:

    # Get developer key. Change path as needed!
    with open('/Users/michaelspalti/oclc_worldcat_my_key.txt', 'r') as fh:
        oclc_developer_key = fh.readline().strip()

    # updated records
    updated_records_writer = TextWriter(open('output/updated-records/updated-records-pretty-' + str(dt) + '.txt', 'w'))

    # unmodified records
    unmodified_records_writer = TextWriter(open('output/updated-records/unmodified-records-pretty-' + str(dt) + '.txt', 'w'))

    # Write unreadable records to binary file.
    bad_records_writer = open('output/updated-records/bad-records-pretty-' + str(dt) + '.txt', 'wb')

    title_log_writer = None
    oclc_xml_writer = None
    field_substitution_audit_writer = None
    input_marc_xml = None

    # optional report on fuzzy title matching for most current OCLC harvest
    if args.track_title_matches:
        title_log_writer = open('output/audit/title_fuzzy_match-' + str(dt) + '.csv', 'w')

    # optional marcxml file for most current OCLC harvest
    if args.save_oclc:
        oclc_xml_writer = open('output/xml/oclc-' + str(dt) + '.xml', 'w')

    # optional field replacement audit for most current OCLC harvest
    if args.track_fields:
        field_substitution_audit_writer = open('output/audit/fields_audit-' + str(dt) + '.txt', 'w')

    # Fields to be replaced if found in the OCLC record.
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

    # title_log_writer.write('original\toclc\ttest 1\ttest 2\tratio\tstatus\n\n')

    modifier = RecordsModifier()

    t = args.no_title_check

    modifier.update_fields_using_oclc(args.source,
                                      database_name,
                                      fields_array,
                                      args.no_title_check,
                                      updated_records_writer,
                                      unmodified_records_writer,
                                      bad_records_writer,
                                      title_log_writer,
                                      oclc_xml_writer,
                                      field_substitution_audit_writer,
                                      oclc_developer_key)

    bad_records_writer.close()
    if title_log_writer is not None:
        title_log_writer.close()
    if field_substitution_audit_writer is not None:
        field_substitution_audit_writer.close()
