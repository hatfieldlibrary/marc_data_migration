from typing import TextIO

from pymarc import TextWriter
from modules.records_modifier import RecordsModifier
import argparse
import datetime

parser = argparse.ArgumentParser(description='Process marc records.')

parser.add_argument('task', metavar='task', type=str,
                    help='the task to run (replace|move|modify)')
parser.add_argument("-oc", "--save-oclc", action="store_true",
                    help="Save records from OCLC to local xml file for reuse.")
parser.add_argument("-t", "--track-fields", action="store_true",
                    help="Create an audit log of modifed fields.")
parser.add_argument("-m", "--track-title-matches", action="store_true",
                    help="Create a log of fuzzy title matches.")
args = parser.parse_args()

task = args.task

dt = datetime.datetime.now()

if task == 'replace':

    # Get developer key. Change path as needed!
    with open('/Users/michaelspalti/oclc_worldcat_my_key.txt', 'r') as fh:
        oclc_developer_key = fh.readline().strip()

    # updated records
    updated_records_writer = TextWriter(open('output/updated-records/updated-records-pretty-' + dt + '.txt', 'w'))

    # unmodified records
    unmodified_records_writer = TextWriter(open('output/updated-records/unmodified-records-pretty-' + dt + '.txt', 'w'))

    # Write unreadable records to binary file.
    bad_records_writer = open('output/updated-records/bad-records-pretty-' + dt + '.txt', 'wb')

    title_log_writer = None
    oclc_xml_writer = None
    field_substitution_audit_writer = None
    input_marc_xml = None

    # optional report on fuzzy title matching for most current OCLC harvest
    if args.track_title_matches:
        title_log_writer = open('output/audit/title_fuzzy_match-' + dt + '.txt', 'w')

    # optional marcxml file for most current OCLC harvest
    if args.save_oclc:
        oclc_xml_writer = open('output/xml/oclc-' + dt + '.xml', 'w')

    # optional field replacement audit for most current OCLC harvest
    if args.track_fields:
        field_substitution_audit_writer = open('output/audit/fields_audit-' + dt + '.txt', 'w')

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

    title_log_writer.write('original\toclc\tratio\n\n')

    modifier = RecordsModifier()

    modifier.update_fields_using_oclc('data/bib/full-export-orig.txt',
                                      fields_array,
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
