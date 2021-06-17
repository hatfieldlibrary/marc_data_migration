from pymarc import TextWriter

from processors.encoding_utils.encoding import EncodingUtils
from processors.reporting.check_duplicates import CheckDuplicates
from processors.reporting.reports import ReportProcessor
from processors.modify_record.record_modify import RecordModifier
from processors.reporting.check_oclc_numbers import CompareOclcNumbers
from processors.oclc_update.record_update import RecordUpdater
from processors.oclc_update.fetch_marcxml import FetchMarcXMLRecs
import argparse
import datetime

# Get developer key. Change path as needed!
with open('/Users/mspalti/oclc_worldcat_my_key.txt', 'r') as fh:
    oclc_developer_key = fh.readline().strip()

parser = argparse.ArgumentParser(description='Process marc records.')

parser.add_argument('source', metavar='source', type=str,
                    help='Required: the path to the marc file to be processed')
parser.add_argument('-p', '--plugin', metavar='plugin name', type=str,
                    help='The plugin module used for record modifications. Example: '
                         'processors.plugins.pnca.pnca_policy')
parser.add_argument('-m', '--modify-recs', action='store_true',
                    help='Just modify records using the provided plugin. ')
parser.add_argument('-r', '--replace-fields', action='store_true',
                    help='Replace fields with fields from the OCLC record.')
parser.add_argument('-pm', '--perfect-match', action='store_true',
                    help='Perfect OCLC title match will be required; records with lower fuzzy match '
                         'ratios are written to a separate file.')
parser.add_argument('-database', '--database-name', metavar='database name', type=str,
                    help='Provide name of the postgres database name to use instead of the OCLC API. '
                         'This significantly speeds up processing.')
parser.add_argument('-udb', '--use-database', action='store_true',
                    help='While replacing fields with OCLC API data insert records into the provided database. '
                         'Requires --use-database flag with database name.')
parser.add_argument('-adb', '--add-to-database', action='store_true',
                    help='While replacing fields with OCLC API data insert records into the provided database. '
                         'Requires --use-database flag with database name.')
parser.add_argument('-nt', '--no-title-check', action='store_false',
                    help='Skip the title match on 245 fields before updating records. You probably do not want '
                         'to do this.')
parser.add_argument('-dft', '--do-fuzzy-test', action='store_true',
                    help='This option adds an additional test of fuzzy match records when the OCLC number was '
                         'found based only on the 003 label.')
parser.add_argument("-t", "--track-fields", action="store_true",
                    help="Create an audit log of modified fields.")
parser.add_argument("-tm", "--track-title-matches", action="store_true",
                    help="Create audit log of fuzzy title matches that includes match accuracy metrics.")
parser.add_argument("-so", "--save-oclc", action="store_true",
                    help="Save records from OCLC to local xml file during while running the replacement task.")
parser.add_argument('-oc', '--oclc-records', action='store_true',
                    help='Only download marcxml from OCLC, no other '
                         'tasks performed.')
parser.add_argument("-ccf", "--check-duplicate-control-field", action="store_true",
                    help="Reports duplicate 001/003 combinations. You must provide a test database name.")
parser.add_argument('-d', '--duplicates', action='store_true',
                    help='Checks for duplicate OCLC numbers in the database.')
parser.add_argument("-dupt", "--check-duplicate-title", action="store_true",
                    help="Check for duplicate 245 fields.")
parser.add_argument("-dupm", "--check-duplicate-main", action="store_true",
                    help="Check for duplicate main entry fields.")
parser.add_argument('-comp', '--compare_oclc_numbers', action='store_true',
                    help='Retrieve OCLC records and compare oclc numbers in '
                         'the response and with the original input file. Logs the discrepancies for analysis.')
parser.add_argument("-encheck", "--encoding-check", action="store_true",
                    help="Uses chardet to guess at the source file encoding.")
parser.add_argument("-enc", "--encoding", metavar='file character encoding', type=str,
                    help="File encoding of the source file.")

args = parser.parse_args()

dt = datetime.datetime.now()

source = args.source
if not source:
    raise AssertionError("You must provide a source file.")

# optional database name to use instead of OCLC API
database_name = args.database_name
# if database requires password replace empty string
password = 'Sibale2'

if args.encoding_check:
    enc = EncodingUtils()
    enc.detect_encoding(source)

if args.check_duplicate_control_field:
    reporter = ReportProcessor()
    reporter.analyze_duplicate_control_fields(source, database_name, password)

if args.check_duplicate_title:
    reporter = ReportProcessor()
    reporter.report_dup_245(source)

if args.check_duplicate_main:
    reporter = ReportProcessor()
    reporter.report_dup_main(source)

if args.compare_oclc_numbers:
    writer = open('output/audit/oclc-number-comparison-' + str(dt) + '.csv', 'w')
    compare = CompareOclcNumbers()
    compare.compare_oclc_numbers(source, writer, database_name, password)

if args.duplicates:
    writer = open('output/audit/duplicate-local-records-' + str(dt) + '.csv', 'w')
    find_dups = CheckDuplicates()
    # add password here if required
    find_dups.check_duplicates(source, database_name, password, writer)

if args.oclc_records:

    oclc_xml_writer = open('output/xml/oclc-' + str(dt) + '.xml', 'w')

    fetch_recs = FetchMarcXMLRecs()
    fetch_recs.fetch_marcxml(source, oclc_xml_writer, oclc_developer_key)

if args.modify_recs:

    # modified records
    modified_records_writer = TextWriter(
        open('output/modified-records/modified-records-pretty-' + str(dt) + '.txt', 'w'))

    # online item records
    modified_online_writer = TextWriter(open('output/modified-records/modified-online-pretty-' + str(dt) + '.txt', 'w'))

    modifier = RecordModifier()
    modifier.record_modify(args.source, args.plugin, modified_records_writer, modified_online_writer)

if args.replace_fields:

    # updated records
    updated_records_writer = TextWriter(open('output/updated-records/updated-records-pretty-' + str(dt) + '.txt', 'w'))

    # online item records
    updated_online_writer = TextWriter(open('output/updated-records/updated-online-pretty-' + str(dt) + '.txt', 'w'))

    # unmodified records
    unmodified_records_writer = TextWriter(
        open('output/updated-records/non-updated-records-pretty-' + str(dt) + '.txt', 'w'))

    # unmodified online item records
    unmodified_online_writer = TextWriter(
        open('output/updated-records/non-updated-online-pretty-' + str(dt) + '.txt', 'w'))

    # fuzzy field match records
    fuzzy_records_writer = TextWriter(
        open('output/updated-records/fuzzy-updated-records-pretty-' + str(dt) + '.txt', 'w'))

    # fuzzy online item records
    fuzzy_online_writer = TextWriter(
        open('output/updated-records/fuzzy-updated-online-pretty-' + str(dt) + '.txt', 'w'))

    # Write unreadable records to binary file.
    bad_records_writer = open('output/updated-records/bad-records-pretty-' + str(dt) + '.txt', 'wb')

    field_035_details_writer = open('output/audit/field-035-details-' + str(dt) + '.txt', 'w')

    title_log_writer = None
    oclc_xml_writer = None
    field_substitution_audit_writer = None

    # optional report on fuzzy title matching for most current OCLC harvest
    if args.track_title_matches:
        title_log_writer = open('output/audit/title-fuzzy-match-' + str(dt) + '.txt', 'w')

    # optional marcxml file for most current OCLC harvest
    if args.save_oclc:
        oclc_xml_writer = open('output/xml/oclc-' + str(dt) + '.xml', 'w')

    # optional field replacement audit for most current OCLC harvest
    if args.track_fields:
        field_substitution_audit_writer = open('output/audit/fields-audit-' + str(dt) + '.txt', 'w')

    updater = RecordUpdater(database_name=database_name,
                            password=password,
                            modified_writer=updated_records_writer,
                            updated_online_writer=updated_online_writer,
                            unmodified_writer=unmodified_records_writer,
                            unmodified_online_writer=unmodified_online_writer,
                            bad_writer=bad_records_writer,
                            title_log_writer=title_log_writer,
                            oclc_xml_writer=oclc_xml_writer,
                            field_audit_writer=field_substitution_audit_writer,
                            fuzzy_record_writer=fuzzy_records_writer,
                            fuzzy_online_writer=fuzzy_online_writer,
                            field_035_details_writer=field_035_details_writer,
                            oclc_developer_key=oclc_developer_key
                            )

    # NOTE the alternative replacement strategy is "replace_only" which
    # simply replaces existing fields with OCLC values and does not
    # add any new tags to the record.
    updater.update_fields_using_oclc(args.source,
                                     args.plugin,
                                     args.perfect_match,
                                     args.no_title_check,
                                     args.add_to_database,
                                     args.use_database,
                                     args.do_fuzzy_test,
                                     encoding=args.encoding,
                                     fuzzy_match_ratio=50,
                                     replacement_strategy='replace_and_add')

    bad_records_writer.close()
    field_035_details_writer.close()
    if title_log_writer is not None:
        title_log_writer.close()
    if field_substitution_audit_writer is not None:
        field_substitution_audit_writer.close()
    if oclc_xml_writer:
        oclc_xml_writer.close()
