import time
import datetime
import re
from importlib import import_module
from urllib.error import HTTPError
import xml.etree.ElementTree as ET
from pymarc import Field, Leader, TextWriter

from processors.oclc_update.add_response_to_database import DatabaseUpdate
from processors.oclc_update.field_generators import ControlFieldGenerator, DataFieldGenerator
from processors.oclc_update.oclc_connector import OclcConnector
from processors.db_connector import DatabaseConnector
from processors.oclc_update.replace_configuration import substitution_array
from processors.read_marc import MarcReader
import processors.oclc_update.field_replacement_count as field_count
import processors.utils as utils


class RecordUpdater:

    ns = {'': 'http://www.loc.gov/MARC21/slim'}

    connector = OclcConnector()
    database_update = DatabaseUpdate()

    failed_oclc_lookup_count = 0
    updated_001_count = 0
    updated_003_count = 0
    updated_leader_count = 0
    processed_records_count = 0
    modified_count = 0
    unmodified_count = 0
    bad_record_count = 0
    fuzzy_record_count = 0

    update_policy = None
    is_online = False
    replacement_strategy = None
    bad_oclc_reponse_writer = None
    original_fuzzy_writer = None
    check_001_match_accuracy = False
    title_check = False
    require_perfect_match = False
    fuzzy_match_ratio = 50


    conn = None
    cursor = None

    oclc_api_error_log = open('output/audit/oclc_api_error_log.txt', 'w')

    def __init__(self,
                 database_name=None,
                 password=None,
                 oclc_developer_key=None,
                 modified_writer=None,
                 unmodified_writer=None,
                 bad_writer=None,
                 title_log_writer=None,
                 oclc_xml_writer=None,
                 field_audit_writer=None,
                 fuzzy_record_writer=None,
                 fuzzy_online_writer=None,
                 updated_online_writer=None,
                 unmodified_online_writer=None,
                 field_035_details_writer=None):
        """
        Constructor.
        :param database_name Optional database name
        :param password Optional database password
        :param modified_writer The output file writer
        :param unmodified_writer The output file writer for unmodifed records
        :param bad_writer The output file records that cannot be processed
        :param title_log_writer The output title for fuzzy matched titles
        :param oclc_xml_writer The output file for OCLC xml
        :param field_audit_writer The output file tracking field updates
        :param updated_online_writer Output pretty records for updated online items
        :param unmodified_online_writer Output pretty records for unmodified online items
        :param fuzzy_online_writer Output pretty records for fuzzy online items
        :param fuzzy_record_writer Output pretty records with fuzzy OCLC title match
        :param field_035_details_writer Outputs 035(z) audit
        :param oclc_developer_key The developer key used to query OCLC
        """

        self.database_name = database_name
        self.password = password
        self.oclc_developer_key = oclc_developer_key
        self.modified_writer = modified_writer
        self.unmodified_writer = unmodified_writer
        self.bad_writer = bad_writer
        self.title_log_writer = title_log_writer
        self.oclc_xml_writer = oclc_xml_writer
        self.field_audit_writer = field_audit_writer
        self.fuzzy_record_writer = fuzzy_record_writer
        self.fuzzy_online_writer = fuzzy_online_writer
        self.updated_online_writer = updated_online_writer
        self.unmodified_online_writer = unmodified_online_writer
        self.field_035_details_writer = field_035_details_writer

    def update_fields_using_oclc(self,
                                 file,
                                 plugin,
                                 require_perfect_match,
                                 title_check,
                                 add_to_database,
                                 use_database,
                                 do_fuzzy_001_test=False,
                                 encoding=None,
                                 fuzzy_match_ratio=50,
                                 replacement_strategy='replace_and_add') -> None:
        """
        Initializes the processing properties, reads the input file, and reports results.
        :param file The marc file (binary)
        :param plugin The module to use when modifying records
        :param require_perfect_match If True a perfect title match with OCLC is required
        :param title_check If true will do 245ab title match
        :param add_to_database If true insert API response into database
        :param use_database If true retrieve OCLC records from the database
        :param do_fuzzy_001_test Indicates whether a separate test is run when fuzzy matching 001/003 combinations
        Default False.
        :param encoding optional encoding parameter.
        :param fuzzy_match_ratio The value used in fuzzy match logging to determine pass/fail status. Default 50.
        :param replacement_strategy strategy used for OCLC replacement values. Default is replace_and_add
        :return:
        """

        self.replacement_strategy = replacement_strategy
        self.title_check = title_check
        self.fuzzy_match_ratio = fuzzy_match_ratio
        self.require_perfect_match = require_perfect_match

        if plugin:
            klass = getattr(import_module(plugin), 'UpdatePolicy')
            self.update_policy = klass()

        if not self.update_policy:
            print('WARNING: You are processing records without a plugin.')
            print('Records will only be updated with OCLC data.')

        print('Using replacement strategy: ' + self.replacement_strategy)

        self.field_audit_writer = self.field_audit_writer

        dt = datetime.datetime.now()

        missing_required_field_writer = TextWriter(
            open('output/audit/missing-245a-pretty-' + str(dt) + '.txt', 'w'))

        if not use_database:
            self.bad_oclc_reponse_writer = open('output/xml/bad-oclc-response-' + str(dt) + '.xml', 'w')
        else:
            self.bad_oclc_reponse_writer = None

        if require_perfect_match:
            self.original_fuzzy_writer = TextWriter(
                open('output/updated-records/fuzzy-original-records-pretty-' + str(dt) + '.txt', 'w'))

        if self.oclc_xml_writer is not None:
            self.oclc_xml_writer.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>')
            self.oclc_xml_writer.write('<collection xmlns="http://www.loc.gov/MARC21/slim" '
                                  'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                                  'xsi:schemaLocation="http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">')

        if use_database or add_to_database:
            if not self.database_name:
                raise Exception("You need to provide the database name.")
            else:
                # If the cursor property is defined requests for
                # OCLC data will use the database instead of the API.
                self.__set_db_connection()

        # If adding records to database we need purge any existing records.
        if add_to_database:
            self.cursor.execute('DELETE FROM oclc')
            self.conn.commit()

        wrapper = MarcReader()
        if encoding:
            # Get reader method using optional encoding.
            reader = wrapper.get_reader_unicode(file, encoding)
        else:
            reader = wrapper.get_reader(file)

        for record in reader:
            if record:

                self.processed_records_count += 1

                field_001 = None
                field_035 = None
                oclc_api_search_id = None
                oclc_001_value = None
                oclc_response = None
                self.is_online = False

                try:
                    if not record.title():
                        print('Record is missing title information')
                        missing_required_field_writer.write(record)
                    if len(record.get_fields('001')) == 1:
                        field_001 = utils.get_oclc_001_value(record['001'], record['003'])
                    if len(record.get_fields('035')) > 0:
                        field_035 = utils.get_035(record)
                        utils.log_035_details(record.get_fields('035'), record.title(), self.field_035_details_writer)

                except Exception as err:
                    print('error reading fields from input record.')
                    print(err)

                # The 035 takes precedence here because
                # of the optional fuzzy match test on the 001.
                # Giving precedence to the 035 reduces the chance
                # that we exclude a valid OCLC match because it's
                # match score doesn't meet the do_fuzzy_001_test.
                if field_035:
                    oclc_api_search_id = field_035
                elif field_001:
                    if do_fuzzy_001_test:
                        self.check_001_match_accuracy = True
                    oclc_api_search_id = field_001

                if self.update_policy:
                    # Note that the online records are only processed separately
                    # when using a plugin. This is because determining
                    # the online status varies between systems and/or
                    # cataloging conventions.
                    self.is_online = self.update_policy.is_online(record)

                try:
                    # If an OCLC number was found, retrieve data from
                    # the Worldcat API or from the local database.
                    if oclc_api_search_id:
                        oclc_response = self.__get_oclc_response(oclc_api_search_id, add_to_database)

                    # Process with the matching OCLC record.
                    if oclc_response is not None:
                        self.__process_oclc_match(record, oclc_response, oclc_api_search_id, add_to_database)

                    # Records with no OCLC match can be modified as-is and written to the
                    # unmodified records file.
                    else:
                        self.__write_unmodifed_record(record, oclc_001_value)
                        self.unmodified_count += 1

                except HTTPError as err:
                    print(err)
                except UnicodeEncodeError as err:
                    print(err)

            else:
                self.bad_record_count += 1
                print(reader.current_exception)
                print(reader.current_chunk)
                self.bad_writer.write(reader.current_chunk)

        reader.close()

        if self.oclc_xml_writer is not None:
            self.oclc_xml_writer.write('</collection>')

        if self.conn is not None:
            self.conn.close()

        print('Total processed records count: ' + str(self.processed_records_count))
        print('Total records should equal the modified count plus the unmodified count.')
        print()
        print('Total modified records count: ' + str(self.modified_count))
        print('Unmodified records count: ' + str(self.unmodified_count))
        print('Records modified using an imperfect fuzzy match: ' + str(self.fuzzy_record_count))
        print('Bad record count: ' + str(self.bad_record_count))
        print()

        field_count_dict = field_count.get_field_count()
        field_c = 0
        print('leader: ' + str(self.updated_leader_count))
        print('001: ' + str(self.updated_001_count))
        print('003: ' + str(self.updated_003_count))
        for key in field_count_dict.keys():
            print(key + ': ' + str(field_count_dict[key]))
            field_c += field_count_dict[key]

        print('Total fields replaced: ' + str(field_c))
        print()
        if not use_database:
            print('Failed OCLC record retrieval count: ' + str(self.failed_oclc_lookup_count))
        print()
        if self.update_policy:
            self.update_policy.print_online_record_counts()

    def __process_oclc_match(self, record, oclc_response, oclc_search_id, add_to_database):
        """
        Primary method for updating a single record with OCLC data.
        Does record processing based on class properties. Reports
        the result to console.
        :param record: pymarc record to be modifed
        :param oclc_response: OCLC API response
        :param oclc_search_id: the ID used in the API request
        :param add_to_database: boolean indicating when API results should be added to database
        :return:
        """
        oclc_001_value = self.__get_field_text('001', oclc_response)

        # Log if the OCLC response does not include 001. This happens
        # when the API returns an error message. Service unavailable
        # errors are rare since up to 3 API requests are made. The most common
        # error is file not found.
        if oclc_search_id and not oclc_001_value:
            print('Missing oclc 001 for ' + oclc_search_id)
            if self.bad_oclc_reponse_writer:
                self.bad_oclc_reponse_writer.write(ET.tostring(oclc_response,
                                                          encoding='utf8',
                                                          method='xml'))
        # Add record to database if requested.
        title = record.title()
        if oclc_search_id and add_to_database:
            self.__database_insert(oclc_search_id,
                                   oclc_001_value,
                                   oclc_response,
                                   title)

        # Write to the OCLC record to file if file handle was provided.
        if self.oclc_xml_writer is not None:
            self.oclc_xml_writer.write(str(ET.tostring(oclc_response,
                                                       encoding='utf8',
                                                       method='xml')))

        # Modify records if input title matches that of the OCLC response.
        #
        # If "require_perfect_match" is True, an exact match
        # on the 245(a)(b) fields is required. Exact matches are written to the
        # updated records file. Imperfect (fuzzy) matches are written to a
        # separate file and labeled in the 962 for later review.
        #
        # If the "require_perfect_match" is False, field substitution
        # will take place when the match ratio is greater than the default
        # fuzzy_match_ratio. Records will be written to the updated records
        # output file.
        #
        # Verification fails when the oclc_response is None. The record
        # will be written to unmodified records file.

        # If not checking titles, just process the record and return.
        if not self.title_check:
            self.__update_record_with_oclc(record, oclc_response, oclc_001_value)
            self.__material_type_analysis(record, 'updated')
            return

        # Get the titles array from the OCLC response.  The first array
        # element is the full title to use when logging. The
        # second element is the title to use in fuzzy comparisons.
        # It is derived from 245 subfield a and b.
        title_for_comparison = utils.get_oclc_title(oclc_response)
        match_ratio = utils.get_match_ratio(title_for_comparison[1], record.title())

        # set label for 962 note.
        if match_ratio >= self.fuzzy_match_ratio:
            fuzzy_match_label = 'fuzzy-match-passed'
        else:
            fuzzy_match_label = 'fuzzy-match-failed'

        # Perfect title match.
        if match_ratio == 100:

            self.__process_oclc_replacements(record, oclc_response, oclc_001_value,
                                             None, 'updated_with_perfect_match')
            self.__write_record(record)

            self.modified_count += 1

        # When "require_perfect_match" is True do substitutions for records
        # with an imperfect OCLC title match. These records will be written to a
        # separate file for "fuzzy" matches. The pass/fail status will be
        # labeled in the 962 field. Recommended if you want to review files
        # with imperfect OCLC title matches.
        elif self.require_perfect_match:
            # Write the original version of the record to a separate output
            # file so the original is available to the reviewer.
            self.original_fuzzy_writer.write(record)
            # If the do_fuzzy_001_test parameter is True, compensate for
            # invalid OCLC numbers in the 003/001 field combination.
            if self.check_001_match_accuracy:
                # When the match_ratio for this record is less than the fuzzy match ratio,
                # the record is not updated with OCLC data and instead is written to the
                # unmodified file.
                #
                # To determine the best test_ratio, value, analyze the title-fuzzy-match
                # audit file. If the matches look good overall, then skip this step
                # entirely (by setting do_fuzzy_001_test to be False). If
                # there are many bad record overlays, use the fuzzy audit file
                # to determine the optimal value for the test_ratio.
                #
                # This was added for a specific marc record set with
                # 001/003 combinations that produced many invalid matches.

                # If the 001 does not have an OCLC prefix and match
                # ratio is too low, skip field replacement and write to
                # the unmodified file.
                if not utils.is_oclc_prefix(record['001']) and not match_ratio >= self.fuzzy_match_ratio:
                    # The test failed, do not modify the record.
                    self.__write_unmodifed_record(record, oclc_001_value)
                    self.unmodified_count += 1
                else:
                    utils.log_fuzzy_match(record.title(), title_for_comparison[1], title_for_comparison[0],
                                          match_ratio, self.fuzzy_match_ratio, oclc_001_value,
                                          self.title_log_writer)
                    self.__process_oclc_replacements(record, oclc_response, oclc_001_value,
                                                     fuzzy_match_label, 'updated_with_fuzzy_match')
                    self.__write_fuzzy_record(record)
                    self.fuzzy_record_count += 1
                    self.modified_count += 1

            # If not analyzing 001/003 combinations, process fuzzy matches normally and write
            # to the separate fuzzy output file.
            else:
                utils.log_fuzzy_match(record.title(), title_for_comparison[1], title_for_comparison[0],
                                      match_ratio, self.fuzzy_match_ratio,
                                      oclc_001_value, self.title_log_writer)
                self.__process_oclc_replacements(record, oclc_response, oclc_001_value,
                                                 fuzzy_match_label, 'updated_with_fuzzy_match')
                self.__write_fuzzy_record(record)
                self.fuzzy_record_count += 1
                self.modified_count += 1
        else:
            # If a perfect match is not required, update records that
            # have a ratio greater than the required fuzzy_match_ratio
            # and write to the updated file. Log the record's fuzzy status.
            # If fuzzy match fails to meet the minimum, do not update the record
            # and write to the unmodified file.
            #
            # You may want to do this when you are confident that OCLC record matches
            # are accurate and you don't want to bother with a separate fuzzy file for
            # titles that do not match perfectly. Doing this may result in more unmodified
            # records.
            if match_ratio >= self.fuzzy_match_ratio:
                utils.log_fuzzy_match(record.title(), title_for_comparison[1], title_for_comparison[0],
                                      match_ratio, self.fuzzy_match_ratio,
                                      oclc_001_value, self.title_log_writer)
                self.__process_oclc_replacements(record, oclc_response, oclc_001_value,
                                                 fuzzy_match_label, 'updated_with_fuzzy_match')
                self.__write_fuzzy_record(record)
                self.fuzzy_record_count += 1
                self.modified_count += 1
            else:
                self.__write_unmodifed_record(record, oclc_001_value)
                self.unmodified_count += 1

    def __process_oclc_replacements(self, record, oclc_response, value001, fuzzy_match_label, update_label):
        """
        Calls the record update and audit methods.
        :param record: pymarc record
        :param oclc_response: OCLC API response
        :param value001: 001 value from OCLC response
        :param fuzzy_match_label: label for the fuzzy match audit log
        :param update_label: label for location analysis log
        :return:
        """
        self.__update_record_with_oclc(record, oclc_response, value001, fuzzy_match_label)
        self.__material_type_analysis(record, update_label)

    def __update_record_with_oclc(self, record, oclc_response, field_001, status=None):
        """
        Updates record using the OCLC response and applies local
        update policy if a plugin was provided.
        :param record: pymarc record
        :param oclc_response: oclc xml response
        :param field_001: oclc 001 field value
        :param status: Indicates status if fuzzy match
        :return:
        """
        # replace fields
        self.replace_fields(field_001, record, oclc_response)
        # add fuzzy match status label
        if status is not None:
            field_generator = DataFieldGenerator()
            field = field_generator.create_data_field('962', [0, 0], 'a', status)
            record.add_ordered_field(field)
        # execute plugin policy after OCLC updates
        if self.update_policy:
            self.update_policy.execute(record, field_001)

    def __write_unmodifed_record(self, record, value001):
        """
        Process and write unmodified record
        :param record: pymarc record
        :param value001: 001 value from record
        :return:
        """
        if self.update_policy:
            for field_move in self.update_policy.conditional_move_tags():
                if len(field_move) == 2:
                    # There was no OCLC response, so this move is not conditional.
                    self.__move_field(record, field_move[0], field_move[1])
            self.update_policy.execute(record, value001)
            # Non-updated records should be passed to the plugin's method
            # for updating 001/003.
            self.update_policy.set_local_id(record)
            self.__material_type_analysis(record, 'unmodified_records')

        if self.is_online:
            self.unmodified_online_writer.write(record)
        else:
            self.unmodified_writer.write(record)

    def __set_db_connection(self):
        """
        Sets the database connection and cursor properties.
        :return:
        """
        # If database provided, initialize the connection and set cursor.
        db_connect = DatabaseConnector()
        self.conn = db_connect.get_connection(self.database_name, self.password)
        print("Database opened successfully.")
        self.cursor = self.conn.cursor()

    def __write_fuzzy_record(self, record):
        """
        Write record to fuzzy output file
        :param record: pymarc record
        :return:
        """
        if self.is_online:
            self.fuzzy_online_writer.write(record)
        else:
            self.fuzzy_record_writer.write(record)

    def __write_record(self, record):
        """
        Write record to the updated record output file.
        :param record: pymarc record
        :return:
        """
        if self.is_online:
            self.updated_online_writer.write(record)
        else:
            self.modified_writer.write(record)

    def __get_oclc_response(self, oclc_number, database_insert):
        """
        Retrieves the OCLC record from API or database
        :param oclc_number: record number
        :param database_insert: database insert task boolean
        :return: oclc response node
        """
        oclc_response = None
        if self.cursor is not None and not database_insert:
            self.cursor.execute("""SELECT oclc FROM oclc where id=%s""", [oclc_number])
            row = self.cursor.fetchone()
            if row:
                oclc_response = ET.fromstring(row[0])
        else:
            # This will make multiple API requests if
            # initial response returns error.
            oclc_response = self.__get_oclc_api_response(oclc_number)

        return oclc_response

    def __material_type_analysis(self, record, output_file):
        """
        Applies optional material type analysis if plugin was
        provided.
        :param record: pymarc record
        :param output_file: a label for the output file used for this record
        :return:
        """
        if self.update_policy:
            self.update_policy.analyze_type(record, output_file)

    def __get_field_text(self, field, oclc_response):
        """
        Gets value for the requested field text.
        :param field: field tag
        :param oclc_response: OCLC marcxml or root node
        :return: field value
        """
        oclc_field = self.__get_oclc_element_field(field, oclc_response)
        if oclc_field is not None:
            return oclc_field.text

        return None

    @staticmethod
    def __get_oclc_element_field(field, oclc_response):
        """
        Gets the field element from oclc response.
        :param field: the field to return
        :param oclc_response: the initial OCLC response (used if valid)
        :return: the OCLC field node
        """
        if oclc_response is not None:
            return oclc_response.find('./*[@tag="' + field + '"]')
        return None

    @staticmethod
    def __remove_fields(field, record):
        """
        Removes 1xx fields. This should be called before
        adding OCLC data to the record.
        The method is called after a successful OCLC
        fetch and before data is added to the
        record.
        :param field: current field
        :param record: current record
        :return:
        """
        field_1xx_regex = re.compile('^1\d{2}')
        # Single 1xx field allowed in record.
        if field_1xx_regex.match(field):
            record.remove_fields('100', '110', '111', '130')
        else:
            record.remove_fields(field)

    @staticmethod
    def __get_original_values(originals):
        """
        Get values from field array.
        :param originals: fields from original marc record
        :return: list of field values
        """
        values = []
        for original in originals:
            values.append(original.value())
        return values

    def __write_to_audit_log(self, replacement_field, original_fields, field, control_field):
        """
        Writes field replacements to audit log.
        :param replacement_field: replacement field tag
        :param original_fields: fields from original marc record
        :param field: current pymarc Field
        :param control_field: value of the record 001 or the record title varying with data and control field contexts.
        :param writer: the audit log writer
        :return:
        """
        for single_field in self.__get_original_values(original_fields):
            # Output order: oclc #, tag, new field value, original field value.
            self.field_audit_writer.write(control_field + '\t'
                         + replacement_field + '\t'
                         + field.value() + '\t'
                         + single_field + '\n')

    def __move_field(self, record, current_field_tag, new_field_tag):
        """
        Moves field to a new field in the record. Used
        to preserve local fields during ingest.
        :param record: pymarc record
        :param current_field_tag: tag of the current field
        :param new_field_tag: tag of the target field
        :return:
        """
        self.__update_field_in_record(record, current_field_tag, new_field_tag)

    def __conditional_move_field(self, record, replacement_field_tag, target_field_tag, oclc_response):
        """
        Conditionally moves field to a new field in the record. Used
        to preserve 505 field during ingest when no replacement is
        provided by OCLC.
        :param record: pymarc record
        :param replacement_field_tag: tag of the field to move
        :param target_field_tag: tag of the new field
        :param oclc_response: the OCLC marcxml response
        :return:
        """
        # Test to see if replacement was provided in the OCLC response.
        # If not, move the field in the current record to preserve information
        # in a local field.
        oclc_field = oclc_response.find('./*[@tag="' + replacement_field_tag + '"]')
        if oclc_field is None:
            self.__update_field_in_record(record, replacement_field_tag, target_field_tag)

    @staticmethod
    def __update_field_in_record(record, origin_field_tag, target_field_tag):
        fields = record.get_fields(origin_field_tag)
        for field in fields:
            subs = utils.get_subfields_arr(field)
            target_field = Field(
                tag=target_field_tag,
                indicators=[field.indicator1, field.indicator2],
                subfields=subs
            )
            record.remove_field(field)
            record.add_ordered_field(target_field)
            field_count.update_field_count(target_field_tag)

    def __data_field_update(self, record, replacement_field_tag, oclc_response):
        """
        Updates the record data field using OCLC XML response.

        :param record: the pymarc Record
        :param replacement_field_tag: the field to replace
        :param oclc_response: the OCLC XML response
        """
        field_generator = DataFieldGenerator()
        # get the replacement fields from OCLC response
        tags = oclc_response.findall('.//*[@tag="' + replacement_field_tag + '"]', self.ns)
        if len(tags) > 0:
            # get the replacement fields from the original record for adding to audit file.
            original_fields = record.get_fields(replacement_field_tag)
            field_001 = record['001'].value()
            # remove replacement fields from the original record
            self.__remove_fields(replacement_field_tag, record)
            for f in tags:
                field = field_generator.get_data_field(f, f.attrib, replacement_field_tag)
                if field:
                    if self.field_audit_writer:
                        self.__write_to_audit_log(replacement_field_tag, original_fields, field,
                                                  field_001)
                    # add new field with OCLC data to record
                    record.add_ordered_field(field)
                    # increment field count
                    field_count.update_field_count(replacement_field_tag)
        else:
            # The policy plugin may define fields that should be moved to a
            # local field when they are not replaced by OCLC data.
            if self.update_policy:
                for field_move in self.update_policy.conditional_move_tags():
                    if len(field_move) == 2:
                        self.__conditional_move_field(record, field_move[0], field_move[1], oclc_response)
                    else:
                        print("Something is wrong with the conditional move configuration.")

    def __replace_control_field(self, record, replacement_field, oclc_response):
        """
        Updates record control field using OCLC response.

        :param record: the pymarc Record
        :param replacement_field: the field to replace
        :param oclc_response: the OCLC XML response
        """
        field_generator = ControlFieldGenerator()
        field = field_generator.get_control_field(replacement_field, oclc_response)
        if field:
            original_fields = record.get_fields(replacement_field)
            field_001 = record['001'].value()
            if self.field_audit_writer:
                self.__write_to_audit_log(replacement_field, original_fields, field, field_001)

            record.remove_fields(replacement_field)
            record.add_ordered_field(field)

    @staticmethod
    def __replace_oclc_001_003(record, oclc_number):
        """
        Add 001 and 003 fields to a record. Use to guarantee this
        information is in every record.
        :param record:  pymarc record
        :param oclc_number: OCLC number
        """
        record.remove_fields('001')
        record.remove_fields('003')
        field_001 = Field(
            tag='001',
            data=oclc_number
        )
        field_003 = Field(
            tag='003',
            data='OCoLC'
        )
        record.add_ordered_field(field_001)
        record.add_ordered_field(field_003)

    def __replace_leader(self, record, oclc_reponse):
        """
        Replaces existing record leader with OCLC value
        :param record: The record pymarc root node
        :param oclc_reponse: the OCLC API response node
        :return:
        """
        oclc_leader = oclc_reponse.find('./', self.ns)
        new_leader = Leader(oclc_leader.text)
        if new_leader:
            record.leader = new_leader

    def __get_oclc_api_response(self, field_value):
        """
        Makes OCLC API request and tests for a valid response. Will attempt
        up to 3 requests before failing. Failure is unlikely unless OCLC is
        having serious problems, or, of course, if the error is file not found.
        :param field_value: the oclc number
        :return: oclc response node
        """
        diagnostic = ''
        oclc_response = self.connector.get_oclc_response(field_value, self.oclc_developer_key)
        oclc_field = oclc_response.find('./*[@tag="001"]')
        # API returns occasional error. Second attempt
        # should be enough to guarantee we get a response.
        if oclc_field is None:
            time.sleep(0.5)
            oclc_response = self.connector.get_oclc_response(field_value, self.oclc_developer_key)
            oclc_field = oclc_response.find('./*[@tag="001"]')
        if oclc_field is None:
            # extra special repeat performance
            time.sleep(0.3)
            oclc_response = self.connector.get_oclc_response(field_value, self.oclc_developer_key)
            oclc_field = oclc_response.find('./*[@tag="001"]')
        if oclc_field is None:
            self.failed_oclc_lookup_count += 1
            diagnostic = ET.tostring(oclc_response, encoding='unicode', method='xml')
            oclc_response = None

        if oclc_response is None:
            self.oclc_api_error_log.write('WARNING: No OCLC response for: ' + field_value)
            self.oclc_api_error_log.write('The response was: ' + diagnostic)

        return oclc_response

    def __database_insert(self, field, oclc_field, oclc_response, title):
        """
        Insert OCLC record into local database
        :param cursor: db cursor
        :param conn: database connection
        :param field:  the 001 field value from local input
        :param oclc_field: the 001 node from oclc response
        :param oclc_response: the OCLC xml reponse
        :param title: the item title
        :return:
        """
        if self.cursor is not None:
            try:
                self.database_update.add_response(
                    field,
                    oclc_response,
                    oclc_field,
                    title,
                    self.cursor
                )
                self.conn.commit()
            except Exception as err:
                print(err)
        else:
            print('Missing database connection.')

    @staticmethod
    def __is_control_field(field):
        if re.match("^00", field):
            return True
        return False

    def replace_fields(self, oclc_001_value, record, oclc_response):
        """
        Handles all OCLC field replacements
        :param oclc_001_value: the 001 value from OCLC
        :param record: the record node
        :param oclc_response: the response from OCLC
        :return:
        """
        if not self.replacement_strategy:
            print('WARNING: You have not defined a replacement strategy.')
            print('Using default strategy.')

        # Assure OCLC values are in 001 and 003. Alma will generate 035.
        # Do this after title validation.
        if oclc_001_value:
            # Update 001 with the value returned in the OCLC API response. This
            # can differ from the original value in the input records.
            self.__replace_oclc_001_003(record, oclc_001_value)
            self.updated_001_count += 1
            self.updated_003_count += 1
        else:
            raise Exception('Something is wrong, OCLC response missing 001.')

        # Replace the leader with OCLC leader value.
        self.__replace_leader(record, oclc_response)
        self.updated_leader_count += 1

        if self.replacement_strategy == 'replace_and_add':
            # This strategy replaces and adds new fields if they exist in the OCLC data.
            for sub in substitution_array:
                if self.__is_control_field(sub):
                    self.__replace_control_field(record, sub, oclc_response)
                else:
                    self.__data_field_update(record, sub, oclc_response)

        elif self.replacement_strategy == 'replace_only':
            # This strategy only replaces fields that already exist in the record.
            fields = record.get_fields()

            for field in fields:
                if field.tag in substitution_array:
                    if self.__is_control_field(field.tag):
                        self.__replace_control_field(record, field.tag, oclc_response)
                    else:
                        self.__data_field_update(record, field.tag, oclc_response)
