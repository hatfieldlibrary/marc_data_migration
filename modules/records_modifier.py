from urllib.error import HTTPError
import xml.etree.ElementTree as ET
from pymarc import MARCReader, Field, Leader

from modules.add_response_to_database import DatabaseUpdate
from modules.field_generators import ControlFieldGenerator, DataFieldGenerator
from modules.oclc_connector import OclcConnector
from db_connector import DatabaseConnector
import modules.utils as utils
import modules.field_replacement_count as field_count
import re


class RecordsModifier:
    connector = OclcConnector()

    ns = {'': 'http://www.loc.gov/MARC21/slim'}
    oclc_developer_key = ''
    database_update = DatabaseUpdate()

    @staticmethod
    def __remove_fields(field, record):
        """
        Removes field, first checking for 1xx an removing
        all 1xx fields on match.  The OCLC record may include
        a different 1xx than the original file, so this is
        necessary. This method is called after a successful
        OCLC data fetch and before OCLC data is added to the
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

    def __get_original_values(self, originals):
        """
        Get values from field array.
        :param originals: fields from original marc record
        :return: list of field values
        """
        values = []
        for original in originals:
            values.append(original.value())
        return values

    def __write_to_audit_log(self, replacement_field, original_fields, field, control_field, writer):
        """
        Writes field replacements to audit log.
        :param replacement_field: name of the replacement field
        :param original_fields: fields from original marc record
        :param field: current pymarc Field
        :param control_field: value of the record 001 or the record title varying with data and control field contexts.
        :param writer: the audit log writer
        :return:
        """
        for single_field in self.__get_original_values(original_fields):
            writer.write(control_field + '\t'
                         + replacement_field + '\t'
                         + field.value() + '\t'
                         + single_field + '\n')

        field_count.update_field_count(replacement_field)

    def __conditional_move(self, record, replacement_field_tag, oclc_response):
        # Move the existing 505(a) to 590 when no replacement value was
        # provided by OCLC
        if replacement_field_tag == '505':
            # Test to see if replacement 505 was provided in OCLC record.
            # If not, move field in the current record to preserve in local
            # 590 field.
            oclc_field = oclc_response.find('./*[@tag="505"]')
            if oclc_field is None:
                field_505s = record.get_fields('505')
                # found 505 in original record
                if len(field_505s) > 0:
                    # capture the single 505 field
                    field_505 = field_505s[0]
                    # clear existing record field
                    record.remove_fields('505')
                    # indicators from record field
                    indicators = [field_505.indicator1, field_505.indicator2]
                    subfield_505 = field_505.get_subfields('a')
                    # subfield a from record
                    if len(subfield_505) == 1:
                        subfields = ['a', subfield_505[0]]
                        # create new 590 with data from the 505
                        field = Field(
                            tag='590',
                            indicators=indicators,
                            subfields=subfields
                        )
                        # add 590 to record
                        record.add_ordered_field(field)
                        field_count.update_field_count('590')

    def __data_field_update(self, record, replacement_field_tag, oclc_response, audit_writer):
        """
        Updates the record data field using OCLC XML response.

        :param record: the pymarc Record
        :param replacement_field_tag: the field to replace
        :param oclc_response: the OCLC XML response
        """
        field_generator = DataFieldGenerator()
        tags = oclc_response.findall('.//*[@tag="' + replacement_field_tag + '"]', self.ns)
        if len(tags) > 0:
            original_fields = record.get_fields(replacement_field_tag)
            field_001 = record['001'].value()
            self.__remove_fields(replacement_field_tag, record)
            for f in tags:
                field = field_generator.get_data_field(f, f.attrib, replacement_field_tag)
                if field:
                    if audit_writer:
                        self.__write_to_audit_log(replacement_field_tag, original_fields, field, field_001, audit_writer)
                    record.add_ordered_field(field)
        else:
            if replacement_field_tag == '505':
                    self.__conditional_move(record, replacement_field_tag, oclc_response)

    def __control_field_update(self, record, replacement_field, oclc_response, audit_writer):
        """
        Updates the record control fields using OCLC XML response.

        :param record: the pymarc Record
        :param replacement_field: the field to replace
        :param oclc_response: the OCLC XML response
        """
        field_generator = ControlFieldGenerator()
        field = field_generator.get_control_field(replacement_field, oclc_response)
        if field:
            original_fields = record.get_fields(replacement_field)
            field_001 = record['001'].value()
            if audit_writer:
                self.__write_to_audit_log(replacement_field, original_fields, field, field_001, audit_writer)

            record.remove_fields(replacement_field)
            record.add_ordered_field(field)

    @staticmethod
    def __add_oclc_001_003(record, oclc_number):
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
        '''
        Replaces existing record leader with OCLC value
        :param record: The record pymarc root node
        :param oclc_reponse: the OCLC API response node
        :return:
        '''
        oclc_leader = oclc_reponse.find('./', self.ns)
        new_leader = Leader(oclc_leader.text)
        if new_leader:
            record.leader = new_leader

    def __get_oclc_field(self, field_value, oclc_response):
        '''
        Gets the 001 field value from oclc.  Redundant API
        call in case first request returned error
        :param field_value: the oclc id  to query
        :param oclc_response: the initial OCLC response (used if valid)
        :return: the OCLC field node
        '''
        oclc_field = oclc_response.find('./*[@tag="001"]')
        # API returns occasional error. Second attempt
        # should be enough to guarantee we get a response.
        if oclc_field is None:
            oclc_response = self.connector.get_oclc_response(field_value, self.oclc_developer_key)
            oclc_field = oclc_response.find('./*[@tag="001"]')
        return oclc_field

    def __database_insert(self, cursor, conn, field, oclc_field, oclc_response, title):
        '''
        Insert OCLC record into local database
        :param cursor: db cursor
        :param conn: database connectin
        :param field:  the 001 field value from local input
        :param oclc_field: the 001 node from oclc response
        :param oclc_response: the OCLC xml reponse
        :param title: the item title
        :return:
        '''
        if cursor is not None:
            try:
                self.database_update.add_response(
                    field,
                    oclc_response,
                    oclc_field,
                    title,
                    cursor
                    )
                conn.commit()
            except Exception as err:
                print(err)
        else:
            print('Missing database connection.')

    def update_fields_using_oclc(self,
                                 file,
                                 database_name,
                                 password,
                                 substitutions,
                                 title_check,
                                 database_insert,
                                 writer,
                                 unmodified_writer,
                                 bad_writer,
                                 title_log_writer,
                                 oclc_xml_writer,
                                 field_audit_writer,
                                 cancelled_oclc_writer,
                                 oclc_developer_key):
        """
        Updates records from input marc file with data obtained
        from OCLC worldcat.  The method takes a substitutions array
        that specifies the fields to be updated.
        :param file: The marc file (binary)
        :param database_name: Optional database name
        :param password: Optional database password
        :param substitutions: he array of fields to update
        :param title_check: If true will do 245a fuzzy title match
        :param database_insert: If true insert API repsonse into database
        :param writer: The output file writer
        :param unmodified_writer: The output file writer for unmodifed records
        :param bad_writer: The output file records that cannot be processed
        :param title_log_writer: The output title for fuzzy matched titles
        :param oclc_xml_writer: The output file for OCLC xml
        :param field_audit_writer: The output file tracking field updates
        :param cancelled_oclc_writer: Outputs 035(z) audit
        :param oclc_developer_key: The developer key used to query OCLC
        :return:
        """

        self.oclc_developer_key = oclc_developer_key

        if oclc_xml_writer is not None:
            oclc_xml_writer.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>')
            oclc_xml_writer.write('<collection xmlns="http://www.loc.gov/MARC21/slim" '
                                  'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                                  'xsi:schemaLocation="http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">')

        conn = None
        cursor = None
        if database_name:
            # If database provided, initialize the connection and get cursor. If not
            # provided the OCLC API will be used.
            db_connect = DatabaseConnector()
            conn = db_connect.get_connection(database_name, password)
            print("Database opened successfully")
            cursor = conn.cursor()

        with open(file, 'rb') as fh:
            modified_count = 0
            unmodified_count = 0
            bad_record_count = 0
            updated_003_count = 0
            updated_001_count = 0
            updated_leader_count = 0
            # Set to permissive to avoid exiting loop; report
            # unreadable records in the output. Prevent python utf-8
            # handling.
            reader = MARCReader(fh, permissive=True, utf8_handling='ignore')

            for record in reader:
                if record:
                    field_001 = None
                    field_035 = None
                    current_oclc_number = None
                    oclc_001_value = None
                    title = ''

                    try:
                        if not record.title():
                            print('record missing 245(a)')
                        if record['245'] and record['245']['a']:
                            title = utils.get_original_title(record)
                        # Get values for 001 value, 035 value, and title.
                        # These are used to request and verify data from OCLC.
                        if len(record.get_fields('001')) == 1:
                            field_001 = utils.get_oclc_001_value(record['001'], record['003'])
                        if len(record.get_fields('035')) > 0:
                            fields = record.get_fields('035')
                            for field in fields:
                                subfields = field.get_subfields('a')
                                if len(subfields) > 1:
                                    print('duplicate 035a')
                                elif len(subfields) == 1:
                                    field_035 = utils.get_oclc_035_value(subfields[0])
                                    utils.log_035z(record['035'], field_035, cancelled_oclc_writer)

                    except Exception as err:
                        print('error reading fields from input record.')
                        print(err)

                    try:

                        # Ugh. The field processing is awkward. Needs improvement. The
                        # mess is partly the result of unpredictability in API responses
                        # and partly complex parameterization around database use.

                        oclc_response = None
                        # Use 001 by default. Try 035 if the 001 is not available.
                        if field_001:
                            # set current oclc number to 001 value.
                            current_oclc_number = field_001

                            # Fetch record from database
                            if cursor is not None and not database_insert:
                                cursor.execute("""SELECT oclc FROM oclc where id=%s""", [field_001])
                                row = cursor.fetchone()
                                if row:
                                    oclc_response = ET.fromstring(row[0])
                                    oclc_field = oclc_response.find('./*[@tag="001"]')
                                    oclc_001_value = oclc_field.text

                            # Fetch record from OCLC and optionally update local database.
                            else:

                                oclc_response = self.connector.get_oclc_response(field_001, oclc_developer_key)
                                oclc_field = self.__get_oclc_field(field_001, oclc_response)
                                oclc_001_value = oclc_field.text

                                if oclc_field is not None:
                                    if database_insert:
                                        self.__database_insert(cursor, conn, field_001, oclc_field, oclc_response, title)
                                else:
                                    print('Missing oclc 001 for ' + field_001)

                            if oclc_xml_writer is not None:
                                oclc_xml_writer.write(str(ET.tostring(oclc_response,
                                                                          encoding='utf8',
                                                                          method='xml')))

                        elif field_035:
                            # set current oclc number to 035 value
                            current_oclc_number = field_035

                            # Fetch record from database
                            if cursor is not None and not database_insert:
                                cursor.execute("""SELECT oclc FROM oclc where id=%s""", [field_035])
                                row = cursor.fetchone()
                                if row:
                                    oclc_response = ET.fromstring(row[0])
                                    oclc_field = oclc_response.find('./*[@tag="001"]')
                                    oclc_001_value = oclc_field.text

                            # Fetch record from OCLC and optionally update local database.
                            else:

                                oclc_response = self.connector.get_oclc_response(field_035, oclc_developer_key)
                                oclc_field = self.__get_oclc_field(field_001, oclc_response)
                                oclc_001_value = oclc_field.text

                                if oclc_field is not None:
                                    if database_insert:
                                        self.__database_insert(cursor, conn, field_035,
                                                               oclc_field, oclc_response, title)
                                else:
                                    print('Missing oclc 001 for ' + field_035)

                            if oclc_xml_writer is not None:
                                oclc_xml_writer.write(str(ET.tostring(oclc_response,
                                                                      encoding='utf8',
                                                                      method='xml')))

                        # FIELD REPLACEMENTS

                        # Modify records if match title matches with OCLC response.
                        if utils.verify_oclc_response(oclc_response, title, title_log_writer, record.title(),
                                                      current_oclc_number, title_check):

                            # Assure OCLC values are in 001 and 003. Alma load will generate 035.
                            # Do this after title validation.
                            if oclc_001_value:
                                # Update 001 with the value returned in the OCLC API response. This
                                # can vary from the original value in the input records.
                                self.__add_oclc_001_003(record, oclc_001_value)
                                updated_001_count += 1
                                updated_003_count += 1
                            else:
                                raise Exception('Something is wrong, OCLC response missing 001.')

                            # Replace the leader with OCLC leader value.
                            self.__replace_leader(record, oclc_response)
                            updated_leader_count += 1

                            # Remaining field substitutions.
                            if '006' in substitutions:
                                self.__control_field_update(record, '006',
                                                            oclc_response, field_audit_writer)
                            if '007' in substitutions:
                                self.__control_field_update(record, '007',
                                                            oclc_response, field_audit_writer)
                            if '008' in substitutions:
                                self.__control_field_update(record, '008',
                                                            oclc_response, field_audit_writer)
                            if '024' in substitutions:
                                self.__data_field_update(record, '024',
                                                         oclc_response, field_audit_writer)
                            if '035' in substitutions:
                                self.__data_field_update(record, '035',
                                                         oclc_response, field_audit_writer)
                            if '028' in substitutions:
                                self.__data_field_update(record, '028',
                                                         oclc_response, field_audit_writer)
                            if '041' in substitutions:
                                self.__data_field_update(record, '041',
                                                         oclc_response, field_audit_writer)
                            if '043' in substitutions:
                                self.__data_field_update(record, '043',
                                                         oclc_response, field_audit_writer)
                            if '082' in substitutions:
                                self.__data_field_update(record, '082',
                                                         oclc_response, field_audit_writer)
                            if '084' in substitutions:
                                self.__data_field_update(record, '084',
                                                         oclc_response, field_audit_writer)
                            if '100' in substitutions:
                                self.__data_field_update(record, '100',
                                                         oclc_response, field_audit_writer)
                            if '110' in substitutions:
                                self.__data_field_update(record, '110',
                                                         oclc_response, field_audit_writer)
                            if '111' in substitutions:
                                self.__data_field_update(record, '111',
                                                         oclc_response, field_audit_writer)
                            if '130' in substitutions:
                                self.__data_field_update(record, '130',
                                                         oclc_response, field_audit_writer)
                            if '222' in substitutions:
                                self.__data_field_update(record, '222',
                                                         oclc_response, field_audit_writer)
                            if '240' in substitutions:
                                self.__data_field_update(record, '240',
                                                         oclc_response, field_audit_writer)
                            if '245' in substitutions:
                                self.__data_field_update(record, '245',
                                                         oclc_response, field_audit_writer)
                            if '246' in substitutions:
                                self.__data_field_update(record, '246',
                                                         oclc_response, field_audit_writer)
                            if '247' in substitutions:
                                self.__data_field_update(record, '247',
                                                         oclc_response, field_audit_writer)
                            if '250' in substitutions:
                                self.__data_field_update(record, '250',
                                                         oclc_response, field_audit_writer)
                            if '264' in substitutions:
                                self.__data_field_update(record, '264',
                                                         oclc_response, field_audit_writer)
                            if '300' in substitutions:
                                self.__data_field_update(record, '300',
                                                         oclc_response, field_audit_writer)
                            if '337' in substitutions:
                                self.__data_field_update(record, '337',
                                                         oclc_response, field_audit_writer)
                            if '340' in substitutions:
                                self.__data_field_update(record, '340',
                                                         oclc_response, field_audit_writer)
                            if '362' in substitutions:
                                self.__data_field_update(record, '362',
                                                         oclc_response, field_audit_writer)
                            if '386' in substitutions:
                                self.__data_field_update(record, '386',
                                                         oclc_response, field_audit_writer)
                            if '490' in substitutions:
                                self.__data_field_update(record, '490',
                                                         oclc_response, field_audit_writer)
                            if '505' in substitutions:
                                self.__data_field_update(record, '505',
                                                         oclc_response, field_audit_writer)
                            if '510' in substitutions:
                                self.__data_field_update(record, '510',
                                                         oclc_response, field_audit_writer)
                            if '511' in substitutions:
                                self.__data_field_update(record, '511',
                                                         oclc_response, field_audit_writer)
                            if '520' in substitutions:
                                self.__data_field_update(record, '520',
                                                         oclc_response, field_audit_writer)
                            if '521' in substitutions:
                                self.__data_field_update(record, '521',
                                                         oclc_response, field_audit_writer)
                            if '526' in substitutions:
                                self.__data_field_update(record, '526',
                                                         oclc_response, field_audit_writer)
                            if '533' in substitutions:
                                self.__data_field_update(record, '533',
                                                         oclc_response, field_audit_writer)
                            if '538' in substitutions:
                                self.__data_field_update(record, '538',
                                                         oclc_response, field_audit_writer)
                            if '541' in substitutions:
                                self.__data_field_update(record, '541',
                                                         oclc_response, field_audit_writer)
                            if '550' in substitutions:
                                self.__data_field_update(record, '550',
                                                         oclc_response, field_audit_writer)
                            if '600' in substitutions:
                                self.__data_field_update(record, '600',
                                                         oclc_response, field_audit_writer)
                            if '610' in substitutions:
                                self.__data_field_update(record, '610',
                                                         oclc_response, field_audit_writer)
                            if '611' in substitutions:
                                self.__data_field_update(record, '611',
                                                         oclc_response, field_audit_writer)
                            if '630' in substitutions:
                                self.__data_field_update(record, '630',
                                                         oclc_response, field_audit_writer)
                            if '650' in substitutions:
                                self.__data_field_update(record, '650',
                                                         oclc_response, field_audit_writer)
                            if '651' in substitutions:
                                self.__data_field_update(record, '651',
                                                         oclc_response, field_audit_writer)
                            if '655' in substitutions:
                                self.__data_field_update(record, '655',
                                                         oclc_response, field_audit_writer)
                            if '700' in substitutions:
                                self.__data_field_update(record, '700',
                                                         oclc_response, field_audit_writer)
                            if '710' in substitutions:
                                self.__data_field_update(record, '710',
                                                         oclc_response, field_audit_writer)
                            if '730' in substitutions:
                                self.__data_field_update(record, '730',
                                                         oclc_response, field_audit_writer)
                            if '740' in substitutions:
                                self.__data_field_update(record, '740',
                                                         oclc_response, field_audit_writer)
                            if '752' in substitutions:
                                self.__data_field_update(record, '752',
                                                         oclc_response, field_audit_writer)
                            if '760' in substitutions:
                                self.__data_field_update(record, '760',
                                                         oclc_response, field_audit_writer)
                            if '765' in substitutions:
                                self.__data_field_update(record, '765',
                                                         oclc_response, field_audit_writer)
                            if '776' in substitutions:
                                self.__data_field_update(record, '776',
                                                         oclc_response, field_audit_writer)
                            if '780' in substitutions:
                                self.__data_field_update(record, '780',
                                                         oclc_response, field_audit_writer)
                            if '830' in substitutions:
                                self.__data_field_update(record, '830',
                                                         oclc_response, field_audit_writer)
                            if '850' in substitutions:
                                self.__data_field_update(record, '850',
                                                         oclc_response, field_audit_writer)
                        else:
                            # For unmodified records, write to a separate file and continue.
                            unmodified_writer.write(record)
                            unmodified_count += 1
                            continue

                    except HTTPError as err:
                        print(err)
                    except UnicodeEncodeError as err:
                        print(err)
                    modified_count += 1
                    writer.write(record)

                else:
                    bad_record_count += 1
                    print(reader.current_exception)
                    print(reader.current_chunk)
                    bad_writer.write(reader.current_chunk)

        if oclc_xml_writer is not None:
            oclc_xml_writer.write('</collection>')

        if conn is not None:
            conn.close()

        print('Modified record count: ' + str(modified_count))
        print('Unmodified record count: ' + str(unmodified_count))
        print('Bad record count: ' + str(bad_record_count))
        print()

        field_count_dict = field_count.get_field_count()
        field_c = 0
        print('leader: ' + str(updated_leader_count))
        print('001: ' + str(updated_001_count))
        print('003: ' + str(updated_003_count))
        for key in field_count_dict.keys():
            print(key + ': ' + str(field_count_dict[key]))
            field_c += field_count_dict[key]

        print('Total fields replaced: ' + str(field_c))
