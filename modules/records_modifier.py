from urllib.error import HTTPError
from pymarc import MARCReader, Field
from modules.field_generators import ControlFieldGenerator, DataFieldGenerator
from modules.oclc_connector import OclcConnector
import modules.utils as utils


class RecordsModifier:

    connector = OclcConnector()

    @staticmethod
    def __data_field_update(record, replacement_field_tag, oclc_response):
        """
        Updates the record data fields using OCLC XML data.
        :param record: the pymarc Record
        :param replacement_field_tag: the field to replace
        :param oclc_response: the OCLC XML response
        """
        field_generator = DataFieldGenerator()
        ns = {'': 'http://www.loc.gov/MARC21/slim'}
        tags = oclc_response.findall('.//*[@tag="' + replacement_field_tag + '"]', ns)
        if tags:
            record.remove_fields(replacement_field_tag)
            for f in tags:
                field = field_generator.get_data_field(f, f.attrib, replacement_field_tag)
                if field:
                    record.add_ordered_field(field)

    @staticmethod
    def __control_field_update(record, replacement_field, oclc_response):
        """
        Updates the record control fields using OCLC XML data.
        :param record: the pymarc Record
        :param replacement_field: the field to replace
        :param oclc_response: the OCLC XML response
        """
        field_generator = ControlFieldGenerator()
        field = field_generator.get_control_field(replacement_field, oclc_response)
        if field:
            record.remove_fields(replacement_field)
            record.add_ordered_field(field)

    @staticmethod
    def __add_oclc_001_003(record, oclc_number):
        """
        Use this method to add 001 and 003 fields to records
        that are missing this information.
        :param record:  pymarc record
        :param oclc_number: OCLC number
        """
        record.remove_fields('001')
        record.remove_fields('003')
        field_001 = Field(
            tag='001',
            data=oclc_number
        )
        field_003 =  Field(
            tag='003',
            data='OCoLC'
        )
        record.add_ordered_field(field_001)
        record.add_ordered_field(field_003)



    def update_fields_using_oclc(self, file, substitutions, writer, unmodified_writer, bad_writer, oclc_developer_key):
        """
        Updates records from input marc file with data obtained
        from OCLC worldcat.  The method takes a substitutions array
        that specifies the fields to be updated. Possible values are
        008, 007, 245.
        :param file: The marc file (binary)
        :param substitutions: The array of fields to update
        :param writer: The output file writer
        :param unmodified_writer: The output file writer for unmodifed records
        :param bad_writer: The output file records that cannot be processed
        :param oclc_developer_key: the developer key used to query OCLC
        """
        with open(file, 'rb') as fh:
            modified_count = 0
            unmodified_count = 0
            bad_record_count = 0
            # Set to permissive to avoid exiting loop; report
            # unreadable records in the output. Prevent python utf-8
            # handling.
            reader = MARCReader(fh, permissive=True, utf8_handling='ignore')
            for record in reader:
                if record:
                    oh_one_value = None
                    oclc_number = None
                    title = ''
                    try:
                        if record['245'] and record['245']['a']:
                            title = record['245']['a']
                        # Get values for 001 value, 035 value, and title.
                        # These are used to request and verify data from OCLC.
                        if len(record.get_fields('001')) == 1:
                            oh_one_value = utils.get_oclc_001_value(record['001'], record['003'])

                        if record['035'] and record['035']['a']:
                            oclc_number = utils.get_oclc_035_value(record['035']['a'])
                    except:
                        print('error reading fields from input record: ' + title)

                    try:
                        oclc_response = None
                        # Use 001 by default. Try 035 if the 001 is not available.
                        if oh_one_value:
                            oclc_response = self.connector.get_oclc_response(oh_one_value, oclc_developer_key)
                        elif oclc_number:
                            oclc_response = self.connector.get_oclc_response(oclc_number, oclc_developer_key)
                            # For loading assure OCLC values in 001 and 003. Alma load will generate 035.
                            self.__add_oclc_001_003(record, oclc_number)

                        # Modify records if match with OCLC response.
                        if utils.verify_oclc_response(oclc_response, title):
                            if '006' in substitutions:
                                self.__control_field_update(record, '006', oclc_response)
                            if '007' in substitutions:
                                self.__control_field_update(record, '007', oclc_response)
                            if '008' in substitutions:
                                self.__control_field_update(record, '008', oclc_response)
                            if '024' in substitutions:
                                self.__data_field_update(record, '024', oclc_response)
                            if '035' in substitutions:
                                self.__data_field_update(record, '035', oclc_response)
                            if '028' in substitutions:
                                self.__data_field_update(record, '028', oclc_response)
                            if '041' in substitutions:
                                self.__data_field_update(record, '041', oclc_response)
                            if '043' in substitutions:
                                self.__data_field_update(record, '043', oclc_response)
                            if '082' in substitutions:
                                self.__data_field_update(record, '082', oclc_response)
                            if '084' in substitutions:
                                self.__data_field_update(record, '084', oclc_response)
                            if '100' in substitutions:
                                self.__data_field_update(record, '100', oclc_response)
                            if '110' in substitutions:
                                self.__data_field_update(record, '110', oclc_response)
                            if '111' in substitutions:
                                self.__data_field_update(record, '111', oclc_response)
                            if '130' in substitutions:
                                self.__data_field_update(record, '130', oclc_response)
                            if '222' in substitutions:
                                self.__data_field_update(record, '222', oclc_response)
                            if '240' in substitutions:
                                self.__data_field_update(record, '240', oclc_response)
                            if '245' in substitutions:
                                self.__data_field_update(record, '245', oclc_response)
                            if '246' in substitutions:
                                self.__data_field_update(record, '246', oclc_response)
                            if '247' in substitutions:
                                self.__data_field_update(record, '247', oclc_response)
                            if '250' in substitutions:
                                self.__data_field_update(record, '250', oclc_response)
                            if '264' in substitutions:
                                self.__data_field_update(record, '264', oclc_response)
                            if '300' in substitutions:
                                self.__data_field_update(record, '300', oclc_response)
                            if '337' in substitutions:
                                self.__data_field_update(record, '337', oclc_response)
                            if '340' in substitutions:
                                self.__data_field_update(record, '340', oclc_response)
                            if '362' in substitutions:
                                self.__data_field_update(record, '362', oclc_response)
                            if '386' in substitutions:
                                self.__data_field_update(record, '386', oclc_response)
                            if '490' in substitutions:
                                self.__data_field_update(record, '490', oclc_response)
                            if '505' in substitutions:
                                self.__data_field_update(record, '505', oclc_response)
                            if '510' in substitutions:
                                self.__data_field_update(record, '510', oclc_response)
                            if '511' in substitutions:
                                self.__data_field_update(record, '511', oclc_response)
                            if '520' in substitutions:
                                self.__data_field_update(record, '520', oclc_response)
                            if '521' in substitutions:
                                self.__data_field_update(record, '521', oclc_response)
                            if '526' in substitutions:
                                self.__data_field_update(record, '526', oclc_response)
                            if '533' in substitutions:
                                self.__data_field_update(record, '533', oclc_response)
                            if '538' in substitutions:
                                self.__data_field_update(record, '538', oclc_response)
                            if '541' in substitutions:
                                self.__data_field_update(record, '541', oclc_response)
                            if '550' in substitutions:
                                self.__data_field_update(record, '550', oclc_response)
                            if '600' in substitutions:
                                self.__data_field_update(record, '600', oclc_response)
                            if '610' in substitutions:
                                self.__data_field_update(record, '610', oclc_response)
                            if '611' in substitutions:
                                self.__data_field_update(record, '611', oclc_response)
                            if '630' in substitutions:
                                self.__data_field_update(record, '630', oclc_response)
                            if '650' in substitutions:
                                self.__data_field_update(record, '650', oclc_response)
                            if '651' in substitutions:
                                self.__data_field_update(record, '651', oclc_response)
                            if '655' in substitutions:
                                self.__data_field_update(record, '655', oclc_response)
                            if '700' in substitutions:
                                self.__data_field_update(record, '700', oclc_response)
                            if '710' in substitutions:
                                self.__data_field_update(record, '710', oclc_response)
                            if '730' in substitutions:
                                self.__data_field_update(record, '730', oclc_response)
                            if '740' in substitutions:
                                self.__data_field_update(record, '740', oclc_response)
                            if '752' in substitutions:
                                self.__data_field_update(record, '752', oclc_response)
                            if '760' in substitutions:
                                self.__data_field_update(record, '760', oclc_response)
                            if '765' in substitutions:
                                self.__data_field_update(record, '765', oclc_response)
                            if '776' in substitutions:
                                self.__data_field_update(record, '776', oclc_response)
                            if '780' in substitutions:
                                self.__data_field_update(record, '780', oclc_response)
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

            print('Modified record count: ' + str(modified_count))
            print('Unmodified record count: ' + str(unmodified_count))
            print('Bad record count: ' + str(bad_record_count))

