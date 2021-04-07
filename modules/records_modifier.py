from urllib.error import HTTPError
from pymarc import MARCReader
from modules.field_generators import TitleGenerator
from modules.oclc_connector import OclcConnector
import modules.utils as utils


class RecordsModifier:

    connector = OclcConnector()

    @staticmethod
    def __title_field_update(record, oclc_response):
        """
        Updates the record 245 field with OCLC data.
        :param record: the pymarc Record
        :param oclc_response: the OCLC XML response
        """
        record.remove_fields('245')
        field_generator = TitleGenerator()
        record.add_ordered_field(field_generator.getTitleField(oclc_response))

    def update_fields(self, file, substitutions, writer, oclc_developer_key):
        """
        Updates records from input marc file with data obtained
        from OCLC worldcat.  The method takes a substitutions array
        that specifies the fields to be updated. Possible values are
        008, 007, 245.
        :param file: The marc file (binary)
        :param substitutions: The array of fields to update
        :param oclc_developer_key: the developer key used to query OCLC
        :param writer: The output file writer
        """
        with open(file, 'rb') as fh:
            counter1 = 0
            # Set to permissive to avoid exiting loop; report
            # unreadable records in the output. Prevent python utf-8
            # handling.
            reader = MARCReader(fh, permissive=True, utf8_handling='ignore')
            for record in reader:
                if record:
                    oh_one_value = None
                    oclc_number = None
                    added = False
                    title = ''
                    try:
                        # Get values for 001 value, 035 value, and title.
                        # These are used to request and verify data from OCLC.
                        if len(record.get_fields('001')) == 1:
                            oh_one_value = utils.get_001_value(record['001'], record['003'])

                        if record['245'] and record['245']['a']:
                            title = record['245']['a']

                        if record['035'] and record['035']['a']:
                            oclc_number = utils.get_035_value(record['035']['a'])
                    except:
                        'error reading fields from input record'

                    try:
                        # Use 001 by default. Try 035 if the 001 is not available.
                        if oh_one_value:
                            oclc_response = self.connector.getOclcResponse(oh_one_value, oclc_developer_key)
                            if utils.verify_oclc_response(oclc_response, title):
                                if '245' in substitutions:
                                    self.__title_field_update(record, oclc_response)
                        elif oclc_number:
                            oclc_response = self.connector.getOclcResponse(oclc_number, oclc_developer_key)
                            if utils.verify_oclc_response(oclc_response, title):
                                if '245' in substitutions:
                                    self.__title_field_update(record, oclc_response)

                    except HTTPError as err:
                        print(err)
                    except UnicodeEncodeError as err:
                        print(err)

                    writer.write(record)

                else:
                    counter1 += 1
                    print(reader.current_exception)
                    print(reader.current_chunk)

            print('Error count: ' + str(counter1))

