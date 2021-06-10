import datetime
import re

from pymarc import Field

from processors.plugins.pnca.location_mapper import LocationMapper
import processors.utils as utils


class UpdatePolicy:
    """
    The idiosyncratic parts of our PNCA migration to Alma live here.
    """

    dt = datetime.datetime.now()
    mat_type_log_writer = open('output/audit/mat-type-analysis-' + str(dt) + '.txt', 'w')

    # Initialize electronic record counts
    streaming_video_count = 0
    ebook_count = 0
    online_periodical_count = 0

    pnca_id_counter = 100

    ns = {'': 'http://www.loc.gov/MARC21/slim'}

    # These fields will get the $9local subfield that preserves
    # the field when importing to Alma.
    local_fields = ['590',
                    '591',
                    '592',
                    '690',
                    '852',
                    '900',
                    '901',
                    '902',
                    '909',
                    '910',
                    '913',
                    '917',
                    '918',
                    '921',
                    '936',
                    '938',
                    '940',
                    '945',
                    '962',
                    '966',
                    '970',
                    '971',
                    '975',
                    '987',
                    '989',
                    '991',
                    '994',
                    '995',
                    '998',
                    '999']

    def execute(self, record, identifier):
        """
        Executes the record update policy for this plugin.
        :param record: pymarc record
        :param identifier: the OCLC identifier
        :return:
        """
        self.__add_location(record, identifier)
        self.__add_inventory(record)
        self.__add_funds(record)
        self.fix_duplicate_100_field(record)
        self.__add_local_field_note(record)

    @staticmethod
    def conditional_move_tags():
        """
        Implement this method if you need to preserve information by moving
        it to a local field when the field was not replaced by data from
        the OCLC response. Called by the OCLC field replacement task.
        :return: An array of string arrays that indicate the original tag and
        the new local target field.
        """
        # return []
        field1 = ['500', '591']
        field2 = ['505', '590']
        return [field1, field2]

    def is_online(self, record):
        """
        The hook for electronic records in our current
        input data is the 900 field. Called by OCLC replacement
        and modify record tasks.
        :param record: a pymarc record
        :return: True if record is electronic
        """
        field_900 = record.get_fields('900')
        # There can be multiple fields
        for field in field_900:
            subfield = field.get_subfields('a')
            # inspect subfield "a"
            if len(subfield) > 0:
                field_value = subfield[0]
                if field_value:
                    if field_value.find('STREAMING VIDEO') > -1:
                        self.streaming_video_count += 1
                        return True
                    if field_value.find('EBOOK') > -1:
                        self.ebook_count += 1
                        return True
                    if field_value.find('ONLINE PERIODICAL') > -1:
                        self.online_periodical_count += 1
                        return True
        return False

    def fix_duplicate_100_field(self, record):
        """
        Quite a few of the exported PNCA records have
        a 100 field and a 130 field indicating language.
        The language needs to be added to the 100 field
        and the 130 field removed from the record
        :param record: pymarc record
        :return:
        """
        fields100 = record.get_fields('100')
        fields130 = record.get_fields('130')
        if len(fields100) > 0 and len(fields130) > 0:
            subfields = fields130[0].subfields_as_dict()
            if 'l' in subfields.keys():
                subfields100 = fields100[0].subfields_as_dict()
                if 'l' not in subfields100.keys():
                    fields100[0].add_subfield('l', subfields['l'])
                record.remove_fields('130')

    def print_online_record_counts(self):
        """
        Displays the counts of electronic record types. This can
        be called by other tasks to report on the types of records
        identified by is_online().
        :return:
        """
        print('Ebook record count: ' + str(self.ebook_count))
        print('Online periodical record count: ' + str(self.online_periodical_count))
        print('Streaming video record count: ' + str(self.streaming_video_count))
        total_electronic_records = self.streaming_video_count + self.ebook_count + self.online_periodical_count
        print('Total electronic records: ' + str(total_electronic_records))

    def analyze_type(self, record, type):
        """
        Adding this method to PNCA plugin so we can check material types.
        :param record: pymarc record
        :param type: indicates oclc modification status
        :return:
        """
        pnca_call_number = self.__get_call_number(record)
        subfield_300a = self.__get_subfield_300a(record)
        title = record.title()
        if subfield_300a is not None and pnca_call_number is not None:

            if "audio" in subfield_300a.lower() and "cdrom" not in pnca_call_number.lower() and "cd-rom" \
                    not in pnca_call_number.lower():
                if not re.match('^cd\s', pnca_call_number.lower()):
                    self.mat_type_log_writer.write(pnca_call_number + "\t" + subfield_300a + "\t" + type +
                                                   "\t" + title + "\n")

            if "video" in pnca_call_number.lower():
                if "videocassette" not in subfield_300a.lower() and "videorecording" not in subfield_300a.lower():
                    self.mat_type_log_writer.write(pnca_call_number + "\t" + subfield_300a + "\t" + type +
                                                   "\t" + title + "\n")

            if "dvd" in pnca_call_number.lower():
                if "videodisc" not in subfield_300a.lower() and "dvd" not in subfield_300a.lower():
                    self.mat_type_log_writer.write(pnca_call_number + "\t" + subfield_300a + "\t" + type +
                                                   "\t" + title + "\n")

            if "cdrom" in pnca_call_number.lower():
                if "cd-rom" not in subfield_300a.lower() and "cdrom" not in subfield_300a.lower() and "optical" \
                        not in subfield_300a.lower():
                    self.mat_type_log_writer.write(pnca_call_number + "\t" + subfield_300a + "\t" + type +
                                                   "\t" + title + "\n")

            if "cd-rom" in pnca_call_number.lower():
                if "cd-rom" not in subfield_300a.lower() and "cdrom" not in subfield_300a.lower() and "optical" \
                        not in subfield_300a.lower():
                    self.mat_type_log_writer.write(pnca_call_number + "\t" + subfield_300a + "\t" + type +
                                                   "\t" + title + "\n")

    def set_local_id(self, record):
        """
        PNCA 001 and 003 control fields include a lot of
        duplicates. These need to be replaced to the extent possible.
        This function replaces the 003 with a PNCA label and assigns a unique,
        incremented value to 001 whenever conditions are met.
        :param record: pymarc record
        :return:
        """
        to_update = False
        field003arr = record.get_fields('003')
        field001arr = record.get_fields('001')
        if len(field003arr) > 0:
            field003 = field003arr[0].value()
            # always replace these
            if field003 == 'COMPanion' or field003 == 'CStRLIN' or field003 == 'DSS'\
                    or field003 == 'DLC' or field003 == '':
                field003 = 'PNCA'
                to_update = True
                self.pnca_id_counter += 1
            # replace if non-valid OCLC number
            if field003 == 'OCoLC':
                if len(field001arr) > 0:
                    if utils.get_oclc_001_value(field001arr[0], field003arr[0]) is None:
                        field003 = 'PNCA'
                        to_update = True
                        self.pnca_id_counter += 1
                    else:
                        # If it appears to be valid OCLC number,
                        # add the 001 value to a local 592 field.
                        self.__add_592(record, field001arr[0].value())
                        field003 = 'PNCA'
                        to_update = True
                        self.pnca_id_counter += 1
        else:
            # add if record has no 003 field
            field003 = 'PNCA'
            to_update = True
            self.pnca_id_counter += 1
        if to_update:
            record.remove_fields('001')
            record.remove_fields('003')
            new001 = Field(tag='001', data=str(self.pnca_id_counter))
            new003 = Field(tag='003', data=field003)
            record.add_ordered_field(new001)
            record.add_ordered_field(new003)

    @staticmethod
    def __add_592(record, value001):
        target_field = Field(
            tag='592',
            indicators=["", ""],
            subfields=['a', 'Candidate OCLC number: ' + value001]
        )
        record.add_ordered_field(target_field)

    @staticmethod
    def __get_subfield_300a(record):
        subfield_300a = None
        field_test = record.get_fields('300')
        if len(field_test) > 0:
            field_300 = field_test[0]
            subfield_300arr = field_300.get_subfields('a')
            if len(subfield_300arr) > 0:
                subfield_300a = subfield_300arr[0]
        return subfield_300a

    def __add_local_field_note(self, record):
        """
        Alma NZ will preserve local fields that are
        labelled with subfield 9 'local'. The fields
        are listed in the fields array.
        :param record: pymarc record
        :return:
        """
        for field in self.local_fields:
            for rec_field in record.get_fields(field):
                rec_field.add_subfield('9', 'local')

    @staticmethod
    def __add_inventory(record):
        """
        Copy the inventory note to 852(i)
        :param record: pymarc record
        :return:
        """
        fields = record.get_fields('852')
        for field in fields:
            subs = field.subfields_as_dict()
            sub = subs["1"]
            for s in sub:
                arr = s.split('|')
                for item in arr:
                    if re.match('^Inventory', item):
                        field.add_subfield('i', item)

    @staticmethod
    def __add_funds(record):
        """
        Copy the funds note to 852(f)
        """
        fields = record.get_fields('852')
        for field in fields:
            subs = field.subfields_as_dict()
            sub = subs['1']
            for s in sub:
                arr = s.split('|')
                for item in arr:
                    if re.match('^Fund', item):
                        field.add_subfield('f', 'PNCA ' + item)

    def __add_location(self, record, oclc_number):
        """
        Add a location based on call number or 852(b)
        :param record:
        :param oclc_number:
        :return:
        """
        # No location mapping for online records
        if self.is_online(record):
            return

        location_mapper = LocationMapper()
        # This is a hack for locations that can't be determined by
        # using the PNCA call number.
        location_field = self.__get_852b(record)
        if location_field == '1st Floor CDs' or location_field == 'OVERSIZE PERIODICALS':
            try:
                location = location_mapper.get_location(location_field)
                self.__replace_location(record, location)
            except Exception as err:
                print('error replacing location field.')
                print(err)

        else:
            call_number = self.__get_call_number(record)
            if not call_number:
                if oclc_number:
                    print('Missing call number for: ' + oclc_number)
                else:
                    print('Missing call number for: ' + record.title())
            else:
                try:
                    location = location_mapper.get_location_by_callnumber(call_number)
                    if location:
                        self.__replace_location(record, location)
                except Exception as err:
                    print('error adding location field.')
                    print(err)

    @staticmethod
    def __get_852b(record):
        """
        Returns value of 852(b) if available.
        :param record: pymarc record
        :return:
        """
        location_field = None
        if len(record.get_fields('852')) > 0:
            fields = record.get_fields('852')
            for field in fields:
                subfields = field.get_subfields('b')
                if len(subfields) == 1:
                    location_field = subfields[0]
        return location_field

    @staticmethod
    def __get_call_number(record):
        """
        Returns call number found in 852(h)
        :param record: record node
        :return: call number
        """
        call_number = None
        if len(record.get_fields('852')) > 0:
            fields = record.get_fields('852')
            for field in fields:
                subfields = field.get_subfields('h')
                if len(subfields) == 1:
                    call_number = subfields[0]
        return call_number

    @staticmethod
    def __replace_location(record, location):
        """
        Replaces the current value of 852(b)
        :param record: pymarc record
        :param location: location
        :return:
        """
        if len(record.get_fields('852')) > 0:
            try:
                fields = record.get_fields('852')
                for field in fields:
                    field.delete_subfield('b')
                    field.add_subfield('b', location, 1)
            except Exception as err:
                print('Error replacing location in record.')
                print(err)

    @staticmethod
    def __add_location_to_record(record, location):
        """
        Adds 852(b) to the record. It turns out that this
        is dangerous since a few PNCA records already have
        an 852(b) and leaving it in the record will trip up
        the Alma import.
        :param record: pymarc record
        :param location: location code
        :return:
        """
        if len(record.get_fields('852')) > 0:
            try:
                fields = record.get_fields('852')
                for field in fields:
                    field.add_subfield('b', location, 1)
            except Exception as err:
                print('Error adding location to record.')
                print(err)
