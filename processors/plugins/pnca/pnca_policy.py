import datetime
import re

from pymarc import Field

from processors.plugins.pnca.location_mapper import LocationMapper
import processors.utils as utils


class UpdatePolicy:
    """
    The idiosyncratic parts of our PNCA migration to Alma live here.
    """

    location_mapper = LocationMapper()

    dt = datetime.datetime.now()
    mat_type_log_writer = open('output/audit/mat-type-analysis-' + str(dt) + '.txt', 'w')

    # Initialize electronic record counts
    streaming_video_count = 0
    ebook_count = 0
    online_periodical_count = 0

    pnca_id_counter = 100

    ns = {'': 'http://www.loc.gov/MARC21/slim'}

    # These fields get the $9local subfield that preserves
    # the field when importing to Alma.
    local_fields = ['590',
                    '591',
                    '592',
                    '690',
                    '852',
                    '900']

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
        self.__set_item_policy(record)
        self.__fix_duplicate_100_field(record)
        self.__add_local_field_note(record)
        self.__remove_035(record)
        self.__remove_9xx_fields(record)

    @staticmethod
    def conditional_move_tags():
        """
        Implement this method if you need to preserve information by moving
        it to a local field if the field was not replaced by data from
        the OCLC response. Called by the OCLC field replacement task.
        :return: An array of string arrays that indicate the original tag and
        the new local target field.

        For PNCA, we want to preserve information in 500 and 505 by moving
        to local fields 590 and 591.
        """
        # return []
        field1 = ['500', '591']
        field2 = ['505', '590']
        return [field1, field2]

    def is_online(self, record):
        """
        Identifies records that describe an electronic resource.
        For PNCA, we use the 900 field to identify electronic
        resources.
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

    @staticmethod
    def __fix_duplicate_100_field(record):
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
        For console output. Displays the counts of electronic record types. This can
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
        Creates an audit file. The method compares the call number
        with location information in the 300 field. Use this method
        to flag possible inconsistencies so they can be reviewed and
        fixed before the records are loaded.
        :param record: pymarc record
        :param type: indicates oclc modification status
        :return:
        """
        pnca_call_number = self.__get_call_number_for_logging(record)
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
        duplicates that need to be replaced in the file of records
        that don't receive an OCLC update.
        This method replaces the 003 with a 'PNCA' label and assigns a unique,
        incremented value to 001. It also checks records
        with 003=OCoLC and adds a 592 local field if the OCLC number
        looks valid and worth reviewing later.
        :param record: pymarc record
        :return:
        """
        to_update = False
        field003arr = record.get_fields('003')
        field001arr = record.get_fields('001')
        if len(field003arr) > 0:
            field003 = field003arr[0].value()
            if field003 == 'OCoLC':
                if len(field001arr) > 0:
                    if utils.get_oclc_001_value(field001arr[0], field003arr[0]) is None:
                        # Not a valid OCLC number format. Just replace.
                        field003 = 'PNCA'
                        to_update = True
                        self.pnca_id_counter += 1
                    else:
                        # If it appears to be valid OCLC number,
                        # add the 001 value to a local 592 field.
                        # This can be used for review later.
                        self.__add_592(record, field001arr[0].value())
                        field003 = 'PNCA'
                        to_update = True
                        self.pnca_id_counter += 1
            else:
                # Just replace the control fields for all other 003 values.
                field003 = 'PNCA'
                to_update = True
                self.pnca_id_counter += 1
        else:
            # If the record has no 003 field, add one.
            field003 = 'PNCA'
            to_update = True
            self.pnca_id_counter += 1
        if to_update:
            # Now update fields.
            record.remove_fields('001')
            record.remove_fields('003')
            new001 = Field(tag='001', data=str(self.pnca_id_counter))
            new003 = Field(tag='003', data=field003)
            record.add_ordered_field(new001)
            record.add_ordered_field(new003)

    @staticmethod
    def __remove_035(record):
        """
        Before Alma loading, we are removing all 035s in the record.
        This is an important cleanup step so it's defined here in it's
        own function.
        :param record: pymarc record
        :return:
        """
        record.remove_fields('035')

    @staticmethod
    def __add_592(record, value001):
        """
        This add a local field for OCLC numbers that have been excluded
        and removed from the record because we believe they are inaccurate.
        Records can later be retrieved in Alma using this field and reviewed.
        :param record: pymarc record
        :param value001:
        :return:
        """
        target_field = Field(
            tag='592',
            indicators=["", ""],
            subfields=['a', 'Candidate OCLC number: ' + value001]
        )
        record.add_ordered_field(target_field)

    @staticmethod
    def __get_subfield_300a(record):
        """
        Returns the 300$a value. Assumes a single 300 field.
        :param record: pymarc record
        :return:
        """
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
        Copy the inventory note to 852$y
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
                        match = re.match('^Inventory:(\d{2,2})\/(\d{2,2})\/(\d{4,5})', item)
                        field_value = "%s%s%s" %(match.group(3), match.group(1), match.group(2))
                        field.add_subfield('x', field_value)

    @staticmethod
    def __add_funds(record):
        """
        Copy the funds note to 852$w
        """
        fields = record.get_fields('852')
        for field in fields:
            subs = field.subfields_as_dict()
            sub = subs['1']
            for s in sub:
                arr = s.split('|')
                for item in arr:
                    if re.match('^Fund', item):
                        field.add_subfield('w', 'PNCA ' + item)

    def __add_location(self, record, oclc_number):
        """
        Add a location based on call number or 852$b
        :param record:
        :param oclc_number:
        :return:
        """
        # No location mapping for online records
        if self.is_online(record):
            return

        fields = record.get_fields('852')
        for field in fields:
            location_fields = field.get_subfields('b')
            # This should be a single subfield.
            # Seems important enough to throw an exception.
            # If we hit a bump, print to console
            # to check for multiple errors in the set.
            if len(location_fields) > 1:
                raise Exception("Multiple location subfields in " + record.title())
            # If 852 field contains a location field, we use it to set the
            # location subfield in some cases. More commonly, we use the
            # call number prefix to determine the location code.
            if len(location_fields) > 0:
                for location_field in location_fields:
                    if location_field == '1st Floor CDs' or location_field == 'OVERSIZE PERIODICALS':
                        try:
                            location = self.location_mapper.get_location(location_field)
                            self.__replace_location_subfield(field, location)
                        except Exception as err:
                            print('error replacing location field.')
                            print(err)

                    else:
                        self.__set_location_using_call_number(record, field, oclc_number)
            else:
                # This handles cases when no location subfield was found.
                self.__set_location_using_call_number(record, field, oclc_number)

    def __set_location_using_call_number(self, record, field, oclc_number):
        """
        Adds a location field to the 852 based on the call number prefix.
        :param record: pymarc record
        :param field: pymarc field
        :param oclc_number:
        :return:
        """
        call_numbers = field.get_subfields('h')
        # Logically, this must be a single subfield.
        # This seems important enough to throw an exception.
        if len(call_numbers) > 1:
            raise Exception("Multiple call number subfields in " + record.title())
        for call_number in call_numbers:
            if not call_number:
                if oclc_number:
                    print('Missing call number for: ' + oclc_number)
                else:
                    print('Missing call number for: ' + record.title())
            else:
                try:
                    location = self.location_mapper.get_location_by_callnumber(call_number)
                    if location:
                        self.__replace_location_subfield(field, location)
                except Exception as err:
                    print('error adding location field.')
                    print(err)

    @staticmethod
    def __replace_location_subfield(field, location):
        """
        Sets the current value of 852$b after removing the
        subfield if it already exists in the field.
        :param field: pymarc field
        :param location: location
        :return:
        """
        try:
            field.delete_subfield('b')
            field.add_subfield('b', location, 1)
        except Exception as err:
            print('Error replacing location in record.')
            print(err)

    @staticmethod
    def __modify_call_number(field, call_number):
        """
        This could be used to remove PNCA prefixes from
        852$h. Add more logic to avoid non-LC call numbers that
        require the prefix.

        IMPORTANT: We decided NOT to do this because we need the prefixes
        to split records into sets for loading.  (Prefixes
        on LC records can be removed before the sets are
        loaded into Alma, or using Alma normalization afterwards.)

        :param field:
        :param call_number:
        :return:
        """
        try:
            modified_call = re.sub("^(over|periodical|thesis|games|archive|spec|dvd|zine|new)", '', call_number,
                                   flags=re.I)
            field.delete_subfield('h')
            field.add_subfield('h', modified_call)

        except Exception as err:
            print(err)

    @staticmethod
    def __get_call_number_for_logging(record):
        """
        Returns call number found in 852$h.
        Convenience method for location analysis logging.
        :param record: record node
        :return: call number
        """
        call_number = None
        if len(record.get_fields('852')) > 0:
            fields = record.get_fields('852')
            for field in fields:
                subfields = field.get_subfields('h')
                if len(subfields) == 1:
                    # Assuming single subfield. This should be
                    # the case, and this method is only used
                    # for logging.
                    call_number = subfields[0]
        return call_number

    @staticmethod
    def __add_location_to_record(record, location):
        """
        Adds 852$b to the record.

        NOT USED. It turns out that this
        is dangerous since a few PNCA records already have
        an 852$b. Leaving it in the record will trip up
        the Alma import. Use __replace_location().
        :param record: pymarc record
        :param location: location code
        :return:
        """
        fields = record.get_fields('852')
        try:
            for field in fields:
                field.add_subfield('b', location, 1)
        except Exception as err:
            print('Error adding location to record.')
            print(err)

    @staticmethod
    def __remove_9xx_fields(record):
        """
        Most 9xx fields in PNCA records are irrelevant
        and should be removed. Also removes 900 fields
        that don't have a subfield a.
        :param record: pymarc record
        :return:
        """
        record.remove_fields('902', '909', '910', '913', '917', '918', '921', '930', '936', '938', '940',
                             '962', '970', '971', '987', '989', '991', '994', '995', '998', '999')
        list_900_fields = record.get_fields('900')
        for field in list_900_fields:
            subs = field.get_subfields('a')
            if len(subs) == 0:
                # field is empty so remove it
                record.remove_field(field)

    @staticmethod
    def __set_item_policy(record):
        """
        We need to pull the 'library use only' policy from the 852
        Alexandria subfield and add a new 852 subfield containing the
        Alma item policy code.
        :param record: pymarc record
        :return:
        """
        fields = record.get_fields('852')
        for field in fields:
            subs = field.subfields_as_dict()
            sub = subs['1']
            for s in sub:
                arr = s.split('|')
                for item in arr:
                    if re.match('^Policy:LIB', item):
                        field.add_subfield('q', 'PNOCIRC')
