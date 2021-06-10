
class UpdatePolicy:

    def execute(self, record, identifier):
        """
        Executes the record updates for this plugin.
        See PCNA policy for example.

        :param record: pymarc record
        :param identifier: the OCLC identifier
        :return:
        """
        pass

    @staticmethod
    def conditional_move_tags():
        """
        Implement this method if you need to preserve information by moving
        it to a local field when the information was not replaced by data in
        the OCLC response. See PCNA policy for example.
        :return: An array of string arrays that contain the original tag and
        the new target field.
        """
        # return []
        pass

    def is_online(self, record):
        """
        Implement if you need a test for electronic records.
        See PCNA policy for example.
        :param record: a pymarc record
        :return: True if record is electronic
        """
        pass

    def print_online_record_counts(self):
        """
        Displays the counts of electronic record types. This can
        be called by other tasks to report on the types of records
        identified by is_online(). See PCNA policy for example.
        :return:
        """
        pass

    def set_local_id(self, record):
        """
        Sets 001 and 003 fields to use a unique local identifier
        and label.
        :param record: pymarc record
        :return:
        """
        pass

    # Implement private methods here.
