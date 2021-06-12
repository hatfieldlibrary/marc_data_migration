from importlib import import_module

from processors import utils
from processors.read_marc import MarcReader


class RecordModifier:

    update_policy = None

    def record_modify(self, file, plugin, writer, online_writer):

        field_001 = None
        field_035 = None
        input_oclc_number = None

        if plugin:
            klass = getattr(import_module(plugin), 'UpdatePolicy')
            self.update_policy = klass()

            reader = MarcReader()

            for record in reader.get_reader(file):
                if record:
                    try:
                        if not record.title():
                            print('Record missing 245(a)')
                        if len(record.get_fields('001')) == 1:
                            field_001 = utils.get_oclc_001_value(record['001'], record['003'])
                        if len(record.get_fields('035')) > 0:
                            field_035 = self.__get_035_value(record)

                    except Exception as err:
                        print('error reading fields from input record.')
                        print(err)

                    if field_001:
                        input_oclc_number = field_001
                    elif field_035:
                        input_oclc_number = field_035

                    # Execute the project-specific update policy.
                    if self.update_policy:
                        self.update_policy.execute(record, input_oclc_number)
                        is_online = self.update_policy.is_online(record)

                        if is_online:
                            online_writer.write(record)

                    writer.write(record)

            reader.close()

    @staticmethod
    def __get_035_value(record):
        """
        Returns value of OCLC 035 field. S
        :param record: record node
        :return: 035 value
        """
        field_035 = None
        if len(record.get_fields('035')) > 0:
            fields = record.get_fields('035')
            for field in fields:
                subfields = field.get_subfields('a')
                if len(subfields) == 1:
                    field_035 = utils.get_oclc_035_value(subfields[0])
        return field_035

