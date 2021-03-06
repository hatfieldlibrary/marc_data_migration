from pymarc import Field


class DataFieldGenerator:

    ns = {'': 'http://www.loc.gov/MARC21/slim'}

    def create_data_field(self, tag, indicators, subfield, value):

        return Field(
            tag=tag,
            indicators=indicators,
            subfields=[subfield, value]
        )

    def get_data_field(self, tag, attrib, field):
        """
        Extracts data field from the OCLC XML
        response and creates a new pymarc Field.

        :param: tag the field element
        :param: attrib the field attributes
        :param: field the field number
        :return: new data field
        """
        new_fields = []
        first_indicator = attrib['ind1']
        second_indicator = attrib['ind2']
        subfields = tag.findall('.//', self.ns)
        for subfield in subfields:
            new_fields.append(subfield.attrib['code'])
            new_fields.append(subfield.text)
        indicators = [first_indicator, second_indicator]
        return Field(
            tag=field,
            indicators=indicators,
            subfields=new_fields
        )


class ControlFieldGenerator:

    ns = {'': 'http://www.loc.gov/MARC21/slim'}

    def get_control_field(self, field, oclc_response):

        tag = oclc_response.find('.//*[@tag="' + field + '"]')
        if tag is not None:
            return Field(
                tag=field,
                data=tag.text
            )

