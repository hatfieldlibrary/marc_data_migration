from pymarc import Field


class TitleGenerator:

    ns = {'': 'http://www.loc.gov/MARC21/slim'}

    def getTitleField(self, oclcResponse):
        """
        Extracts title fields from the OCLC XML
        response and creates a new pymarc Field
        from the data.

        :param: The OCLC XML respone
        :return: New 245 Field
        """
        tag = oclcResponse.find('.//*[@tag="245"]')
        fielda = tag.find('.//subfield[@code="a"]', self.ns)
        fieldb = tag.find('.//subfield[@code="b"]', self.ns)
        fieldc = tag.find('./subfield[@code="c"]', self.ns)
        firstIndicator = tag.attrib['ind1']
        secondIndicaor = tag.attrib['ind2']
        subfields = []
        if fielda.text:
            subfields.append('a')
            subfields.append(fielda.text)
        if fieldb and fieldb.text:
            subfields.append('b')
            subfields.append(fieldb.text)
        if fieldc and fieldc.text:
            subfields.append('c')
            subfields.append(fieldc.text)
        indicators = [firstIndicator, secondIndicaor]

        return Field(
            tag='245',
            indicators=indicators,
            subfields=subfields
        )
