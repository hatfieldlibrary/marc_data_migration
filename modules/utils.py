import re
from modules.fuzzy_match import FuzzyMatcher as fuzz

valid_format_regex = re.compile('^\d+$')
ns = {'': 'http://www.loc.gov/MARC21/slim'}


def get_001_value(field_001, field_035):
    """
    Returns value from the 001 field. The value is first checked to
    see if it's an OCLC number.  The criteria are:

        The 001 value begins with ocn, ocm, or on.
        The 003 field contains the OCoLC group identifier

    :param: The 001 pymarc Field
    :param: The 003 pymarc Field
    :return: The 001 value or empty string
    """
    # ohOneFinalValue = ''
    value_001 = field_001.value()
    value_035 = field_035.value()
    if 'ocn' in value_001 or 'ocm' in value_001 or 'om' in value_001:
        final_value_001 = value_001.replace('ocn', '').replace('ocm', '').replace('on', '')
    elif 'OCoLC' in value_035:
        final_value_001 = value_001
    if valid_format_regex.match(final_value_001):
        return final_value_001


def get_035_value(field_035):
    """
    Returns value from the 035 field. Verifies that
    the field contains the (OCoLC) identifier. Removes the
    identifier and returns the OCLC number only.

    :param: The 035a strng value
    :return: The 035 value or None
    """
    if 'OCoLC' in field_035:
        oclc_number = field_035.replace('(OCoLC)', '')
        if valid_format_regex.match(oclc_number):
            return oclc_number

def verify_oclc_response(oclc_response, title):
    """
    Verifies that the 245a value in the OCLC response
    matches the expected value for the current record.
    This uses a fuzzy match to allow for encoding errors
    in the original record. If you need an exact match,
    adjust the fuzzy matching threshold to be 100.

    :param: The OCLC XML respone
    :param: The expected title from 245a
    :return: boolean that indicates the match result
    """
    try:
        data_node = oclc_response.find('.//*[@tag="245"]//subfield[@code="a"]', ns)
        # ignore end-of-field punctuation
        end_of_line_substitution = re.compile('[:|/|;|.]$')
        # remove spaces from comparison
        normalization = re.compile('\s+')
        pnca_title = re.sub(end_of_line_substitution, '', title)
        pnca_title = re.sub(normalization, '', pnca_title)
        node_text = re.sub(end_of_line_substitution, '', data_node.text)
        node_text = re.sub(normalization, '', node_text)
        return fuzz.findMatch(node_text.lower(), pnca_title.lower())
    except:
        print('error')


