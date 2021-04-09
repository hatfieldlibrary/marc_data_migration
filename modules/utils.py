import re
from modules.fuzzy_match import FuzzyMatcher

valid_format_regex = re.compile('^\d+$')
ns = {'': 'http://www.loc.gov/MARC21/slim'}


def get_oclc_001_value(field_001, field_003):
    """
    Returns value from the 001 field. The value is first checked to
    see if it's an OCLC number.  The criteria are:

        The 001 value begins with ocn, ocm, or on.
        The 003 field contains the OCoLC group identifier

    :param: field_001 The 001 pymarc Field
    :param: field_003 The 003 pymarc Field
    :return: The 001 value or empty None of not OCLC record
    """
    final_value_001 = None
    if field_001:
        value_001 = field_001.value()
    if field_003:
        value_003 = field_003.value()
    if value_001 is not None:
        if 'ocn' in value_001 or 'ocm' in value_001 or 'om' in value_001:
            final_value_001 = value_001.replace('ocn', '').replace('ocm', '').replace('on', '')
    elif value_003 is not None:
        if 'OCoLC' in value_003:
            final_value_001 = value_001
    if final_value_001 is not None:
        if valid_format_regex.match(final_value_001):
            return final_value_001

    return final_value_001


def get_oclc_035_value(field_035):
    """
    Returns value from the 035 field. Verifies that
    the field contains the (OCoLC) identifier. Removes the
    identifier and returns the OCLC number only.

    :param: field_035 The 035a strng value
    :return: The 035 value or None if not OCLC record
    """
    oclc_number = None
    if 'OCoLC' in field_035:
        oclc_number = field_035.replace('(OCoLC)', '')
        if valid_format_regex.match(oclc_number):
            return oclc_number

    return oclc_number


def verify_oclc_response(oclc_response, title, title_log_writer):
    """
    Verifies that the 245a value in the OCLC response
    matches the expected value for the current record.
    This uses a fuzzy match to allow for encoding errors
    in the original record. If you need an exact match,
    adjust the fuzzy matching threshold to be 100.

    :param: oclc_response The OCLC XML respone
    :param: title the expected title in 245a
    :return: boolean true for match
    """
    if oclc_response:
        try:
            data_node = oclc_response.find('.//*[@tag="245"]//subfield[@code="a"]', ns)
            if data_node.text:
                # ignore end-of-field punctuation
                end_of_line_substitution = re.compile('\W+$')
                # remove spaces from comparison
                normalization = re.compile('\s+')
                pnca_title = re.sub(end_of_line_substitution, '', title)
                pnca_title = re.sub(normalization, '', pnca_title)
                if data_node is not None:
                    node_text = re.sub(end_of_line_substitution, '', data_node.text)
                    node_text = re.sub(normalization, '', node_text)
                    # do fuzzy match
                    fuzz = FuzzyMatcher()
                    return fuzz.find_match(node_text.lower(), pnca_title.lower(),
                                           title, data_node.text, title_log_writer)
        except Exception as e:
            print(e)
    else:
        return False

