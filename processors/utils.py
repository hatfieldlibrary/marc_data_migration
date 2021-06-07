import re
from processors.oclc_update.fuzzy_match import FuzzyMatcher

valid_format_regex = re.compile('^\d+$')

ns = {'': 'http://www.loc.gov/MARC21/slim'}

fuzz = FuzzyMatcher()


def get_original_title(record):
    title = ''
    field_245 = record.get_fields('245')
    if field_245 is not None:
        for field in field_245:
            subfields = field.subfields_as_dict()
            for key in subfields:
                if key == 'a' or key == 'b':
                    title += ' ' + subfields[key][0]

    return title


def remove_control_chars(field):
    # Odd thing that is in at least one record. This obviously should never happen.
    # control_character_replacement = re.compile('\s+\d+$')
    # field = re.sub(control_character_replacement, '', field)
    return field


def get_oclc_001_value(field_001, field_003):
    """
    Returns value from the 001 field if it's an OCLC number.
    The criteria for determining OCLC identifiers are:

        The 001 value begins with 'ocn', 'ocm', or 'on'
        OR the 003 field contains the OCoLC group identifier
        AND the value is a valid OCLC number format.

    :param: field_001 The 001 pymarc Field
    :param: field_003 The 003 pymarc Field
    :return: The 001 value or empty None of not OCLC record
    """
    final_value_001 = None
    value_001 = None
    value_003 = None
    if field_001:
        value_001 = field_001.value()
    if field_003:
        value_003 = field_003.value()

    if value_001:
        # set 001 if oclc prefix is prepended
        if 'ocn' in value_001 or 'ocm' in value_001 or 'on' in value_001:
            final_value_001 = value_001.replace('ocn', '').replace('ocm', '').replace('on', '')
        elif value_003:
            # if no oclc prefix, check 003 for oclc label
            if 'OCoLC' in value_003:
                final_value_001 = value_001

    if final_value_001 is not None:
        # verify that we have a valid oclc number format.
        if valid_format_regex.match(final_value_001):
            final_value_001 = remove_control_chars(final_value_001)
            return final_value_001

    return None


def get_035(record):
    field_035 = None
    if len(record.get_fields('035')) > 0:
        fields = record.get_fields('035')
        for field in fields:
            subfields = field.get_subfields('a')
            if len(subfields) > 1:
                print('duplicate 035a')
            elif len(subfields) == 1:
                field_035 = __get_oclc_035_value(subfields[0])
    return field_035


def __get_oclc_035_value(field_035):
    """
    Returns value from the 035 field. Verifies that
    the field contains the (OCoLC) identifier. Removes the
    identifier and returns the OCLC number only.

    :param: field_035 The 035a string value
    :return: The 035 value or None if not OCLC record
    """
    if field_035:
        if 'OCoLC' in field_035:
            try:
                oclc_number = field_035.replace('(OCoLC)', '')
                match = valid_format_regex.match(oclc_number)
                if match:
                    oclc_number = remove_control_chars(oclc_number)
                    return oclc_number
            except Exception as err:
                print(err)
    return None


def get_oclc_node(oclc_response):
    try:
        data_node = oclc_response.find('./*[@tag="245"]/*[@code="a"]', ns)
        data_node2 = oclc_response.find('./*[@tag="245"]/*[@code="b"]', ns)
        data_node3 = oclc_response.find('./*[@tag="245"]/*[@code="c"]', ns)
        data_node4 = oclc_response.find('./*[@tag="245"]/*[@code="n"]', ns)
        data_node5 = oclc_response.find('./*[@tag="245"]/*[@code="p"]', ns)
        full_oclc_title = ''
        oclc_comparison_value = ''
        if data_node.text:
            if data_node is not None:
                full_oclc_title += data_node.text
                oclc_comparison_value += data_node.text
            if data_node2 is not None:
                full_oclc_title += data_node2.text
                oclc_comparison_value += data_node2.text
            if data_node3 is not None:
                full_oclc_title += data_node3.text
            if data_node4 is not None:
                full_oclc_title += '(n: ' + data_node4.text + ')'
            if data_node5 is not None:
                full_oclc_title += '(p: ' + data_node5.text + ')'
            # full_oclc_title for audit log and oclc_comparison_value for match.
            return [full_oclc_title, oclc_comparison_value]

    except Exception as e:
        print('Unable to read oclc response 245 field.')
        print(e)

    return None


def normalize_title(title):
    # remove non-characters
    end_of_line_substitution = re.compile('[\W|\\t]+')
    # remove spaces
    start_of_line_substitution = re.compile('\s+')
    title = re.sub(end_of_line_substitution, '', title)
    title = re.sub(start_of_line_substitution, '', title)
    return title


def verify_oclc_response(oclc_response, title, title_log_writer, input_title, current_oclc_number, title_check,
                         require_perfect_match):
    """
    Verifies that the 245 subfield a and b values in the OCLC response
    match the expected value in the current record. This uses a fuzzy
    match based on Levenshtein distance, originally to
    account for diacritics issues. If you need an
    exact match, adjust the matching threshold to be 100.

    :param oclc_response: The XML root Element
    :param title: The record title 245(a),(b)
    :param title_log_writer: fuzzy match audit file
    :param input_title: The input file title 245(a),(b)
    :param current_oclc_number: the number used in the current OCLC lookup
    :param title_check: if True do comparison
    :param require_perfect_match: if True require a perfect title match
    :return boolean true if record is verified or does not require verification
    """

    if oclc_response is None:
        return False

    if not title_check:
        if oclc_response is not None:
            return True

    if oclc_response is not None:
        try:
            title_arr = get_oclc_node(oclc_response)
            if title_arr is not None:
                if len(title_arr) == 2:
                    node_text = normalize_title(title_arr[1])
                    pnca_title = normalize_title(title)
                    if require_perfect_match:
                        # This is using ratio 100 so a perfect match is required.
                        return fuzz.find_match_with_ratio(pnca_title.lower(), node_text.lower(),
                                                          input_title, title_arr[0], 100, current_oclc_number,
                                                          title_log_writer)
                    else:
                        # Any match with a ratio greater than the default ratio will pass.
                        return fuzz.find_match(pnca_title.lower(), node_text.lower(),
                                               input_title, title_arr[0], current_oclc_number, title_log_writer)

        except Exception as e:
            print(e)
    else:
        return False


def log_035_details(fields, title, writer):
    for field_element in fields:
        for field in field_element.get_subfields('z'):
            z_value = __get_oclc_035_value(field)
            try:
                if z_value:
                    writer.write('z\t' + z_value + '\t' + title + '\n')
            except Exception as err:
                print('error writing to 035 log.')
                print(err)

        a_subfield = field_element.get_subfields('a')
        if len(a_subfield) == 0:
            writer.write('a missing\t' + ' ' + '\t' + title + '\n')
        for field in a_subfield:
            if len(a_subfield) > 1:
                a_value = __get_oclc_035_value(field)
                try:
                    if a_value:
                        writer.write('a duplicate\t' + a_value + '\t' + title + '\n')
                except Exception as err:
                    print('error writing to 035 log.')
                    print(err)


def get_subfields_arr(field):
    sub_fields = field.subfields_as_dict()
    keys = sub_fields.keys()
    subs = []
    for key in keys:
        subs_list = sub_fields[key]
        for val in subs_list:
            subs.append(key)
            subs.append(val)
    return subs
