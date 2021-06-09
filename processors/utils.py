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


def remove_control_field_extra_chars(field):
    # Odd thing found in one record. This obviously should never happen.
    control_character_replacement = re.compile('\W+\d+$')
    field = re.sub(control_character_replacement, '', field)
    return field


def get_oclc_001_value(field_001, field_003):
    """
    Returns value from the 001 field if it's an OCLC number.
    The criteria for determining OCLC identifiers are:

        - The 001 value begins with 'ocn', 'ocm', or 'on'
        - OR the 003 field contains the OCoLC group identifier
        - AND the value is a valid OCLC number format.

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
        if value_003:
            # if no oclc prefix, check 003 for oclc label
            if 'OCoLC' in value_003:
                final_value_001 = value_001

    if final_value_001 is not None:
        # verify that we have a valid oclc number format.
        if valid_format_regex.match(final_value_001):
            final_value_001 = remove_control_field_extra_chars(final_value_001)
            return final_value_001

    return None


def is_oclc_prefix(field001):
    """
    Pymarc Field for 001
    :param field001: pymarc Field
    :return: boolean true if field includes an OCLC prefix
    """
    return 'ocn' in field001.value() or 'ocm' in field001.value() or 'on' in field001.value()


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
                    oclc_number = remove_control_field_extra_chars(oclc_number)
                    return oclc_number
            except Exception as err:
                print(err)
    return None


def get_oclc_title(oclc_response):
    """
    Gets the item title from the OCLC XML response.
    :param oclc_response: XML response
    :return: an array containing full OCLC title data and data from subfields a and b to use with fuzzy matching.
    """
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


def __normalize_title(title):
    """
    Remove punctuation and unnecessary spaces. This
    retains single spaces between words since they
    have no effect on levenshtein distance but might
    be useful for other metrics, like jaccard similarity.
    :param title: the title string
    :return: modified title
    """
    # remove non-characters
    non_char_substitution = re.compile('[.,\/#!$%\^&\*;:{}\[\]=\-_`~()]')
    # remove double spaces that might be created by non-char substitution
    double_space_substitution = re.compile('\s{2,}')
    # get rid of initial space
    initial_space_substitution = re.compile('^\s+')
    title = re.sub(non_char_substitution, ' ', title)
    title = re.sub(double_space_substitution, ' ', title)
    title = re.sub(initial_space_substitution, '', title)
    return title


def __remove_stop_words(title):
    stop_words = re.compile('\s[the|of|a|an|of|p|n]\s')
    return re.sub(stop_words, ' ', title)


def verify_oclc_response(oclc_response, title, title_check, require_perfect_match, ratio=50):
    """
    Verifies that the 245 subfield a and b values in the OCLC response
    match the expected value in the current record. This uses a fuzzy
    match based on Levenshtein distance, originally to
    account for diacritics issues. If you need an
    exact match, adjust the matching threshold to be 100.
    This method is currently not used.

    :param oclc_response: The XML root Element
    :param title: The record title 245(a),(b)
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
            title_arr = get_oclc_title(oclc_response)
            if title_arr is not None:
                if len(title_arr) == 2:
                    node_text = __normalize_title(title_arr[1])
                    pnca_title = __normalize_title(title)
                    if require_perfect_match:
                        # This is using ratio 100 so a perfect match is required.
                        return fuzz.find_match_with_ratio(pnca_title.lower(), node_text.lower(), 100)
                    else:
                        # Any match with a ratio greater than the provided ratio will pass.
                        return fuzz.find_match_with_ratio(pnca_title.lower(), node_text.lower(), ratio)

        except Exception as e:
            print(e)
    else:
        return False


def get_fuzzy_match_ratio(oclc_response, title):
    if oclc_response is not None:
        try:
            title_arr = get_oclc_title(oclc_response)
            if title_arr is not None:
                if len(title_arr) == 2:
                    node_text = __normalize_title(title_arr[1])
                    pnca_title = __normalize_title(title)
                    return fuzz.get_ratio(pnca_title.lower(), node_text.lower())
        except Exception as e:
            print(e)
    return None


def get_match_ratio(value1, value2):
    norm1 = __normalize_title(value1)
    norm2 = __normalize_title(value2)
    return fuzz.get_ratio(norm1, norm2)


def jaccard(list1, list2):
    """
    Computes and returns jaccard similarity for two titles
    represented as sets of words.
    :param list1: list containing first title words
    :param list2: list containing second title words
    :return: jaccard similarity measure
    """
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    return float(intersection) / union


def log_035_details(fields, title, writer):
    """
    Logs information about 035 fields
    :param fields: 035 pymarc fields
    :param title: item title
    :param writer: file handle for log file
    :return:
    """
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


def log_fuzzy_match(original_title, oclc_title, oclc_comparison, match_result, required_ratio,
                    current_oclc_number, title_log_writer):
    if title_log_writer is not None:
        norm1 = __normalize_title(original_title)
        norm2 = __normalize_title(oclc_comparison)
        norm1 = __remove_stop_words(norm1)
        norm2 = __remove_stop_words(norm2)
        list1 = norm1.lower().split()
        list2 = norm2.lower().split()
        jaccard_similarity = jaccard(list1, list2)
        __log_fuzzy_matches(original_title, oclc_comparison, match_result, required_ratio,
                            jaccard_similarity, current_oclc_number, title_log_writer)


def __log_fuzzy_matches(value1, value2, match_result,
                        required_ratio, jaccard_similarity, current_oclc_number, title_log_writer):
    """
    Logs matches and match ratios for later review.
    :param value1: oclc title without normalization
    :param value2: oclc title without normalization
    :param match_result: the fuzzy match ratio
    :param required_ratio: the ratio required to "pass"
    :param current_oclc_number: the oclc number used
    :param title_log_writer: fuzzy log file handle
    :return:
    """
    if title_log_writer is not None:
        if match_result >= required_ratio:
            message = 'passed'
        else:
            message = 'failed'

        try:
            log_message = str(match_result) + '\t' \
                          + "{:.4f}".format(jaccard_similarity) + '\t' \
                          + value1 + '\t' \
                          + value2 + '\t' \
                          + message + '\t' \
                          + current_oclc_number + '\t\n'

        except TypeError as err:
            print('Error creating title match log entry: ' + err)

        title_log_writer.write(log_message)
