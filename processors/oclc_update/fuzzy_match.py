from fuzzywuzzy import fuzz


class FuzzyMatcher:

    # Because of wide variation in cataloging practice
    # Levenshtein distance is not a good tool. Ultimately,
    # the score is of some value, but all records
    # require review.
    default_ratio = 50

    @staticmethod
    def __log_fuzzy_matches(value1, value2, match_result,
                            required_ratio, current_oclc_number, title_log_writer):
        """
        Logs matches and match ratios for later review.

        :param value1: oclc title without normalization
        :param value2: oclc title without normalization
        :param match_ratio: the fuzzy match ratio
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
                log_message = value1 + '\t' \
                          + value2 + '\t' \
                          + str(match_result) + '\t' \
                          + message + '\t' \
                          + current_oclc_number + '\t\n'

            except TypeError as err:
                print('Error creating title match log entry: ' + err)

            title_log_writer.write(log_message)

    def find_match(self, value1, value2):
        """
        Checks for matching titles using default fuzzy match ratio.

        :param value1: normalized original title
        :param value2: normalized oclc title
        :return: true for match
        """
        # Sorted tokens better handle variations in word order.
        match_ratio = fuzz.token_sort_ratio(value1, value2)

        if match_ratio >= self.default_ratio:
            return True

        return False

    def find_match_with_ratio(self, value1, value2, ratio):
        """
        Checks for matching titles using provided fuzzy match ratio.

        :param value1: normalized original title
        :param value2: normalized oclc title
        :param ratio: the matching threshold
        :return: true for match
        """
        match_ratio = fuzz.ratio(value1, value2)

        if match_ratio >= ratio:
            return True

        return False

    @staticmethod
    def check_ratio(value1, value2, required_ratio):
        match_ratio = fuzz.token_sort_ratio(value1, value2)
        return match_ratio >= required_ratio

    @staticmethod
    def get_ratio(value1, value2):
        return fuzz.token_sort_ratio(value1, value2)

    def log_fuzzy_match(self, original_title, oclc_title, match_result, required_ratio, current_oclc_number, title_log_writer):
        if title_log_writer is not None:
            self.__log_fuzzy_matches(original_title, oclc_title, match_result, required_ratio,
                                     current_oclc_number, title_log_writer)
