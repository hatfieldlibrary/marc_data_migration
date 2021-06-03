from fuzzywuzzy import fuzz


class FuzzyMatcher:

    # Because of wide variation in cataloging practice
    # Levenshtein distance is not a good tool. Ultimately,
    # the score is of some interest, but all records
    # require review.
    default_ratio = 50

    @staticmethod
    def __log_fuzzy_matches(orig1, orig2, value1, value2, match_ratio,
                            ratio, current_oclc_number, title_log_writer):
        """
        Logs matches and match ratios for later review.

        :param orig1: original title without normalization
        :param orig2: oclc title without normalization
        :param value1: oclc title without normalization
        :param value2: oclc title without normalization
        :param match_ratio: the fuzzy match ratio
        :param current_oclc_number: the oclc number used
        :param title_log_writer: fuzzy log file handle
        :return:
        """
        if title_log_writer is not None:
            if match_ratio >= ratio:
                message = 'passed'
            else:
                message = 'failed'

            try:
                log_message = orig1 + '\t' \
                          + orig2 + '\t' \
                          + '"' + value1 + '"\t' \
                          + '"' + value2 + '"\t' \
                          + str(match_ratio) + '\t' \
                          + message + '\t' \
                          + current_oclc_number + '\t\n'

            except TypeError as err:
                print('Error creating title match log entry: ' + err)

            title_log_writer.write(log_message)

    def find_match(self, value1, value2, orig1, orig2, current_oclc_number, title_log_writer):
        """
        Checks for matching titles using default fuzzy match ratio.

        :param value1: normalized original title
        :param value2: normalized oclc title
        :param orig1: original title
        :param orig2: oclc title
        :param current_oclc_number: the oclc number used
        :param title_log_writer: fuzzy log file handle
        :return: true for match
        """
        # Sorted tokens better handle variations in word order.
        match_ratio = fuzz.token_sort_ratio(value1, value2)
        # Log all matches that are not exact.
        if match_ratio < 100:
            # Will be None if option not selected
            if title_log_writer is not None:
                self.__log_fuzzy_matches(orig1, orig2, value1, value2,
                                         match_ratio, self.default_ratio, current_oclc_number, title_log_writer)
        if match_ratio >= self.default_ratio:
            return True

        return False

    def find_match_with_ratio(self, value1, value2, orig1, orig2, ratio, current_oclc_number, title_log_writer):
        """
        Checks for matching titles using provided fuzzy match ratio.

        :param value1: normalized original title
        :param value2: normalized oclc title
        :param orig1: original title
        :param orig2: oclc title
        :param ratio: the matching threshold
        :param current_oclc_number: the oclc number used
        :param title_log_writer: fuzzy log file handle
        :return: true for match
        """
        match_ratio = fuzz.ratio(value1, value2)

        if match_ratio < 100:
            if title_log_writer is not None:
                self.__log_fuzzy_matches(orig1, orig2, value1, value2, match_ratio, ratio,
                                         current_oclc_number, title_log_writer)

        if match_ratio >= ratio:
            return True

        return False

