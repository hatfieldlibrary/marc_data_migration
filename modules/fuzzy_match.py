from fuzzywuzzy import fuzz


class FuzzyMatcher:

    def __log_fuzzy_matches(self, orig1, orig2, match_ratio, title_log_writer):
        """
        Logs matches and match ratios.

        :param orig1: original title without normalization
        :param orig2: oclc title without normalization
        :param match_ratio: the fuzzy match ratio
        :param title_log_writer: fuzzy log file handle
        :return:
        """
        if title_log_writer is not None:
            if match_ratio > 80:
                message = 'passed'
            else:
                message = 'failed'
            title_log_writer.write(orig1 + '\t' +
                                   orig2 + '\t' + str(match_ratio) + '\t' + message + '\n')

    def find_match(self, value1, value2, orig1, orig2,  title_log_writer):
        """
        Checks for matching titles using default fuzzy match ratio.

        :param value1: normalized original title
        :param value2: normalized oclc title
        :param orig1: original title
        :param orig2: oclc title
        :param title_log_writer: fuzzy log file handle
        :return: true for match
        """
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            # Will be None if option not selected
            if title_log_writer is not None:
                self.__log_fuzzy_matches(orig1, orig2, match_ratio, title_log_writer)
        if match_ratio >= 80:
            return True
        return False

    def find_match_with_ratio(self, value1, value2, orig1, orig2,  ratio, title_log_writer):
        """
        Checks for matching titles using provided fuzzy match ratio.

        :param value1: normalized original title
        :param value2: normalized oclc title
        :param orig1: original title
        :param orig2: oclc title
        :param ratio: the matching threshold
        :param title_log_writer: fuzzy log file handle
        :return: true for match
        """
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            # Will be None if option not selected
            if title_log_writer is not None:
                self.__log_fuzzy_matches(orig1, orig2, match_ratio, title_log_writer)
        if match_ratio >= ratio:
            return True
        return False

