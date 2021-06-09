from fuzzywuzzy import fuzz


class FuzzyMatcher:

    # Because of wide variation in cataloging practice
    # Levenshtein distance is not a good tool. Ultimately,
    # the score is of some value, but all records
    # require review.
    default_ratio = 50

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

    @staticmethod
    def find_match_with_ratio(value1, value2, ratio):
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

