from fuzzywuzzy import fuzz


class FuzzyMatcher:

    @staticmethod
    def findMatch(value1, value2):
        if fuzz.ratio(value1, value2) >= 80:
            return True
        return False

    @staticmethod
    def findMatchWithRatio(value1, value2, ratio):
        if fuzz.ratio(value1, value2) >= ratio:
            return True
        return False

