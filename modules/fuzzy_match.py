from fuzzywuzzy import fuzz


class FuzzyMatcher:

    @staticmethod
    def log(value1, value2):

    @staticmethod
    def findMatch(value1, value2, title_log_writer):
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            title_log_writer.write('Input title: ' + value1 + ', Output title: ' +
                                   value2 + ' ratio: ' + str(match_ratio))
        if match_ratio >= 80:
            return True
        return False

    @staticmethod
    def findMatchWithRatio(value1, value2, ratio, title_log_writer):
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            title_log_writer.write('Input title: ' + value1 + ', Output title: ' +
                                   value2 + ' ratio: ' + str(match_ratio))
        if match_ratio >= ratio:
            return True
        return False

