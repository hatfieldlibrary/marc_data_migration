from fuzzywuzzy import fuzz


class FuzzyMatcher:

    def log_match(self, value1, value2, match_ratio, title_log_writer):
        title_log_writer.write('Input title: ' + value1 + ', Output title: ' +
                               value2 + ' ratio: ' + str(match_ratio))

    def find_match(self, value1, value2, title_log_writer):
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            self.log_match(value1, value2, match_ratio, title_log_writer)
        if match_ratio >= 80:
            return True
        return False

    def find_match_with_ratio(self, value1, value2, ratio, title_log_writer):
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            self.log_match(value1, value2, match_ratio, title_log_writer)
        if match_ratio >= ratio:
            return True
        return False

