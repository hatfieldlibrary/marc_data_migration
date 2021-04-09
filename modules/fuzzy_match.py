from fuzzywuzzy import fuzz


class FuzzyMatcher:

    def log_fuzzy_matches(self, orig1, orig2, match_ratio, title_log_writer):
        if match_ratio > 80:
            message = 'passed'
        else:
            message = 'failed'
        title_log_writer.write(orig1 + '\t' +
                               orig2 + '\t' + str(match_ratio) + '\t' + message + '\n')

    def find_match(self, value1, value2, orig1, orig2,  title_log_writer):
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            self.log_fuzzy_matches(orig1, orig2, match_ratio, title_log_writer)
        if match_ratio >= 80:
            return True
        return False

    def find_match_with_ratio(self, value1, value2, orig1, orig2,  ratio, title_log_writer):
        match_ratio = fuzz.ratio(value1, value2)
        if match_ratio < 100:
            self.log_fuzzy_matches(orig1, orig2, match_ratio, title_log_writer)
        if match_ratio >= ratio:
            return True
        return False

