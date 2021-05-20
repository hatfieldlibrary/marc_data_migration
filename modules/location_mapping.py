import re


class LocationMapper:
    mapping = {
        'a': 'pstacks',
        'b': 'pstacks',
        'c': 'pstacks',
        'd': 'pstacks',
        'e': 'pstacks',
        'f': 'pstacks',
        'g': 'pstacks',
        'h': 'pstacks',
        'j': 'pstacks',
        'k': 'pstacks',
        'l': 'pstacks',
        'm': 'pstacks',
        'n': 'pstacks',
        'na': 'pstacks',
        'nb': 'pstacks',
        'nc': 'pstacks',
        'nd': 'pstacks',
        'ne': 'pmezzstacks',
        'nk': 'pmezzstacks',
        'nx': 'pmezzstacks',
        'p': 'pmezzstacks',
        'q': 'pmezzstacks',
        'r': 'pmezzstacks',
        's': 'pmezzstacks',
        't': 'pmezzstacks',
        'u': 'pmezzstacks',
        'v': 'pmezzstacks',
        'z': 'pmezzstacks',
        'over': 'pover',
        'zine': 'pzine',
        'periodical': 'pperiod',
        'video': 'pvhs',
        'thesis': 'ptheses',
        'dvd': 'pmezzdvd',
        'games': 'pmezzgame',
        'spec': 'pspecial',
        'archive': 'parchives',

        '1st Floor CDs': 'pcds',
        'OVERSIZE PERIODICALS': 'pmezzover'
    }

    def get_location(self, temp_location):
        return self.mapping[temp_location]

    def get_location_by_callnumber(self, call_number):
        key = self.get_key(call_number)
        location = self.mapping[key]
        return location

    @staticmethod
    def get_key(call_number):
        if not call_number:
            print('Missing call number')
        lower_case = call_number.lower()
        try:
            if re.match("^over", lower_case):
                return 'over'
            if re.match("^periodical", lower_case):
                return 'periodical'
            if re.match("^thesis", lower_case):
                return 'thesis'
            if re.match("^games", lower_case):
                return 'games'
            if re.match("^archive", lower_case):
                return 'archive'
            if re.match("^spec", lower_case):
                return 'spec'
            if re.match("^dvd", lower_case):
                return 'dvd'
            if re.match("^zine", lower_case):
                return 'zine'
            if re.match(r"^(na|nb|nc|nd|ne|nk|nx)", lower_case):
                call = re.match("^(na|nb|nc|nd|ne|nk|nx)", lower_case)
                return call.group(0)
            # all special cases handled so return first character.
            return lower_case[0]

        except Exception as err:
            print('Error getting call number map key')
            print(err)
