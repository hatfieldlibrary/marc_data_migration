from pymarc import MARCReader


class MarcReader:
    handle = None

    def get_reader(self, file, utf8_handling='ignore', permissive=True):
        self.handle = open(file, 'rb')
        return MARCReader(self.handle, permissive=permissive, to_unicode=True, utf8_handling=utf8_handling)

    def close(self):
        self.handle.close()

    def get_reader_unicode(self, file, encoding='utf-8'):
        self.handle = open(file, 'rb')
        # If pymarc's MARC-8 handling doesn't work, provide the encoding, or your best guess.
        # In our initial project, specifying Windows-1252 was the best solution.
        # utf8_handling: see https://docs.python.org/3/library/codecs.html#codec-base-classes for details.
        # 'replace' will replace invalid encoding with a replacement marker.
        return MARCReader(self.handle, file_encoding='Windows-1252', to_unicode=True, utf8_handling='replace')
