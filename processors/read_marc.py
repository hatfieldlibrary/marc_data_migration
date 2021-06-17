from pymarc import MARCReader


class MarcReader:
    handle = None

    def get_reader(self, file, utf8_handling='ignore', permissive=True):
        self.handle = open(file, 'rb')
        return MARCReader(self.handle, permissive=permissive, to_unicode=True, utf8_handling=utf8_handling)

    def get_reader_unicode(self, file, encoding: str = 'iso8859-1'):
        self.handle = open(file, 'rb')
        # If MARC-8 handling doesn't work it could be that your source file is using an unexpected
        # character encoding set. If the file is encoded in anything other that iso8859-1 (extended Latin),
        # try setting the file_encoding parameter accordingly. This will skip marc8 to utf-8 conversion.
        # Python 3 will covert to utf-8 by default.
        # In our initial project, Windows-1252 was the right encoding.
        # See https://docs.python.org/3/library/codecs.html#codec-base-classes for python 3 documentation.
        # 'replace' will replace invalid utf-8 encoding with a replacement marker.
        return MARCReader(self.handle, file_encoding=encoding, to_unicode=True, utf8_handling='replace')

    def close(self):
        self.handle.close()