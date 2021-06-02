from pymarc import MARCReader


class MarcReader:
    handle = None

    def get_reader(self, file, utf8_handling='ignore', permissive=True):
        self.handle = open(file, 'rb')
        return MARCReader(self.handle, permissive=permissive, to_unicode=True, utf8_handling=utf8_handling)

    def close(self):
        self.handle.close()
