from pymarc import MARCReader


class MarcReader:
    handle = None

    def get_reader(self, file):
        self.handle = open(file, 'rb')
        return MARCReader(self.handle, permissive=True, utf8_handling='ignore')

    def close(self):
        self.handle.close()
