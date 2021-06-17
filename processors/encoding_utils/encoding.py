from chardet import UniversalDetector


class EncodingUtils:

    @staticmethod
    def detect_encoding(file):
        """
        If you are having problems with character encoding, it
        may help to check the source file.  This function uses
        the chardet python library to determine the file encoding.
        Result is printed to the console.
        :param file: the path to a processed file that contains bad encoding.
        :return:
        """
        input = open(file, 'rb')
        print("Opened the source file: " + file)
        print("Reading the file to detect character encoding.  This can take a while.")
        detector = UniversalDetector()
        for line in input:
            detector.feed(line)
            if detector.done: break
        detector.close()
        print()
        print(detector.result)



