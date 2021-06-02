from processors.read_marc import MarcReader


class EncodingProcessor:

    def decode(self, file):
        wrapper = MarcReader()
        reader = wrapper.get_reader(file, 'replace', False)
        for record in reader:
            if record is None:
                print(
                    "Current chunk: ",
                    reader.current_chunk,
                    " was ignored because the following exception raised: ",
                    reader.current_exception)
            else:
                title = record.title()
                #decoded = title.encode('utf-8', 'replace')
                #print(decoded)
                #print(bytes.decode(decoded, 'utf-8', 'replace'))
                rec_245 = record.get_fields('245')
                if len(rec_245) > 1:
                    print(title)
