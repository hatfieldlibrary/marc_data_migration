import urllib.request
import xml.etree.ElementTree as ET


class OclcConnector:

    def getOclcResponse(self, oclc, oclc_developer_key):
        path = 'http://www.worldcat.org/webservices/catalog/content/' + oclc + '?wskey=' + oclc_developer_key
        # print(path)
        with urllib.request.urlopen(path) as response:
            xml_response = response.read().decode()
            try:
                return ET.fromstring(xml_response)
            except:
                print('Cannot parse marcxml response.')






