from rsockets2.extensions.wellknown_mime_types import WellknownMimeTypes
from rsockets2.extensions.stream_data_mime_types import encode_data_mime_types, decode_data_mime_types
import unittest


class StreamDataMimeTypesTest(unittest.TestCase):

    def test_encode_decode_wellknown(self):

        mime_types = tuple(
            map(lambda v: v.name, WellknownMimeTypes.__members__.values()))

        encoded = encode_data_mime_types(mime_types)

        decoded = decode_data_mime_types(memoryview(encoded))

        self.assertEqual(list(mime_types), decoded)

    def test_encode_decode_not_wellknown(self):

        mime_types = tuple(
            map(lambda v: v.name + "changed", WellknownMimeTypes.__members__.values()))

        encoded = encode_data_mime_types(mime_types)

        decoded = decode_data_mime_types(memoryview(encoded))

        self.assertEqual(list(mime_types), decoded)
