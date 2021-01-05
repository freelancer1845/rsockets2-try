from rsockets2.extensions.wellknown_mime_types import WELLKNOWN_MIME_TYPES
from rsockets2.extensions.stream_data_mime_types import encode_data_mime_types, decode_data_mime_types
import unittest


class StreamDataMimeTypesTest(unittest.TestCase):

    def test_encode_decode_wellknown(self):

        mime_types = tuple(WELLKNOWN_MIME_TYPES.keys())

        encoded = encode_data_mime_types(mime_types)

        decoded = decode_data_mime_types(memoryview(encoded))

        self.assertEqual(list(mime_types), decoded)

    def test_encode_decode_not_wellknown(self):

        mime_types = tuple(map(lambda x: x + "modified", WELLKNOWN_MIME_TYPES.keys()))

        encoded = encode_data_mime_types(mime_types)

        decoded = decode_data_mime_types(memoryview(encoded))

        self.assertEqual(list(mime_types), decoded)

