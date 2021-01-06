from rsockets2.extensions.composite_metadata import decode_composite_metadata, encode_as_composite_metadata
from rsockets2.extensions.wellknown_mime_types import WellknownMimeTypes
import unittest
import random


class CompositeMetadataTests(unittest.TestCase):

    def test_encode_decode_wellknown(self):
        mime_types = tuple(
            map(lambda v: v.name, WellknownMimeTypes.__members__.values()))
        metadata = [random.randbytes(100) for x in mime_types]

        encoded = encode_as_composite_metadata(mime_types, metadata).reduce()

        decoded = decode_composite_metadata(encoded)
        self.assertEqual(len(decoded), len(mime_types))
        for i in range(len(decoded)):
            mime_type = decoded[i][0]
            this_metadata = decoded[i][1]

            self.assertEqual(mime_types[i], mime_type)
            self.assertEqual(metadata[i], this_metadata)

    def test_encoded_decode_not_wellknown(self):
        mime_types = tuple(
            map(lambda v: v.name + "changed", WellknownMimeTypes.__members__.values()))
        metadata = [random.randbytes(100) for x in mime_types]

        encoded = encode_as_composite_metadata(mime_types, metadata).reduce()

        decoded = decode_composite_metadata(encoded)
        self.assertEqual(len(decoded), len(mime_types))
        for i in range(len(decoded)):
            mime_type = decoded[i][0]
            this_metadata = decoded[i][1]

            self.assertEqual(mime_types[i], mime_type)
            self.assertEqual(metadata[i], this_metadata)
