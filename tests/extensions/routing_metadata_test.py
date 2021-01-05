import unittest
import random
from rsockets2.extensions.routing_metadata import encode_route_tags, decode_route_tags


class RoutingMetadataTests(unittest.TestCase):

    UTFCHARS = ''.join(tuple(chr(i) for i in range(32, 0x110000) if chr(i).isprintable()))

    def generate_random_utf8(self) -> bytes:
        return ''.join(random.sample(self.UTFCHARS, 50)).encode('UTF-8')


    def test_encode_decode(self):
        tags = []
        for i in range(20):
            tag = self.generate_random_utf8().decode('UTF-8')
            tags.append(tag)

        encoded = encode_route_tags(tags)
        decoded = decode_route_tags(encoded)

        self.assertEqual(tags, decoded)


