from rsockets2.core.frames.lease import LeaseFrame
import unittest
import struct
import random


class LeaseFrameTests(unittest.TestCase):

    def test_resolves_time_to_live(self):

        buffer = bytearray(10)
        ttl = random.randint(0, 2 ** 31 - 1)
        struct.pack_into('>I', buffer, 6, ttl)

        self.assertEqual(LeaseFrame.time_to_live(buffer), ttl)

    def test_resolves_number_of_requests(self):

        buffer = bytearray(14)
        n = random.randint(0, 2 ** 31 - 1)
        struct.pack_into('>I', buffer, 10, n)

        self.assertEqual(LeaseFrame.number_of_requests(buffer), n)

    def test_resolves_metadata(self):
        metadata = "test_resolves_time_to_live (frames.lease_frame_test.LeaseFrameTests) ... ok".encode(
            'UTF-8')
        buffer = bytearray(14 + len(metadata))
        buffer[14:] = metadata

        self.assertEqual(LeaseFrame.metadata(buffer), metadata)

    def test_creates_lease_frame(self):

        ttl = random.randint(0, 2**31 - 1)
        n = random.randint(0, 2**31 - 1)
        metadata = "test_resolves_time_to_live (frames.lease_frame_test.LeaseFrameTests) ... ok".encode(
            'UTF-8')

        fragments = LeaseFrame.create_new(ttl, n, metadata)
        
        self.assertEqual(LeaseFrame.stream_id_type_and_flags(fragments[0])[0], 0)
        self.assertEqual(LeaseFrame.stream_id_type_and_flags(fragments[0])[1], 0x02)
        self.assertEqual(LeaseFrame.time_to_live(fragments[0]), ttl)
        self.assertEqual(LeaseFrame.number_of_requests(fragments[0]), n)
        self.assertEqual(fragments[1], metadata)
