from rsockets2.core.frames.keepalive import KeepaliveFrame
import unittest
import struct
import random


class KeepaliveFrameTests(unittest.TestCase):

    def test_resolves_respond_flag(self):

        buffer = bytearray(6)

        self.assertFalse(KeepaliveFrame.respond_with_keepalive(buffer))

        buffer[5] |= 1 << 7

        self.assertTrue(KeepaliveFrame.respond_with_keepalive(buffer))

    def test_resolves_last_received_position(self):
        buffer = bytearray(14)
        lrp = random.randint(0, 2**63 - 1)
        struct.pack_into('>Q', buffer, 6, lrp)

        self.assertEqual(KeepaliveFrame.last_received_position(buffer), lrp)

    def test_resolves_data(self):
        data = "test_resolves_last_received_position (frames.keeaplive_frame_test.KeepaliveFrameTests) ... ok".encode(
            'UTF-8')
        buffer = bytearray(14 + len(data))
        buffer[14:] = data

        self.assertEqual(KeepaliveFrame.data(buffer), data)

    def test_encodes_frame(self):
        lrp = random.randint(0, 2**63 - 1)
        data = "test_resolves_last_received_position (frames.keeaplive_frame_test.KeepaliveFrameTests) ... ok".encode(
            'UTF-8')
        
        frame = KeepaliveFrame.create_new(False, lrp, data)

        self.assertEqual(KeepaliveFrame.stream_id_type_and_flags(frame[0])[1], 0x03)
        self.assertFalse(KeepaliveFrame.respond_with_keepalive(frame[0]))
        self.assertEqual(KeepaliveFrame.last_received_position(frame[0]), lrp)
        self.assertEqual(frame[1], data)

        frame = KeepaliveFrame.create_new(True, lrp, data)

        self.assertEqual(KeepaliveFrame.stream_id_type_and_flags(frame[0])[1], 0x03)
        self.assertTrue(KeepaliveFrame.respond_with_keepalive(frame[0]))
        self.assertEqual(KeepaliveFrame.last_received_position(frame[0]), lrp)
        self.assertEqual(frame[1], data)

        frame = KeepaliveFrame.create_new(True, lrp, None)

        self.assertEqual(KeepaliveFrame.stream_id_type_and_flags(frame[0])[1], 0x03)
        self.assertTrue(KeepaliveFrame.respond_with_keepalive(frame[0]))
        self.assertEqual(KeepaliveFrame.last_received_position(frame[0]), lrp)
        self.assertTrue(frame.segments() == 1)


