import unittest
from rsockets2.core.frames.frame_header import FrameHeader
import random


class FrameHeaderTest(unittest.TestCase):

    def test_stream_id_zero(self):
        data = bytes([0, 0, 0, 0, 0, 0])
        self.assertEqual(FrameHeader.stream_id_type_and_flags(
            data)[0], 0, 'Stream Id did not equal zero!')

    def test_stream_id_random(self):
        stream_id = random.randint(0, 2**31 - 1)
        data = bytes([*stream_id.to_bytes(4, 'big'), 0, 0])
        self.assertEqual(FrameHeader.stream_id_type_and_flags(
            data)[0], stream_id, 'Stream Id did not equal randomly generated id!')

    def test_parses_frame_type_correct(self):
        frame_type = random.randint(0, 63)

        data = bytes([0, 0, 0, 0, *(int(frame_type << 10).to_bytes(2, 'big'))])
        self.assertEqual(FrameHeader.stream_id_type_and_flags(
            data)[1], frame_type, 'Did not correctly parse frame type')

    def test_recognizes_ignore_frame_if_not_understood(self):

        data = bytes([0, 0, 0, 0, *(int(512).to_bytes(2, 'big'))])
        self.assertTrue(FrameHeader.is_ignore_if_not_understood(
            data), 'Should signal ignore if not understood')

    def test_recognizes_metadata_present(self):
        data = bytes([0, 0, 0, 0, *(int(256).to_bytes(2, 'big'))])
        self.assertTrue(FrameHeader.is_metdata_present(
            data), 'Should signal metadata present')

    def test_encodes_stream_id(self):

        buffer = bytearray(6)
        stream_id = random.randint(0, 2**31 - 1)
        FrameHeader.encode_frame_header(buffer, stream_id, 0, False, False)
        self.assertEqual(FrameHeader.stream_id_type_and_flags(buffer)[
                         0], stream_id, 'Failed to encode stream_id')

    def test_encodes_frame_type(self):
        buffer = bytearray(6)
        frame_type = random.randint(0, 63)
        FrameHeader.encode_frame_header(buffer, 0, frame_type, False, False)
        self.assertEqual(FrameHeader.stream_id_type_and_flags(buffer)[
                         1], frame_type, 'Failed to encode frame_type')
