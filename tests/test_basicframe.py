import unittest

from rsockets2.frames.common import FrameType
from rsockets2.frames.parser import FrameParser


class BasicFrameTest(unittest.TestCase):

    def test_readsStreamIdCorrect(self):
        parser = FrameParser()

        data = self._testBytes(20, 0)
        frame = parser.parseFrame(data)

        self.assertEqual(frame.stream_id, 20)

        data = self._testBytes(5140, 0)
        frame = parser.parseFrame(data)

        self.assertEqual(frame.stream_id, 5140)

        data = self._testBytes(1315860, 0)
        frame = parser.parseFrame(data)

        self.assertEqual(frame.stream_id, 1315860)

        data = self._testBytes(336860180, 0)
        frame = parser.parseFrame(data)

        self.assertEqual(frame.stream_id, 336860180)

        data = self._testBytes(0xFFFFFFFF, 0)
        frame = parser.parseFrame(data)

        self.assertEqual(frame.stream_id, 2147483647)

    def test_readsFlagsAndTypes(self):
        frame_type = FrameType.EXT
        flags = 319

        type_and_flags = (frame_type << 10) | flags

        data = self._testBytes(20, type_and_flags)
        frame = BasicFrame.from_bytes(data)
        self.assertFalse(frame._metadata_present)
        self.assertTrue(frame._can_ignore)
        self.assertEqual(frame.frame_type, FrameType.EXT)
        self.assertEqual(frame._flags, 319)

    def test_readsMetaData(self):
        frame_type = FrameType.PAYLOAD
        flags = 575

        type_and_flags = (frame_type << 10) | flags

        data = self._testBytes(20, type_and_flags, bytes([0xFF, 0xF1, 0xF4]))
        frame = BasicFrame.from_bytes(data)
        self.assertTrue(frame._metadata_present)
        self.assertFalse(frame._can_ignore)
        self.assertEqual(frame.frame_type, FrameType.PAYLOAD)

        self.assertEqual(frame._flags, 575)
        self.assertEqual(frame.meta_data, bytes([0xFF, 0xF1, 0xF4]))

    def test_readsMetaDataAndFrameData(self):
        frame_type = FrameType.PAYLOAD
        flags = 575

        type_and_flags = (frame_type << 10) | flags

        data = self._testBytes(20, type_and_flags, bytes(
            [0xFF, 0xF1, 0xF4]), b'Hello World')
        frame = BasicFrame.from_bytes(data)
        self.assertTrue(frame._metadata_present)
        self.assertFalse(frame._can_ignore)
        self.assertEqual(frame.frame_type, FrameType.PAYLOAD)

        self.assertEqual(frame._flags, 575)
        self.assertEqual(frame.meta_data, bytes([0xFF, 0xF1, 0xF4]))
        self.assertEqual(frame.data, b'Hello World')

    def _testBytes(self, stream_id: int, flags_and_type: int, metaData: bytes = None, frameData: bytes = None):

        data = []

        data.append(stream_id >> 24 & 0x7F)
        data.append(stream_id >> 16 & 0xFF)
        data.append(stream_id >> 8 & 0xFF)
        data.append(stream_id & 0xFF)

        data.append(flags_and_type >> 8 & 0xFF)
        data.append(flags_and_type & 0xFF)

        if metaData != None:
            metaLength = len(metaData)
            data.append(metaLength >> 16 & 0xFF)
            data.append(metaLength >> 8 & 0xFF)
            data.append(metaLength & 0xFF)
            data.append(0xFF)
            for mData in metaData:
                data.append(mData)

        if frameData != None:
            for d in frameData:
                data.append(d)

        return bytes(data)
