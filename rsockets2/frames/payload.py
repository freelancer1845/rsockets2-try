from .common import FrameType
import struct
import typing


class Payload(object):

    def __init__(self):
        super().__init__()

        self.meta_data_present = False
        self.stream_id = 0
        self.next_present = False
        self.complete = False
        self.follows = False
        self.meta_data = bytes(0)
        self.payload = bytes(0)

    @staticmethod
    def from_data(stream_id: int, flags: int, full_data: bytes):
        frame = Payload()

        data_read = 6
        frame.stream_id = stream_id
        frame.meta_data_present = flags >> 8 & 1 == 1
        frame.next_present = flags >> 5 & 1 == 1
        frame.complete = flags >> 6 & 1 == 1
        frame.follows = flags >> 7 & 1 == 1
        if frame.meta_data_present:
            metaDataLength = struct.unpack_from(">I", full_data, data_read)
            data_read += 3
            metaDataLength = metaDataLength >> 8 & 0xFFFFFF
            frame.meta_data = full_data[data_read:(data_read + metaDataLength)]
            data_read += metaDataLength

        frame.payload = full_data[data_read:]
        return frame

    def to_bytes(self):
        raise NotImplementedError()

    @staticmethod
    def from_fragments(fragments: []):
        frame = Payload()
        frame.stream_id = fragments[0].stream_id
        frame.next_present = fragments[0].next_present
        frame.complete = fragments[0].complete
        for fragment in fragments:
            frame.meta_data += fragment.meta_data
            frame.payload += fragment.payload
        if len(frame.meta_data) > 0:
            frame.meta_data_present = True
