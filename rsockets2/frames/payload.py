from .common import FrameType, read_meta_data_length
import struct
import typing
from .frame_abc import Frame_ABC
from abc import abstractmethod


class Payload(Frame_ABC):

    def __init__(self):
        super().__init__()

        self.meta_data_present = False
        self.next_present = False
        self.complete = False
        self.follows = False
        self.meta_data = bytes(0)
        self.payload = bytes(0)

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = Payload()

        data_read = 6
        frame.stream_id = stream_id
        frame.meta_data_present = flags >> 8 & 1 == 1
        frame.next_present = flags >> 5 & 1 == 1
        frame.complete = flags >> 6 & 1 == 1
        frame.follows = flags >> 7 & 1 == 1
        if frame.meta_data_present:
            metaDataLength = read_meta_data_length(full_data, data_read)
            data_read += 3
            frame.meta_data = full_data[data_read:(data_read + metaDataLength)]
            data_read += metaDataLength

        frame.payload = full_data[data_read:]
        return frame

    def __len__(self):
        bufferSize = 6

        if len(self.meta_data) > 0:
            bufferSize += 3
            bufferSize += len(self.meta_data)

        bufferSize += len(self.payload)
        return bufferSize

    def to_bytes(self):
        if self.stream_id == 0:
            raise ValueError("Stream ID must be set!")

        bufferSize = len(self)

        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, self.stream_id)
        dataWritten = 4
        type_and_flags = FrameType.PAYLOAD << 10

        if self.meta_data != None:
            type_and_flags |= (1 << 8)
        if self.next_present:
            type_and_flags |= (1 << 5)
        if self.complete:
            type_and_flags |= (1 << 6)
        if self.follows:
            type_and_flags |= (1 << 7)
        type_and_flags = type_and_flags
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2

        if len(self.meta_data) > 0:
            meta_data_length = len(self.meta_data)
            data[dataWritten] = meta_data_length >> 16 & 0xFF
            dataWritten += 1
            data[dataWritten] = meta_data_length >> 8 & 0xFF
            dataWritten += 1
            data[dataWritten] = meta_data_length & 0xFF
            dataWritten += 1
            data[dataWritten:(dataWritten + meta_data_length)] = self.meta_data
            dataWritten += meta_data_length

        data[dataWritten:] = self.payload
        return data

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
