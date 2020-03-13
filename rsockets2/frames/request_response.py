from .common import FrameType, read_meta_data_length
import struct
from .frame_abc import Frame_ABC
from abc import abstractmethod


class RequestResponse(Frame_ABC):

    def __init__(self):
        super().__init__()

        self.meta_data_present = False
        self.meta_data = None
        self.request_data = None

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = RequestResponse()

        data_read = 6
        frame.stream_id = stream_id
        frame.meta_data_present = flags >> 8 & 1 == 1
        if frame.meta_data_present:
            metaDataLength = read_meta_data_length(full_data, data_read)
            data_read += 3
            frame.meta_data = full_data[data_read:(data_read + metaDataLength)]
            data_read += metaDataLength

        frame.request_data = full_data[data_read:]
        return frame

    def __len__(self):
        bufferSize = 10

        if self.meta_data != None:
            bufferSize += 3
            bufferSize += len(self.meta_data)
        return bufferSize

    def to_bytes(self):
        if self.stream_id == 0:
            raise ValueError("Stream ID must be set!")

        bufferSize = len(self)

        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, self.stream_id)
        dataWritten = 4
        type_and_flags = FrameType.REQUEST_RESPONSE << 10
        if self.meta_data != None:
            type_and_flags |= (1 << 8)
        type_and_flags = type_and_flags
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2

        if self.meta_data != None:
            meta_data_length = len(self.meta_data)
            data[dataWritten] = meta_data_length >> 16 & 0xFF
            dataWritten += 1
            data[dataWritten] = meta_data_length >> 8 & 0xFF
            dataWritten += 1
            data[dataWritten] = meta_data_length & 0xFF
            dataWritten += 1
            data[dataWritten:(dataWritten + meta_data_length)] = self.meta_data
            dataWritten += meta_data_length

        data[dataWritten:] = self.request_data
        return data
