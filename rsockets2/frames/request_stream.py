from .common import FrameType, read_meta_data_length
import struct


class RequestStream(object):

    def __init__(self):
        super().__init__()

        self.meta_data_present = False
        self.stream_id = 0
        self.initial_request = 1
        self.meta_data = None
        self.request_data = None

    @staticmethod
    def from_data(stream_id: int, flags: int, full_data: bytes):
        frame = RequestStream()

        data_read = 6
        frame.stream_id = stream_id
        frame.meta_data_present = flags >> 8 & 1 == 1

        frame.initial_request, = struct.unpack_from(">I", full_data, data_read)
        data_read += 4


        if frame.meta_data_present:
            metaDataLength = read_meta_data_length(full_data, data_read)
            data_read += 3
            frame.meta_data = full_data[data_read:(data_read + metaDataLength)]
            data_read += metaDataLength

        frame.request_data = full_data[data_read:]
        return frame

    def to_bytes(self):
        if self.stream_id == 0:
            raise ValueError("Stream ID must be set!")

        bufferSize = 10

        if self.meta_data != None:
            bufferSize += 3
            bufferSize += len(self.meta_data)

        bufferSize += len(self.request_data)

        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, self.stream_id)
        dataWritten = 4
        type_and_flags = FrameType.REQUEST_STREAM << 10
        if self.meta_data != None:
            type_and_flags |= (1 << 8)
        type_and_flags = type_and_flags
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2
        struct.pack_into(">I", data, dataWritten, self.initial_request)
        dataWritten += 4

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
