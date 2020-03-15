from .common import FrameType, read_meta_data_length
import struct
from .frame_abc import Frame_ABC
from abc import abstractmethod
from ..common import RSocketConfig


class SetupFrame(Frame_ABC):

    def __init__(self):
        super().__init__()

        self.major_version = 0
        self.minor_version = 0
        self.keepalive_time = 0
        self.max_lifetime = 0
        self.resume_identification_token = None
        self.meta_data_mime_type = bytes(0)
        self.data_mime_type = bytes(0)
        self.meta_data = None
        self.setup_payload = bytes(0)
        self.honors_lease = False

    @classmethod
    def from_config(cls, config: RSocketConfig):
        frame = SetupFrame()
        frame.major_version = config.major_version
        frame.minor_version = config.minor_version
        frame.keepalive_time = config.keepalive_time
        frame.max_lifetime = config.max_liftime
        frame.meta_data_mime_type = config.meta_data_mime_type
        frame.data_mime_type = config.data_mime_type
        frame.honors_lease = config.honors_lease

        return frame

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = SetupFrame()

        data_read = 6

        frame.major_version, frame.minor_version, frame.keepalive_time, frame.max_lifetime = struct.unpack_from(
            ">HHII", full_data, data_read)
        frame.keepalive_time &= 0x7FFFFFFF
        frame.max_lifetime &= 0x7FFFFFFF

        data_read += 12

        if flags >> 6 & 1 == 1:
            frame.honors_lease = True

        if flags >> 7 & 1 == 1:
            token_length = struct.unpack_from(">H", full_data, data_read)
            data_read += 2
            frame.resume_identification_token = full_data[data_read:(
                data_read + token_length)]
            data_read += token_length

        mime_length = int(full_data[data_read])
        data_read += 1
        frame.meta_data_mime_type = full_data[data_read:(
            data_read + mime_length)].decode('US-ASCII')
        data_read += mime_length
        mime_length = int(full_data[data_read])
        data_read += 1
        frame.data_mime_type = full_data[data_read:(
            data_read + mime_length)].decode('US-ASCII')

        if flags >> 8 & 1 == 1:
            metaDataLength = read_meta_data_length(full_data, data_read)
            data_read += 3
            frame.meta_data = full_data[data_read:(data_read + metaDataLength)]
            data_read += metaDataLength

        frame.setup_payload = full_data[data_read:]

        return frame

    def __len__(self):
        bufferSize = 18
        if self.resume_identification_token != None:
            bufferSize += 2
            bufferSize += len(self.resume_identification_token)
        bufferSize += 1  # Mime Length Meta
        if self.meta_data_mime_type != None:
            bufferSize += len(self.meta_data_mime_type)
        bufferSize += 1  # Mime Length Data
        if self.data_mime_type != None:
            bufferSize += len(self.data_mime_type)
        bufferSize += len(self.setup_payload)
        return bufferSize

    def to_bytes(self):
        bufferSize = len(self)

        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, 0)
        dataWritten = 4
        type_and_flags = FrameType.SETUP << 10
        if self.meta_data != None:
            type_and_flags |= (1 << 8)
        if self.resume_identification_token != None:
            type_and_flags |= (1 << 7)
        if self.honors_lease == True:
            type_and_flags |= (1 << 6)
        type_and_flags = type_and_flags
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2
        struct.pack_into(">HH", data, dataWritten,
                         self.major_version, self.minor_version)
        dataWritten += 4
        struct.pack_into(">II", data, dataWritten, self.keepalive_time,
                         self.max_lifetime)
        dataWritten += 8

        if self.resume_identification_token != None:
            struct.pack_into(">H", data, dataWritten, len(
                self.resume_identification_token))
            dataWritten += 2
            data[(dataWritten):(dataWritten + len(self.resume_identification_token))
                 ] = self.resume_identification_token
            dataWritten += len(self.resume_identification_token)

        mime_meta_data_length = len(self.meta_data_mime_type) & 0xFF
        data[dataWritten] = mime_meta_data_length
        dataWritten += 1
        data[dataWritten:(dataWritten + mime_meta_data_length)
             ] = self.meta_data_mime_type
        dataWritten += mime_meta_data_length

        mime_data_length = len(self.data_mime_type) & 0xFF
        data[dataWritten] = mime_data_length
        dataWritten += 1
        data[dataWritten:(dataWritten + mime_data_length)
             ] = self.data_mime_type
        dataWritten += mime_data_length

        data[dataWritten:(dataWritten + len(self.setup_payload))
             ] = self.setup_payload

        return data
