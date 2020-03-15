from .common import FrameType, read_meta_data_length
import struct
from .frame_abc import Frame_ABC
from abc import abstractmethod
from ..common import RSocketConfig


class ResumeOkFrame(Frame_ABC):

    def __init__(self):
        super().__init__()

        self.last_received_client_position = 0

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = ResumeOkFrame()

        data_read = 6

        frame.last_received_client_position, = struct.unpack_from(
            ">Q", full_data, data_read)

        return frame

    def __len__(self):
        bufferSize = 6 + 8
        return bufferSize

    def to_bytes(self):
        data = bytearray(len(self))

        struct.pack_into(">I", data, 0, 0)
        dataWritten = 4
        type_and_flags = FrameType.RESUME_OK << 10
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2

        struct.pack_into(">Q", data, dataWritten,
                         self.last_received_client_position)

        return data


class ResumeFrame(Frame_ABC):

    def __init__(self):
        super().__init__()

        self.major_version = 0
        self.minor_version = 0
        self.resume_identification_token = bytes(0)
        self.last_received_server_position = -1
        self.first_available_client_position = -1

    @classmethod
    def from_config(cls, config: RSocketConfig, token: bytes, last_received_server_position: int, first_available_client_position: int):
        frame = ResumeFrame()
        frame.major_version = config.major_version
        frame.minor_version = config.minor_version
        frame.resume_identification_token = token
        frame.last_received_server_position = last_received_server_position
        frame.first_available_client_position = first_available_client_position

        return frame

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = ResumeFrame()

        data_read = 6

        frame.major_version, frame.minor_version, token_length = struct.unpack_from(
            ">HHH", full_data, data_read)

        data_read += 6

        frame.resume_identification_token = full_data[data_read:(
            data_read + token_length)]

        data_read += token_length

        frame.last_received_server_position, frame.first_available_client_position = struct.unpack_from(
            ">QQ", full_data, data_read)

        return frame

    def __len__(self):
        bufferSize = 12
        if self.resume_identification_token != None:
            bufferSize += 2
            bufferSize += len(self.resume_identification_token)
        bufferSize += 8
        bufferSize += 8
        return bufferSize

    def to_bytes(self):
        if self.resume_identification_token == None or len(self.resume_identification_token) == 0:
            raise ValueError("Resume Identification token not set or length 0")
        bufferSize = len(self)

        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, 0)
        dataWritten = 4
        type_and_flags = FrameType.RESUME << 10
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2
        struct.pack_into(">HHH", data, dataWritten,
                         self.major_version, self.minor_version, len(self.resume_identification_token))
        dataWritten += 6

        data[dataWritten:(dataWritten + len(self.resume_identification_token))
             ] = self.resume_identification_token
        dataWritten += len(self.resume_identification_token)

        struct.pack_into(">QQ", data, dataWritten, self.last_received_server_position,
                         self.first_available_client_position)
        dataWritten += 16

        return data
