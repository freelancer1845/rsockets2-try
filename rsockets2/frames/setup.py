from typing import Optional, Tuple

from rsockets2.frames.frame_header import FrameHeader


class SetupFrame(FrameHeader):

    @staticmethod
    def is_ignore_if_not_understood(buffer: bytes) -> bool:
        raise NotImplementedError('SetupFrames cannot be ignored')

    @staticmethod
    def is_resume_enable(buffer: bytes) -> bool:
        return SetupFrame.stream_id_type_and_flags(buffer)[2] << 7

    @staticmethod
    def honors_lease(buffer: bytes) -> bool:
        return SetupFrame.stream_id_type_and_flags(buffer)[2] << 6

    @staticmethod
    def major_version(buffer: bytes) -> int:
        return SetupFrame._decode_short(buffer, 6)[0]

    @staticmethod
    def minor_version(buffer: bytes) -> int:
        return SetupFrame._decode_short(buffer, 8)[0]

    @staticmethod
    def time_between_keepalive_frames(buffer: bytes) -> int:
        return SetupFrame._decode_integer(buffer, 10)[0]

    @staticmethod
    def max_lifetime(buffer: bytes) -> int:
        return SetupFrame._decode_integer(buffer, 14)[0]

    @staticmethod
    def resume_identification_token_length(buffer: bytes) -> int:
        return SetupFrame._decode_short(buffer, 18)[0]

    @staticmethod
    def resume_identification_token(buffer: bytes) -> bytes:
        return buffer[20:(20 + SetupFrame.resume_identification_token_length(buffer))]

    @staticmethod
    def meta_data_mime_length_and_pos(buffer: bytes) -> Tuple[int, int]:
        if SetupFrame.is_resume_enable(buffer):
            start = 20 + \
                SetupFrame.resume_identification_token_length(buffer)
            return buffer[start], (start + 1)
        else:
            return buffer[18], 19

    @staticmethod
    def meta_data_encoding_mime_type(buffer: bytes) -> str:
        length, start = SetupFrame.meta_data_mime_length_and_pos(buffer)
        return buffer[start:(start + length)].decode('ASCII')

    @staticmethod
    def data_mime_length_and_pos(buffer: bytes) -> Tuple[int, int]:
        meta_data_start = SetupFrame.meta_data_mime_length_and_pos(buffer)
        start = meta_data_start[0] + meta_data_start[1]
        return buffer[start], (start + 1)

    @staticmethod
    def data_mime_encoding_mime_type(buffer: bytes) -> str:
        length, start = SetupFrame.data_mime_length_and_pos(buffer)
        return buffer[start:(start + length)].decode('ASCII')

    @staticmethod
    def metadata_and_payload_start(buffer: bytes) -> int:
        length, start = SetupFrame.data_mime_length_and_pos(buffer)
        return start + length

    @staticmethod
    def metadata_length_and_pos(buffer: bytes) -> Tuple[int, int]:
        start = SetupFrame.metadata_and_payload_start(buffer)
        length = SetupFrame.decode_24_bit(buffer, start)
        return length, start + 3

    @staticmethod
    def metadata(buffer: bytes) -> bytes:
        length, start = SetupFrame.metadata_length_and_pos(buffer)
        return buffer[start:(start + length)]

    @staticmethod
    def data_start(buffer: bytes) -> bytes:
        if SetupFrame.is_metdata_present(buffer):
            meta_data_start_length_and_start = SetupFrame.metadata_length_and_pos(
                buffer)
            start = meta_data_start_length_and_start[0] + \
                meta_data_start_length_and_start[1]
            return start
        else:
            return SetupFrame.metadata_and_payload_start(buffer)

    @staticmethod
    def data(buffer: bytes) -> bytes:
        return buffer[SetupFrame.data_start(buffer):]

    @staticmethod
    def _set_major_version(buffer: bytearray, version: int):
        SetupFrame._encode_short(buffer, 6, version)

    @staticmethod
    def _set_minor_version(buffer: bytearray, version: int):
        SetupFrame._encode_short(buffer, 8, version)

    @staticmethod
    def _set_time_between_keepalive_frames(buffer: bytearray, time: int):
        SetupFrame._encode_integer(buffer, 10, time)

    @staticmethod
    def _set_max_lifetime(buffer: bytearray, time: int):
        SetupFrame._encode_integer(buffer, 14, time)

    @staticmethod
    def _set_resume_identification_token(buffer: bytearray, token: bytes):
        SetupFrame._encode_short(buffer, 18, len(token))
        buffer[20:(20 + len(token))] = token
        buffer[5] |= (1 << 7)

    @staticmethod
    def _set_metadata_encoding_mime_type(buffer: bytearray, mime_type: str):
        start = SetupFrame.meta_data_mime_length_and_pos(buffer)[1] - 1
        buffer[start] = len(mime_type) & 0xFF
        buffer[start + 1:len(mime_type) + 1 +
               start] = mime_type.encode('ASCII')

    @staticmethod
    def _set_data_encoding_mime_type(buffer: bytearray, mime_type: str):
        start = SetupFrame.data_mime_length_and_pos(buffer)[1] - 1
        buffer[start] = len(mime_type) & 0xFF
        buffer[start + 1:len(mime_type) + 1 +
               start] = mime_type.encode('ASCII')

    @staticmethod
    def _set_metadata(buffer: bytearray, metadata: bytes):
        start = SetupFrame.metadata_and_payload_start(buffer)

        buffer[4] |= 1
        SetupFrame.encode_24_bit(buffer, start, len(metadata))
        buffer[(start + 3):(start + 3 + len(metadata))] = metadata

    @staticmethod
    def _set_data(buffer: bytearray, data: bytes):
        if SetupFrame.is_metdata_present(buffer):
            meta_length, meta_pos = SetupFrame.metadata_length_and_pos(buffer)
            buffer[meta_pos + meta_length:] = data
        else:
            buffer[SetupFrame.metadata_and_payload_start(buffer):] = data

    @staticmethod
    def create_new(
        major_version: int,
        minor_version: int,
        time_between_keepalive_frames: int,
        max_lifetime: int,
        resume_identification_token: Optional[bytes],
        metadata_encoding_mime_type: str,
        data_encoding_mime_type: str,
        metadata: Optional[bytes],
        data: Optional[bytes]
    ) -> bytes:
        buffer_size = 18
        if (resume_identification_token != None):
            buffer_size += 2 + len(resume_identification_token)
        buffer_size += 1 + len(metadata_encoding_mime_type)
        buffer_size += 1 + len(data_encoding_mime_type)
        if metadata != None:
            buffer_size += 3 + len(metadata)
        if data != None:
            buffer_size += len(data)

        buffer = bytearray(buffer_size)

        SetupFrame._set_major_version(buffer, major_version)
        SetupFrame._set_minor_version(buffer, minor_version)
        SetupFrame._set_time_between_keepalive_frames(
            buffer, time_between_keepalive_frames)
        SetupFrame._set_max_lifetime(buffer, max_lifetime)
        if resume_identification_token != None:
            SetupFrame._set_resume_identification_token(buffer,
                                                        resume_identification_token)
        SetupFrame._set_metadata_encoding_mime_type(buffer,
                                                    metadata_encoding_mime_type)
        SetupFrame._set_data_encoding_mime_type(
            buffer, data_encoding_mime_type)
        if metadata != None:
            SetupFrame._set_metadata(buffer, metadata)
        if data != None:
            SetupFrame._set_data(buffer, data)
        return buffer
