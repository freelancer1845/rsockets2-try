

from typing import Tuple, Union
import struct


class FrameHeader(object):

    header_decode = struct.Struct('>IH').unpack_from
    header_encode = struct.Struct('>IH').pack_into

    _decode_integer = struct.Struct('>I').unpack_from
    _encode_integer = struct.Struct('>I').pack_into
    _decode_short = struct.Struct('>H').unpack_from
    _encode_short = struct.Struct('>H').pack_into

    @staticmethod
    def stream_id_type_and_flags(buffer: bytes) -> Tuple[int, int]:
        data = FrameHeader.header_decode(buffer, 0)
        return (data[0], data[1] >> 10 & 63, data[1])

    @staticmethod
    def is_ignore_if_not_understood(buffer: bytes) -> bool:
        return (FrameHeader.stream_id_type_and_flags(buffer)[2] >> 9 & 1) == 1

    @staticmethod
    def is_metdata_present(buffer: bytes) -> bool:
        return (FrameHeader.stream_id_type_and_flags(buffer)[2] >> 8 & 1) == 1

    @staticmethod
    def encode_frame_header(buffer: bytearray,
                            stream_id: int,
                            frame_type: int,
                            ignore_if_not_understood: bool,
                            metadata_present: bool):
        frame_type_and_flags = frame_type << 10
        if ignore_if_not_understood:
            frame_type_and_flags &= 1 << 9
        if metadata_present:
            metadata_present &= 1 << 8
        FrameHeader.header_encode(buffer, 0, stream_id, frame_type_and_flags)

    @staticmethod
    def decode_24_bit(buffer: bytes, offset: int):
        return (buffer[offset] << 16 | buffer[offset + 1] << 8 | buffer[offset + 2])

    @staticmethod
    def encode_24_bit(buffer: bytearray, offset: int, value: int):
        buffer[offset] = value >> 16 & 0xFF
        buffer[offset + 1] = value >> 8 & 0xFF
        buffer[offset + 2] = value & 0xFF
