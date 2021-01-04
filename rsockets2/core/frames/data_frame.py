
from .frame_header import FrameHeader


class DataFrame(FrameHeader):

    @staticmethod
    def fragments_follow(buffer: bytes) -> bool:
        return buffer[5] >> 7 & 1 == 1

    @staticmethod
    def complete(buffer: bytes) -> bool:
        return buffer[5] >> 6 & 1 == 1


    @staticmethod
    def metadata(buffer: bytes, offset: int) -> memoryview:
        length = FrameHeader.decode_24_bit(buffer, offset)
        return memoryview(buffer)[(offset + 3):(offset + 3 + length)]

    @staticmethod
    def data(buffer: bytes, offset: int) -> memoryview:
        if DataFrame.is_metdata_present(buffer):
            offset = FrameHeader.decode_24_bit(buffer, offset) + 3 + offset
        else:
            offset = offset
        return memoryview(buffer)[offset:]

    @staticmethod
    def _set_fragment_follows(buffer: bytearray, value: bool):
        if value == True:
            buffer[5] |= 1 << 7
    
    @staticmethod
    def _set_complete(buffer: bytearray, value: bool):
        if value:
            buffer[5] |= 1 << 6

    @staticmethod
    def _set_metadata_length(buffer: bytearray, length: int, offset: int):
        DataFrame.encode_24_bit(buffer, offset, length)
