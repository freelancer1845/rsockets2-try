

from rsockets2.core.types import ReadBuffer, WriteBuffer
from .wellknown_auth_types import WellknownAuthenticationTypes
from rsockets2.common import str_codec


class Authentication(object):

    @staticmethod
    def auth_type(buffer: ReadBuffer) -> str:
        id_or_length = buffer[0]
        if id_or_length >> 7 & 1 == 1:
            return WellknownAuthenticationTypes(id_or_length & 0x7F).name
        else:
            return str_codec.decode_ascii(buffer[1:(1 + id_or_length)])

    @staticmethod
    def auth_payload(buffer: ReadBuffer) -> ReadBuffer:
        id_or_length = buffer[0]
        if id_or_length >> 7 & 1 == 1:
            return buffer[1:]
        else:
            return buffer[(1 + id_or_length):]

    @staticmethod
    def create_new(auth_type: str, payload: ReadBuffer) -> WriteBuffer:
        buffer = bytearray()
        if auth_type in WellknownAuthenticationTypes:
            buffer.append(
                WellknownAuthenticationTypes[auth_type].value | (1 << 7))
        else:
            encoded_type = str_codec.encode_ascii(auth_type)
            buffer.append(len(encoded_type))
            buffer.extend(encoded_type)
        buffer.extend(payload)
        return buffer
