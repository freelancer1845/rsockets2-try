

from typing import List, Tuple
from .wellknown_mime_types import WELLKNOWN_MIME_TYPES, WELLKNOWN_MIME_TYPES_REVERSE
import codecs


def encode_data_mime_types(mime_types: Tuple[str]) -> bytes:

    buffer = bytearray(0)
    for mime_type in mime_types:
        if mime_type in WELLKNOWN_MIME_TYPES:
            buffer.append(WELLKNOWN_MIME_TYPES[mime_type] | (1 << 7))
        else:
            encoded = mime_type.encode('ASCII')
            buffer.append(len(encoded))
            buffer.extend(encoded)
    return buffer


def decode_data_mime_types(data: memoryview) -> List[str]:

    mime_types = []
    size = len(data)
    pos = 0
    while pos < size:
        id_or_length = data[pos]
        pos += 1
        if id_or_length >> 7 & 1 == 1:
            mime_types.append(WELLKNOWN_MIME_TYPES_REVERSE[id_or_length & 0x7F])
        else:
            mime_types.append(
                data[pos:(pos + id_or_length)].tobytes().decode('ASCII'))
            pos += id_or_length

    return mime_types
