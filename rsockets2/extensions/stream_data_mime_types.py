

from rsockets2.common import str_codec
from typing import List, Tuple
from .wellknown_mime_types import WellknownMimeTypes
import codecs


def encode_data_mime_types(mime_types: Tuple[str]) -> bytes:

    buffer = bytearray(0)
    for mime_type in mime_types:
        if mime_type in WellknownMimeTypes:
            buffer.append(WellknownMimeTypes[mime_type].value | (1 << 7))
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
            mime_types.append(WellknownMimeTypes(id_or_length & 0x7F).name)
        else:
            mime_types.append(
                str_codec.decode_ascii(data[pos:(pos + id_or_length)]))
            pos += id_or_length

    return mime_types
