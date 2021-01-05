
from rsockets2.extensions.wellknown_mime_types import WELLKNOWN_MIME_TYPES, WELLKNOWN_MIME_TYPES_REVERSE
from rsockets2.core.frames.segmented_frame import FrameSegments
from typing import List, Tuple, Union
from rsockets2.core.frames import FrameHeader


def encode_as_composite_metadata(
        mime_types: Tuple[str],
        payloads: Tuple[Union[bytes, bytearray, memoryview]]) -> FrameSegments:

    if len(mime_types) != len(payloads):
        raise ValueError(
            'Must provide the same amount of mime_types as payloads!')

    fragments = FrameSegments()
    for i in range(len(mime_types)):
        mime_type = mime_types[i]
        payload = payloads[i]
        payload_length = len(payload)
        if mime_type in WELLKNOWN_MIME_TYPES:
            header = bytearray(4)
            header[0] = (WELLKNOWN_MIME_TYPES[mime_type] | 1 << 7)
            FrameHeader.encode_24_bit(header, 1, payload_length)
            fragments.append_fragment(
                header)
            fragments.append_fragment(payload)
        else:
            encoded_mime_type = mime_type.encode('ASCII')
            header = bytearray(1 + len(encoded_mime_type) + 3)
            header[0] = len(encoded_mime_type)
            header[1:(1 + len(encoded_mime_type))] = encoded_mime_type
            FrameHeader.encode_24_bit(
                header, 1 + len(encoded_mime_type), payload_length)
            fragments.append_fragment(header)
            fragments.append_fragment(payload)

    return fragments


def decode_composite_metadata(
        metadata: Union[bytes, bytearray, memoryview]) -> List[Tuple[str, memoryview]]:

    content = []

    pos = 0
    while pos < len(metadata):
        id_or_length = metadata[pos]
        pos += 1
        mime_type = ""
        if id_or_length >> 7 & 1 == 1:
            mime_type = WELLKNOWN_MIME_TYPES_REVERSE[id_or_length & 0x7F]
        else:
            mime_type = memoryview(metadata)[pos:(
                pos + id_or_length)].tobytes().decode('ASCII')
            pos += id_or_length
        payload_length = FrameHeader.decode_24_bit(metadata, pos)
        pos += 3
        content.append((
            mime_type,
            memoryview(metadata)[pos:(pos + payload_length)]
        ))
        pos += payload_length
    return content
