
from rsockets2.core.types import WriteBuffer
from rsockets2.common import str_codec
from rsockets2.extensions.wellknown_mime_types import WellknownMimeTypes
from rsockets2.core.frames.segmented_frame import FrameSegments
from typing import List, Tuple, Union
from rsockets2.core.frames import FrameHeader


def extend_composite_metadata(target: Union[WriteBuffer, FrameSegments], mime_type: Union[str, WellknownMimeTypes], payload: Union[bytes, bytearray, memoryview]) -> FrameSegments:
    if isinstance(target, FrameSegments):
        payload_length = len(payload)
        if isinstance(mime_type, WellknownMimeTypes):
            header = bytearray(4)
            header[0] = (mime_type.value | 1 << 7)
            FrameHeader.encode_24_bit(header, 1, payload_length)
            target.append_fragment(
                header)
            target.append_fragment(payload)
        else:
            encoded_mime_type = mime_type.encode('ASCII')
            header = bytearray(1 + len(encoded_mime_type) + 3)
            header[0] = len(encoded_mime_type)
            header[1:(1 + len(encoded_mime_type))] = encoded_mime_type
            FrameHeader.encode_24_bit(
                header, 1 + len(encoded_mime_type), payload_length)
            target.append_fragment(header)
            target.append_fragment(payload)
    else:
        segments = FrameSegments()
        segments.append_fragment(target)
        return extend_composite_metadata(segments, mime_type, payload)


def encode_as_composite_metadata(
        mime_types: Tuple[Union[str, WellknownMimeTypes]],
        payloads: Tuple[Union[bytes, bytearray, memoryview]]) -> FrameSegments:

    if isinstance(mime_types, str) or isinstance(mime_types, WellknownMimeTypes):
        if isinstance(payloads, tuple) or isinstance(payloads, tuple):
            raise ValueError('Either proied as tuple or as single value!')
        mime_types = (mime_types,)
        payloads = (payloads,)

    if len(mime_types) != len(payloads):
        raise ValueError(
            'Must provide the same amount of mime_types as payloads!')

    fragments = FrameSegments()
    for i in range(len(mime_types)):
        mime_type = mime_types[i]
        payload = payloads[i]
        extend_composite_metadata(fragments, mime_type, payload)

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
            mime_type = WellknownMimeTypes(id_or_length & 0x7F).name
        else:
            mime_type = str_codec.decode_ascii(
                metadata[pos:(pos + id_or_length)])
            pos += id_or_length
        payload_length = FrameHeader.decode_24_bit(metadata, pos)
        pos += 3
        content.append((
            mime_type,
            memoryview(metadata)[pos:(pos + payload_length)]
        ))
        pos += payload_length
    return content
