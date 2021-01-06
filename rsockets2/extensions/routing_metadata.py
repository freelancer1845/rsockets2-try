

from typing import List, Union


def encode_route_tags(tags: Union[str, List[str]]) -> bytes:
    buffer = bytearray()
    if isinstance(tags, str):
        tags = [tags]

    for tag in tags:
        encoded = tag.encode('UTF-8')
        buffer.append(len(encoded))
        buffer.extend(encoded)

    return buffer


def decode_route_tags(data: memoryview) -> List[str]:

    tags = []

    if isinstance(data, memoryview) == False:
        data = memoryview(data)

    pos = 0
    size = len(data)

    while pos < size:
        tag_length = data[pos]
        pos += 1
        tags.append(data[pos:(pos + tag_length)].tobytes().decode('UTF-8'))
        pos += tag_length

    return tags
