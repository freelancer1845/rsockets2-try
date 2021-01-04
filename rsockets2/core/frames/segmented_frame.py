

from typing import Union


class FrameSegments(object):

    def __init__(self) -> None:
        self._fragments = []
        self._length = 0

    def append_fragment(self, data: Union[bytes, bytearray]) -> int:
        self._fragments.append(data)
        self._length += len(data)

    def __iter__(self):
        return iter(self._fragments)

    def __getitem__(self, key: int) -> Union[bytes, bytearray]:
        return self._fragments[key]

    def reduce(self) -> bytes:
        buffer = bytearray(self._length)
        pos = 0
        for fragment in self._fragments:
            buffer[pos:pos+len(fragment)] = fragment
            pos += len(fragment)

        return buffer

    def segments(self) -> int:
        return len(self._fragments)
