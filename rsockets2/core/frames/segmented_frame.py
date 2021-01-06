

from typing import Union


class FrameSegments(object):

    def __init__(self) -> None:
        self._fragments = []
        self._length = 0

    def append_fragment(self, data: Union[bytes,
                                          bytearray, memoryview, 'FrameSegments']) -> None:
        if isinstance(data, memoryview):
            self._fragments.append(data)
        elif isinstance(data, FrameSegments):
            for fragment in data:
                self.append_fragment(fragment)
        else:
            self._fragments.append(data)
            self._length += len(data)

    def __iter__(self):
        return iter(self._fragments)

    def __len__(self):
        return self.length

    def fragments(self) -> int:
        return len(self._fragments)

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

    @property
    def length(self) -> int:
        return self._length
