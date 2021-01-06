from typing import Union


def encode_ascii(input: str) -> bytes:
    return input.encode('ASCII')


def decode_ascii(input: Union[bytes, bytearray, memoryview]) -> str:
    if isinstance(input, memoryview):
        return input.tobytes().decode('ASCII')
    else:
        return input.decode('ASCII')


def encode_utf8(input: str) -> bytes:
    return input.encode('UTF-8')


def decode_utf8(input: Union[bytes, bytearray, memoryview]) -> str:
    if isinstance(input, memoryview):
        return input.tobytes().decode('UTF-8')
    else:
        return input.decode('UTF-8')
