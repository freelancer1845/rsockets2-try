from typing import Tuple
from rsockets2.core.frames import FrameHeader


def encode_simple_authentication(username: str, password: str) -> bytes:

    encoded_username = username.encode('UTF-8')
    encoded_password = password.encode('UTF-8')
    length_username = len(encoded_username)
    buffer = bytearray(2 + length_username + len(encoded_password))

    FrameHeader._encode_short(buffer, 0, length_username)
    buffer[2:(2 + length_username)] = encoded_username
    buffer[(2 + length_username):] = encoded_password

    return buffer


def decode_simple_authentication(data: memoryview) -> Tuple(str, str):

    if isinstance(data, memoryview) == False:
        data = memoryview(data)

    username_length = FrameHeader._decode_short(data, 0)[0]
    username = data[2:(2 + username_length)].tobytes().decode('UTF-8')
    password = data[(2 + username_length):].tobytes().decode('UTF-8')

    return (username, password)
