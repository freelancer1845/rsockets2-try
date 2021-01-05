

def encode_bearer(token: str) -> bytes:
    return token.encode('UTF-8')


def decode_bearer(data: memoryview):
    if isinstance(data, memoryview) == False:
        data = memoryview(data)

    return data.tobytes().decode('UTF-8')

