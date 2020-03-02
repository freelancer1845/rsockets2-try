from enum import IntEnum
import struct



class FrameType(IntEnum):
    RESERVED = 0x00
    SETUP = 0x01
    LEASE = 0x02
    KEEPALIVE = 0x03
    REQUEST_RESPONSE = 0x04
    REQUEST_FNF = 0x05
    REQUEST_STREAM = 0x06
    REQUEST_CHANNEL = 0x07
    REQUEST_N = 0x08
    CANCEL = 0x09
    PAYLOAD = 0x0A
    ERROR = 0x0B
    METADATA_PUSH = 0x0C
    RESUME = 0x0D
    RESUME_OK = 0x0E
    EXT = 0x3F


