import random
from rsockets2.core.frames.error import ErrorCodes, ErrorFrame
import unittest
import struct


class ErrorFrameTest(unittest.TestCase):

    def test_resolves_error_code(self):
        buffer = bytearray(10)
        code = random.choice(list(ErrorCodes)).value
        struct.pack_into('>I', buffer, 6, code)
        self.assertEqual(ErrorFrame.error_code(buffer), code)

    def test_resolves_error_data(self):
        message = "test_resolves_error_code (frames.error_frame_test.ErrorFrameTest) ... ok"
        message_data = message.encode('UTF-8')
        buffer = bytearray(10 + len(message_data))
        buffer[10:] = message_data
        self.assertEqual(ErrorFrame.error_data(buffer), message)

    def test_creates_all_necessary_field(self):

        stream_id = random.randint(0, 2 ** 31 - 1)
        code = random.choice(list(ErrorCodes)).value
        message = "test_resolves_error_data (frames.error_frame_test.ErrorFrameTest) ... ok"

        data = ErrorFrame.create_new(stream_id, code, message)
        
        self.assertEqual(ErrorFrame.stream_id_type_and_flags(data[0])[0], stream_id)
        self.assertEqual(ErrorFrame.stream_id_type_and_flags(data[0])[1], 0x0B)
        self.assertEqual(ErrorFrame.error_code(data[0]), code)
        self.assertEqual(data[1].decode('UTF-8'), message)
