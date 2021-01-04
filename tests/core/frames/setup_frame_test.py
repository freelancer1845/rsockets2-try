import random
import struct
from typing import Tuple
from rsockets2.core.frames.setup import SetupFrame
import unittest
import string


class SetupFrameTests(unittest.TestCase):

    def test_cannot_be_ignored_throws_exception(self):
        with self.assertRaises(NotImplementedError):
            SetupFrame.is_ignore_if_not_understood(bytes(24))

    def test_resolves_resume_enabled(self):

        data = bytearray(6)

        self.assertFalse(SetupFrame.is_resume_enable(data))

        data[5] |= (1 << 7)

        self.assertTrue(SetupFrame.is_resume_enable(data))

    def test_resolves_lease_enabled(self):

        data = bytearray(6)

        self.assertFalse(SetupFrame.honors_lease(data))

        data[5] |= (1 << 6)

        self.assertTrue(SetupFrame.honors_lease(data))

    def test_resolves_major_version(self):
        data = bytearray(10)
        major_version = random.randint(0, 2 ** 16)
        struct.pack_into('>H', data, 6, major_version)

        self.assertEqual(major_version, SetupFrame.major_version(data))

    def test_resolves_minor_version(self):
        data = bytearray(10)
        minor_version = random.randint(0, 2 ** 16)
        struct.pack_into('>H', data, 8, minor_version)

        self.assertEqual(minor_version, SetupFrame.minor_version(data))

    def test_resolves_time_between_keepalive(self):
        data = bytearray(14)
        keepalive_time = random.randint(0, 2 ** 31 - 1)
        struct.pack_into('>I', data, 10, keepalive_time)

        self.assertEqual(
            keepalive_time, SetupFrame.time_between_keepalive_frames(data))

    def test_resolves_max_lifetime(self):
        data = bytearray(18)
        max_lifetime = random.randint(0, 2 ** 31 - 1)
        struct.pack_into('>I', data, 14, max_lifetime)

        self.assertEqual(max_lifetime, SetupFrame.max_lifetime(data))

    def test_resolves_setup_token(self):
        token_length = random.randint(0, 2 ** 16)
        token = random.randbytes(token_length)
        buffer = bytearray(18 + 2 + token_length)
        buffer[18 + 2:] = token
        struct.pack_into('>H', buffer, 18, token_length)
        self.assertEqual(token, SetupFrame.resume_identification_token(buffer))

    def random_mime_type(self, length: int):
        return ''.join((random.choice(string.printable) for i in range(length)))

    def test_resolves_metadata_encoding_mime_type_without_resume(self):
        mime_type_length = random.randint(1, 256)
        mime_type = self.random_mime_type(mime_type_length)
        buffer = bytearray(18 + 1 + mime_type_length)
        buffer[18] = mime_type_length
        buffer[19:] = mime_type.encode('ASCII')

        self.assertEqual(
            mime_type, SetupFrame.meta_data_encoding_mime_type(buffer))

    def test_resolves_metadata_encoding_mime_type_with_resume(self):

        meta_mime_type = self.random_mime_type(random.randint(0, 256))
        data_mime_type = self.random_mime_type(random.randint(0, 256))
        buffer, offset = self.create_buffer_with_resume_token_and_mime_types(
            meta_mime_type, data_mime_type, 0)

        self.assertEqual(
            data_mime_type, SetupFrame.data_mime_encoding_mime_type(buffer))

        self.assertEqual(
            meta_mime_type, SetupFrame.meta_data_encoding_mime_type(buffer))

    def test_resolves_data_encoding_mime_type(self):

        meta_mime_type = self.random_mime_type(random.randint(0, 256))
        data_mime_type = self.random_mime_type(random.randint(0, 256))
        buffer, offset = self.create_buffer_with_resume_token_and_mime_types(
            meta_mime_type, data_mime_type, 0)

        self.assertEqual(
            data_mime_type, SetupFrame.data_mime_encoding_mime_type(buffer))

    def create_buffer_with_resume_token_and_mime_types(self, meta_mime_type: str, data_mime_type: str, end_bytes: int) -> Tuple[bytearray, int]:
        meta_bytes = meta_mime_type.encode('ASCII')
        data_bytes = data_mime_type.encode('ASCII')
        mime_type_length = len(meta_bytes)
        data_mime_type_length = len(data_bytes)
        buffer, offset = self.create_buffer_with_token(
            mime_type_length + 1 + data_mime_type_length + 1 + end_bytes)
        buffer[offset] = mime_type_length
        buffer[offset + 1:(offset + 1 + mime_type_length)] = meta_bytes

        buffer[offset + 1 + mime_type_length] = data_mime_type_length
        buffer[offset + 1 + mime_type_length +
               1:(offset + 1 + mime_type_length + 1 + data_mime_type_length)] = data_bytes

        return buffer, (offset + 1 + mime_type_length + 1 + data_mime_type_length)

    def test_resolves_metadata(self):
        meta_mime_type = self.random_mime_type(random.randint(0, 256))
        data_mime_type = self.random_mime_type(random.randint(0, 256))

        meta_data_size = random.randint(0, 2 ** (24 - 1))
        meta_data = random.randbytes(meta_data_size)

        buffer, offset = self.create_buffer_with_resume_token_and_mime_types(
            meta_mime_type, data_mime_type, meta_data_size + 3)
        SetupFrame.encode_24_bit(buffer, offset, meta_data_size)
        buffer[(offset + 3):(offset + 3 + meta_data_size)] = meta_data

        self.assertEqual(
            meta_data, SetupFrame.metadata(buffer))

    def test_resolves_data(self):
        meta_mime_type = self.random_mime_type(random.randint(0, 256))
        data_mime_type = self.random_mime_type(random.randint(0, 256))

        meta_data_size = random.randint(0, 2 ** (24 - 1))
        meta_data = random.randbytes(meta_data_size)

        data_size = random.randint(0, 2 ** (24 - 1))
        data = random.randbytes(data_size)

        buffer, offset = self.create_buffer_with_resume_token_and_mime_types(
            meta_mime_type, data_mime_type, meta_data_size + 3 + data_size)

        buffer[4] |= 1

        SetupFrame.encode_24_bit(buffer, offset, meta_data_size)
        buffer[(offset + 3):(offset + 3 + meta_data_size)] = meta_data
        offset = offset + 3 + meta_data_size

        buffer[offset:(offset + data_size)] = data

        self.assertEqual(
            data, SetupFrame.data(buffer))

    def create_buffer_with_token(self, buffer_size_after_token: int) -> Tuple[bytearray, int]:
        token_length = random.randint(0, 2 ** 16)
        token = random.randbytes(token_length)
        buffer = bytearray(18 + 2 + token_length + buffer_size_after_token)
        struct.pack_into('>H', buffer, 18, token_length)
        buffer[5] |= (1 << 7)
        buffer[20:(20 + token_length)] = token
        return buffer, 20 + token_length

    def test_creates_without_resume_no_metadata_no_data(self):

        major_version = random.randint(0, 2 ** 16)
        minor_version = random.randint(0, 2 ** 16)
        keepalive_time = random.randint(0, 2 ** 31)
        max_lifetime = random.randint(0, 2 ** 31)
        meta_encoding = self.random_mime_type(random.randint(0, 256))
        data_encoding = self.random_mime_type(random.randint(0, 256))

        buffer = SetupFrame.create_new(
            major_version,
            minor_version,
            keepalive_time,
            max_lifetime,
            None,
            meta_encoding,
            data_encoding,
            None,
            None
        )
        self.assertEqual(SetupFrame.stream_id_type_and_flags(buffer)[1], 0x01)
        self.assertEqual(SetupFrame.major_version(buffer), major_version)
        self.assertEqual(SetupFrame.minor_version(buffer), minor_version)
        self.assertEqual(SetupFrame.time_between_keepalive_frames(buffer), keepalive_time)
        self.assertEqual(SetupFrame.max_lifetime(buffer), max_lifetime)
        self.assertEqual(SetupFrame.meta_data_encoding_mime_type(buffer), meta_encoding)
        self.assertEqual(SetupFrame.data_mime_encoding_mime_type(buffer), data_encoding)
    
    def test_creates_with_resume_no_metadata_no_data(self):
        major_version = random.randint(0, 2 ** 16)
        minor_version = random.randint(0, 2 ** 16)
        keepalive_time = random.randint(0, 2 ** 31)
        max_lifetime = random.randint(0, 2 ** 31)
        resume_token = random.randbytes(random.randint(0, 2 ** 16))
        meta_encoding = self.random_mime_type(random.randint(0, 256))
        data_encoding = self.random_mime_type(random.randint(0, 256))

        buffer = SetupFrame.create_new(
            major_version,
            minor_version,
            keepalive_time,
            max_lifetime,
            resume_token,
            meta_encoding,
            data_encoding,
            None,
            None
        )

        self.assertEqual(SetupFrame.resume_identification_token(buffer), resume_token)
        self.assertEqual(SetupFrame.meta_data_encoding_mime_type(buffer), meta_encoding)

    def test_creates_with_resume_metadata_no_data(self):
        major_version = random.randint(0, 2 ** 16)
        minor_version = random.randint(0, 2 ** 16)
        keepalive_time = random.randint(0, 2 ** 31)
        max_lifetime = random.randint(0, 2 ** 31)
        resume_token = random.randbytes(random.randint(0, 2 ** 16))
        meta_encoding = self.random_mime_type(random.randint(0, 256))
        data_encoding = self.random_mime_type(random.randint(0, 256))
        meta_data = random.randbytes(random.randint(0, 2 ** 24))


        buffer = SetupFrame.create_new(
            major_version,
            minor_version,
            keepalive_time,
            max_lifetime,
            resume_token,
            meta_encoding,
            data_encoding,
            meta_data,
            None
        )

        self.assertEqual(SetupFrame.metadata(buffer), meta_data)
    
    def test_creates_with_resume_metadata_data(self):
        major_version = random.randint(0, 2 ** 16)
        minor_version = random.randint(0, 2 ** 16)
        keepalive_time = random.randint(0, 2 ** 31)
        max_lifetime = random.randint(0, 2 ** 31)
        resume_token = random.randbytes(random.randint(0, 2 ** 16))
        meta_encoding = self.random_mime_type(random.randint(0, 256))
        data_encoding = self.random_mime_type(random.randint(0, 256))
        meta_data = random.randbytes(random.randint(0, 2 ** 24))
        data = random.randbytes(random.randint(0, 2 ** 24))


        buffer = SetupFrame.create_new(
            major_version,
            minor_version,
            keepalive_time,
            max_lifetime,
            resume_token,
            meta_encoding,
            data_encoding,
            meta_data,
            data
        )

        self.assertEqual(SetupFrame.data(buffer), data)

    def test_creates_with_resume_no_metadata_data(self):
        major_version = random.randint(0, 2 ** 16)
        minor_version = random.randint(0, 2 ** 16)
        keepalive_time = random.randint(0, 2 ** 31)
        max_lifetime = random.randint(0, 2 ** 31)
        resume_token = random.randbytes(random.randint(0, 2 ** 16))
        meta_encoding = self.random_mime_type(random.randint(0, 256))
        data_encoding = self.random_mime_type(random.randint(0, 256))
        data = random.randbytes(random.randint(0, 2 ** 24))


        buffer = SetupFrame.create_new(
            major_version,
            minor_version,
            keepalive_time,
            max_lifetime,
            resume_token,
            meta_encoding,
            data_encoding,
            None,
            data
        )

        self.assertEqual(SetupFrame.data(buffer), data)