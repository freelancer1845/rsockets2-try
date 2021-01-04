from tests.core.interactions.default_connection_mock import DefaultConnectionMock
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rsockets2.core.connection.default_connection import DefaultConnection
import unittest
from threading import Event
from rsockets2.core.interactions.fire_and_forget import local_fire_and_forget


class FireAndForgetTests(unittest.TestCase):

    def test_basic_fnf(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')

        event = Event()

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_FNF)
            event.set()

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        local_fire_and_forget(connection, 1, [
            metadata, data
        ])

        event.wait(1.0)
        self.assertTrue(event.is_set())
