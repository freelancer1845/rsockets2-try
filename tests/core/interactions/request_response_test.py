from rsockets2.core.exceptions import ApplicationError
from rsockets2.core.frames.error import ErrorCodes, ErrorFrame
from rsockets2.core.frames.payload import PayloadFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from tests.core.interactions.default_connection_mock import DefaultConnectionMock
from rsockets2.core.interactions.request_response import local_request_response
import unittest
from threading import Event


class RequestResponseTest(unittest.TestCase):

    def test_empty_response(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_RESPONSE)
            ans = PayloadFrame.create_new(1, False, True, False, None, None)
            connection.queue_on_receiver_stream(ans)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()

        local_request_response(connection, [
            metadata, data
        ]).subscribe(on_next=lambda x: self.fail(), on_error=lambda err: self.fail(str(err)), on_completed=lambda: event.set())

        event.wait(1.0)
        self.assertTrue(event.is_set())

    def test_response_with_complete(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_RESPONSE)
            ans = PayloadFrame.create_new(1, False, True, True, metadata, data)
            connection.queue_on_receiver_stream(ans)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()
        payloadReceived = Event()

        def assert_payload(response):
            self.assertEqual(response[0], metadata)
            self.assertEqual(response[1], data)
            payloadReceived.set()

        local_request_response(connection,  [
            metadata, data
        ]).subscribe(on_next=lambda x: assert_payload(x), on_error=lambda err: self.fail(str(err)), on_completed=lambda: event.set())

        event.wait(1.0)
        self.assertTrue(event.is_set())
        self.assertTrue(payloadReceived.is_set())

    def test_response_with_following_complete(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_RESPONSE)
            ans = PayloadFrame.create_new(
                1, False, True, True, metadata, data)
            connection.queue_on_receiver_stream(ans)
            cmpl = PayloadFrame.create_new(1, False, True, False, None, None)
            connection.queue_on_receiver_stream(cmpl)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()
        payloadReceived = Event()

        def assert_payload(response):
            self.assertEqual(response[0], metadata)
            self.assertEqual(response[1], data)
            payloadReceived.set()

        local_request_response(connection, [
            metadata, data
        ]).subscribe(on_next=lambda x: assert_payload(x), on_error=lambda err: self.fail(str(err)), on_completed=lambda: event.set())

        event.wait(1.0)
        self.assertTrue(event.is_set())
        self.assertTrue(payloadReceived.is_set())

    def test_error_response(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')
        error = "Hello World"

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_RESPONSE)
            ans = ErrorFrame.create_new(1, ErrorCodes.APPLICATION_ERROR, error)
            connection.queue_on_receiver_stream(ans)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()

        def handle_error(err):
            self.assertEqual(type(err), ApplicationError)
            self.assertEqual(str(err), error)
            event.set()

        local_request_response(connection,  [
            metadata, data
        ]).subscribe(on_next=lambda x: self.fail('Should emit error'), on_error=lambda err: handle_error(err), on_completed=lambda: self.fail('Completed but should result in error'))

        event.wait(1.0)
        self.assertTrue(event.is_set())
