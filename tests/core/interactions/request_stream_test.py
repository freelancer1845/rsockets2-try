
from rsockets2.core.exceptions import ApplicationError
from rsockets2.core.frames.request_n import RequestNFrame
from rx.subject.subject import Subject
from rsockets2.core.frames.error import ErrorCodes, ErrorFrame
from rsockets2.core.frames.payload import PayloadFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rsockets2.core.frames import RequestStreamFrame
from tests.core.interactions.default_connection_mock import DefaultConnectionMock
from rsockets2.core.interactions.request_stream import local_request_stream
import unittest
from threading import Event, Thread
import rx.operators as op
from rx.scheduler.threadpoolscheduler import ThreadPoolScheduler


class RequestStreamTest(unittest.TestCase):

    def test_empty_response(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_STREAM)
            self.assertTrue(
                RequestStreamFrame.initial_request_n(request), 2**31 - 1)
            ans = PayloadFrame.create_new(1, False, True, False, None, None)
            connection.queue_on_receiver_stream(ans)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()

        local_request_stream(connection,  [
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
                request)[1] == FrameType.REQUEST_STREAM)
            self.assertTrue(
                RequestStreamFrame.initial_request_n(request), 2**31 - 1)
            ans = PayloadFrame.create_new(
                1, False, True, True, metadata, data)
            connection.queue_on_receiver_stream(ans)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()
        payloadReceived = Event()

        def assert_payload(response):
            self.assertEqual(response[0], metadata)
            self.assertEqual(response[1], data)
            payloadReceived.set()

        local_request_stream(connection,  [
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
                request)[1] == FrameType.REQUEST_STREAM)
            self.assertTrue(
                RequestStreamFrame.initial_request_n(request), 2**31 - 1)
            ans = PayloadFrame.create_new(
                1, False, False, True, metadata, data)
            connection.queue_on_receiver_stream(ans)
            cmpl = PayloadFrame.create_new(
                1, False, True, False, None, None)
            connection.queue_on_receiver_stream(cmpl)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()
        payloadReceived = Event()

        def assert_payload(response):
            self.assertEqual(response[0], metadata)
            self.assertEqual(response[1], data)
            payloadReceived.set()

        local_request_stream(connection,  [
            metadata, data
        ]).subscribe(on_next=lambda x: assert_payload(x), on_error=lambda err: self.fail(str(err)), on_completed=lambda: event.set())

        event.wait(1.0)
        self.assertTrue(event.is_set())
        self.assertTrue(payloadReceived.is_set())

    def test_stream_multiple(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_STREAM)
            self.assertTrue(
                RequestStreamFrame.initial_request_n(request), 2**31 - 1)
            for i in range(100):
                ans = PayloadFrame.create_new(
                    1, False, False, True, metadata, i.to_bytes(4, 'big'))
                connection.queue_on_receiver_stream(ans)
            cmpl = PayloadFrame.create_new(
                1, False, True, False, None, None)
            connection.queue_on_receiver_stream(cmpl)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()
        current = 0

        def assert_payload(response):
            nonlocal current
            self.assertEqual(response[0], metadata)
            self.assertEqual(int.from_bytes(response[1], 'big'), current)
            current += 1

        local_request_stream(connection,  [
            metadata, data
        ]).subscribe(on_next=lambda x: assert_payload(x), on_error=lambda err: self.fail(str(err)), on_completed=lambda: event.set())

        event.wait(1.0)
        self.assertTrue(event.is_set())
        self.assertEqual(current, 100)

    def test_backpressure(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')

        send_answers = 0

        def server_handle(request: bytes):
            nonlocal send_answers
            if FrameHeader.stream_id_type_and_flags(
                    request)[1] == FrameType.REQUEST_STREAM:
                self.assertEqual(
                    RequestStreamFrame.initial_request_n(request), 0)
            elif FrameHeader.stream_id_type_and_flags(
                    request)[1] == FrameType.REQUEST_N:
                n = RequestNFrame.request_n(request)
                for i in range(n):
                    ans = PayloadFrame.create_new(
                        1, False, False, True, metadata, send_answers.to_bytes(4, 'big'))
                    send_answers += 1
                    connection.queue_on_receiver_stream(ans)
                    if send_answers == 100:
                        cmpl = PayloadFrame.create_new(
                            1, False, True, False, None, None)
                        connection.queue_on_receiver_stream(cmpl)

        connection.listen_on_sender_stream(1).pipe(
            op.observe_on(ThreadPoolScheduler())
        ).subscribe(
            lambda x: server_handle(x))

        event = Event()
        current = 0

        requester = Subject()

        def assert_payload(response):
            nonlocal current
            self.assertEqual(response[0], metadata)
            self.assertEqual(int.from_bytes(response[1], 'big'), current)
            current += 1
            if current < 100:
                requester.on_next(1)

        local_request_stream(connection, [
            metadata, data
        ], 0, requester).subscribe(on_next=lambda x: assert_payload(x), on_error=lambda err: self.fail(str(err)), on_completed=lambda: event.set())

        requester.on_next(10)
        event.wait(1.0)
        self.assertTrue(event.is_set())
        self.assertEqual(current, 100)

    def test_error_response(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')
        error = "Hello World"

        def server_handle(request: bytes):
            self.assertTrue(FrameHeader.stream_id_type_and_flags(
                request)[1] == FrameType.REQUEST_STREAM)
            ans = ErrorFrame.create_new(1, ErrorCodes.APPLICATION_ERROR, error)
            connection.queue_on_receiver_stream(ans)

        connection.listen_on_sender_stream(1).subscribe(
            lambda x: server_handle(x))

        event = Event()

        def handle_error(err):

            self.assertEqual(type(err), ApplicationError)
            self.assertEqual(str(err), 'Hello World')
            event.set()

        local_request_stream(connection, [
            metadata, data
        ]).subscribe(on_next=lambda x: self.fail('Should emit error'), on_error=lambda err: handle_error(err), on_completed=lambda: self.fail('Completed but should result in error'))

        event.wait(1.0)
        self.assertTrue(event.is_set())

    def test_premature_unsubscribe_send_cancel(self):
        connection = DefaultConnectionMock()
        metadata = "Metadata".encode('UTF-8')
        data = "Data".encode('UTF-8')
        cancel_received = Event()

        def server_handle(request: bytes):
            if FrameHeader.stream_id_type_and_flags(request)[1] == FrameType.REQUEST_STREAM:
                self.assertTrue(
                    RequestStreamFrame.initial_request_n(request), 2**31 - 1)
                for i in range(100):
                    ans = PayloadFrame.create_new(
                        1, False, False, True, metadata, i.to_bytes(4, 'big'))
                    connection.queue_on_receiver_stream(ans)
                cmpl = PayloadFrame.create_new(
                    1, False, True, False, None, None)
                connection.queue_on_receiver_stream(cmpl)
            elif FrameHeader.stream_id_type_and_flags(request)[1] == FrameType.CANCEL:
                cancel_received.set()
            else:
                self.fail('Unexpected frame')

        connection.listen_on_sender_stream(1).pipe(op.observe_on(ThreadPoolScheduler())).subscribe(
            lambda x: server_handle(x))

        event = Event()

        local_request_stream(connection, [
            metadata, data
        ]).pipe(
            op.take(20)
        ).subscribe(on_completed=lambda: event.set())

        event.wait(1.0)
        cancel_received.wait(1.0)
        self.assertTrue(event.is_set())
        self.assertTrue(cancel_received.is_set())
