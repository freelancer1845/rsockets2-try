

from rx.scheduler.newthreadscheduler import NewThreadScheduler
from rsockets2.core.factories.setup_config import RSocketSetupConfig
import unittest
import logging
import threading
import subprocess
import re
import time
from rsockets2.messages.message_handler import RSocketMessageHandler
import rx
import rx.operators as op
from rsockets2.core.factories.rsocket_client import rsocket_client
from rsockets2.core.transport.tcp_transport import TcpClientTransport

from rsockets2.common import str_codec


class MessageHandlerTests(unittest.TestCase):

    logging.basicConfig(format='%(threadName)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

    TEST_PORT = 61562
    proc = None

    dispose_event = threading.Event()
    complete_event = threading.Event()

    def setUp(self) -> None:
        self.dispose_event.clear()
        self.complete_event.clear()
        print("Starting test server")
        MessageHandlerTests.proc = subprocess.Popen([
            'mvn',
            'spring-boot:run',
        ],
            cwd='python-tester',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        get_port = re.compile(r'Netty RSocket started on port\(s\): (\d+)')
        while True:
            line = MessageHandlerTests.proc.stdout.readline().decode('UTF-8')
            matches = get_port.findall(line)
            if len(matches) > 0:
                MessageHandlerTests.TEST_PORT = int(matches[0])
                break
            proc_status = MessageHandlerTests.proc.poll()
            if proc_status != None:
                raise AssertionError('Failed to start test server')

    def tearDown(self) -> None:
        print("Killing test server")
        MessageHandlerTests.proc.kill()
        while MessageHandlerTests.proc.poll() == None:
            print("Waiting for process to die...")
            time.sleep(1.0)

    def test_request_response(self):
        handler = RSocketMessageHandler()

        def assert_route_called(payload):
            self.complete_event.set()
            return rx.empty()
        handler.router.register_request_response_route(
            '/route/request-response', assert_route_called)
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        rsocket = rsocket_client(
            transport=transport,
            setup_config=RSocketSetupConfig(
                time_between_keepalive=30000,
                max_lifetime=100000,
                metadata_encoding_mime_type='message/x.rsocket.composite-metadata.v0'),
            handler=handler
        ).run()
        completed = threading.Event()
        handler.request_response(
            '/route/request-response', (None, 'Hello World'.encode('UTF-8'))
        ).subscribe(on_error=lambda err: self.fail(str(err)), on_completed=lambda: completed.set())

        if completed.wait(2.0) == False:
            self.fail('Request Response did not complete')
        rsocket.dispose()

    def test_request_stream(self):
        handler = RSocketMessageHandler()
        test_payload = 'Hello World'.encode('UTF-8')
        request_count = 100

        def assert_route_called(payload, initial_requests, requests):
            self.assertEqual(test_payload, payload[1])
            self.complete_event.set()
            return rx.range(0, request_count, 1).pipe(op.map(lambda value: value.to_bytes(4, 'big')))
            # return rx.interval(0.001).pipe(op.take(request_count), op.map(lambda value: value.to_bytes(4, 'big')))
        handler.router.register_request_stream_route(
            '/route/request-stream', assert_route_called)
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        rsocket = rsocket_client(
            transport=transport,
            setup_config=RSocketSetupConfig(
                time_between_keepalive=30000,
                max_lifetime=100000,
                metadata_encoding_mime_type='message/x.rsocket.composite-metadata.v0'),
            handler=handler
        ).run()
        completed = threading.Event()

        def handle_error(err):
            print(err)
            self.fail(str(err))
        counter = 0

        def handle_next(x):
            nonlocal counter
            counter += 1
        handler.request_stream(
            '/route/request-stream', (None, test_payload)
        ).subscribe(on_next=handle_next, on_error=lambda err: handle_error(err), on_completed=lambda: completed.set(), scheduler=NewThreadScheduler())

        if completed.wait() == False:
            self.fail('Request Response did not complete')
        self.assertEqual(counter, request_count)
        rsocket.dispose()
