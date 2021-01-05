import logging
import random
import re
import subprocess
import threading
import time
import unittest

import rx
import rx.operators as op
from rx.core.typing import Observable
from rx.scheduler.threadpoolscheduler import ThreadPoolScheduler
from rsockets2.core.exceptions import ApplicationError
from rsockets2.core.factories.rsocket_client import NoopHandler, rsocket_client
from rsockets2.core.factories.setup_config import RSocketSetupConfig
from rsockets2.core.rsocket import RSocket
from rsockets2.core.transport.tcp_transport import TcpClientTransport
from rsockets2.core.types import RequestPayload, ResponsePayload


class RSocketEchoServerTests(unittest.TestCase):

    logging.basicConfig(format='%(threadName)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

    TEST_PORT = 64052
    proc = None

    dispose_event = threading.Event()
    complete_event = threading.Event()

    def setUp(self) -> None:
        self.dispose_event.clear()
        self.complete_event.clear()
        print("Starting test server")
        RSocketEchoServerTests.proc = subprocess.Popen([
            'mvn',
            'exec:java',
            '-Dexec.mainClass=rsockets.python.tester.pythontester.BinarayRSocketTestService'
        ],
            cwd='python-tester',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        get_port = re.compile(r'L:\/\[0:0:0:0:0:0:0:0\]:(\d+)')
        while True:
            line = RSocketEchoServerTests.proc.stdout.readline().decode('UTF-8')
            matches = get_port.findall(line)
            if len(matches) > 0:
                RSocketEchoServerTests.TEST_PORT = int(matches[0])
                break
            proc_status = RSocketEchoServerTests.proc.poll()
            if proc_status != None:
                raise AssertionError('Failed to start test server')

    def tearDown(self) -> None:
        print("Killing test server")
        RSocketEchoServerTests.proc.kill()
        while RSocketEchoServerTests.proc.poll() == None:
            print("Waiting for process to die...")
            time.sleep(1.0)

    def test_can_keep_connection_alive(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=500)
        rsocket: RSocket = rsocket_client(
            transport,
            config,
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())
        while counter < 5:
            time.sleep(1.0)
            counter += 1
            if self.dispose_event.is_set():
                self.fail(
                    'RSocket disposed prematurely... Most likely keepalive failed')
        rsocket.dispose()

    def test_request_response_echo(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=5000)
        rsocket: RSocket = rsocket_client(
            transport,
            config,
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())
        # metadata = random.randbytes(random.randint(0, 8))
        # data = random.randbytes(random.randint(0, 8))
        metadata = "Hello Metadata".encode('UTF-8')
        data = "Hello Data".encode('UTF-8')

        def check_answer(ans: ResponsePayload):
            self.assertEqual(ans[0], metadata)
            self.assertEqual(ans[1], data)
        rsocket.request_response(
            (metadata, data)).subscribe(on_next=lambda x: check_answer(x), on_error=lambda err: self.fail(str(err)), on_completed=lambda: self.complete_event.set())
        self.complete_event.wait(10.0)
        if self.complete_event.is_set() == False:
            self.fail('Request did not complete')
        if self.dispose_event.is_set():
            self.fail(
                'RSocket disposed prematurely... Most likely keepalive failed')
        rsocket.dispose()

    def test_request_response_error(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=5000)
        rsocket: RSocket = rsocket_client(
            transport,
            config,
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())
        # metadata = random.randbytes(random.randint(0, 8))
        # data = random.randbytes(random.randint(0, 8))
        metadata = "Throw Error".encode('UTF-8')
        data = "What I want as error message".encode('UTF-8')

        def check_error(error: ApplicationError):
            self.assertEqual(str(error), "What I want as error message")
            self.complete_event.set()
        rsocket.request_response(
            (metadata, data)).subscribe(on_next=lambda x: self.fail('Expected an error'), on_error=lambda err: check_error(err), on_completed=lambda: self.fail('Expected an error'))
        self.complete_event.wait(1.0)
        if self.complete_event.is_set() == False:
            self.fail('Did not receive error...')
        if self.dispose_event.is_set():
            self.fail(
                'RSocket disposed prematurely... Most likely keepalive failed')
        rsocket.dispose()

    def test_request_stream_echo(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=1000)
        rsocket: RSocket = rsocket_client(
            transport,
            config,
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())
        count = 1000
        received = 0

        def stream_handler(ans: ResponsePayload):
            nonlocal received
            received += 1
            if received == count - 1:
                self.complete_event.set()

        response: ResponsePayload = rsocket.request_stream(
            (None, count.to_bytes(4, 'big'))).subscribe(lambda x: stream_handler(x))

        self.complete_event.wait(1.5)
        if self.complete_event.is_set() == False:
            self.fail('Did not receive all stream elements in time')
        rsocket.dispose()

    def test_request_stream_error(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=5000)
        rsocket: RSocket = rsocket_client(
            transport,
            config,
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())
        # metadata = random.randbytes(random.randint(0, 8))
        # data = random.randbytes(random.randint(0, 8))
        metadata = "Throw Error".encode('UTF-8')
        data = "What I want as error message".encode('UTF-8')

        def check_error(error: ApplicationError):
            self.assertEqual(str(error), "What I want as error message")
            self.complete_event.set()
        rsocket.request_stream(
            (metadata, data)).subscribe(on_next=lambda x: self.fail('Expected an error'), on_error=lambda err: check_error(err), on_completed=lambda: self.fail('Expected an error'))
        self.complete_event.wait(1.0)
        if self.complete_event.is_set() == False:
            self.fail('Did not receive error...')
        if self.dispose_event.is_set():
            self.fail(
                'RSocket disposed prematurely... Most likely keepalive failed')
        rsocket.dispose()

    def test_foregin_request_response(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=5000)
        metadata = "EchoRequestResponse".encode('UTF-8')
        data = "This is what I send".encode('UTF-8')

        class Handler(NoopHandler):
            complete = threading.Event()
            answer_payload = None

            def on_request_response(self, payload):
                self.answer_payload = payload
                self.complete.set()
                return rx.of((None, data))
        handler = Handler()
        rsocket: RSocket = rsocket_client(
            transport,
            config,
            handler=handler
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())

        rsocket.request_fnf((metadata, data))
        handler.complete.wait(1.0)
        if handler.complete.is_set() == False:
            self.fail('Did not receive complete...')
        self.assertEqual(handler.answer_payload[0], metadata)
        self.assertEqual(handler.answer_payload[1], data)

        def verify_replay(answer: ResponsePayload):
            self.assertEqual(answer[1], data)
        rsocket.request_response(("ReplayPayload".encode(
            'UTF-8'), None)).subscribe(on_next=lambda x: verify_replay(x), on_completed=lambda: self.complete_event.set())

        self.complete_event.wait(1.0)
        if self.complete_event.is_set() == False:
            self.fail('Failed to verify returned payload')
        if self.dispose_event.is_set():
            self.fail(
                'RSocket disposed prematurely... Most likely keepalive failed')
        rsocket.dispose()

    def test_foregin_request_stream(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=5000)
        metadata = "EchoRequestStream".encode('UTF-8')
        data = "This is what I send".encode('UTF-8')

        class Handler(NoopHandler):
            complete = threading.Event()
            request_payload = None
            initial_requests = 0

            def on_request_stream(self, payload: RequestPayload, initial_requests: int, requests: Observable[int]):
                self.request_payload = payload
                self.initial_requests = initial_requests
                return rx.interval(0.001, ThreadPoolScheduler(1)).pipe(
                    op.take(100),
                    op.map(lambda x: (None, x.to_bytes(4, 'big'))),
                    op.do_action(on_completed=lambda: self.complete.set())
                )

        handler = Handler()
        rsocket: RSocket = rsocket_client(
            transport,
            config,
            handler=handler
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())

        rsocket.request_fnf((metadata, data))
        handler.complete.wait(2.0)
        if handler.complete.is_set() == False:
            self.fail('Did not receive complete...')
        self.assertEqual(handler.request_payload[0], metadata)
        self.assertEqual(handler.request_payload[1], data)
        self.assertEqual(handler.initial_requests, 2147483647)

        def verify_replay(answer: ResponsePayload):
            self.assertEqual(int.from_bytes(answer[1], 'big'), 99)
        rsocket.request_response(("ReplayPayload".encode(
            'UTF-8'), None)).subscribe(on_next=lambda x: verify_replay(x), on_completed=lambda: self.complete_event.set())

        self.complete_event.wait(1.0)
        if self.complete_event.is_set() == False:
            self.fail('Failed to verify returned payload')
        if self.dispose_event.is_set():
            self.fail(
                'RSocket disposed prematurely... Most likely keepalive failed')
        rsocket.dispose()

    def test_foregin_request_fnf(self):
        transport = TcpClientTransport()
        transport.connect('localhost', self.TEST_PORT)
        config = RSocketSetupConfig(time_between_keepalive=5000)
        metadata = "EchoRequestFNF".encode('UTF-8')
        data = "This is what I send".encode('UTF-8')

        class Handler(NoopHandler):
            complete = threading.Event()
            request_payload = None

            def on_request_fnf(self, payload: RequestPayload) -> None:
                self.request_payload = payload
                self.complete.set()

        handler = Handler()
        rsocket: RSocket = rsocket_client(
            transport,
            config,
            handler=handler
        ).run()
        counter = 0
        rsocket.on_dispose.subscribe(lambda x: self.dispose_event.set())

        rsocket.request_fnf((metadata, data))
        handler.complete.wait(2.0)
        if handler.complete.is_set() == False:
            self.fail('Did not receive complete...')
        self.assertEqual(handler.request_payload[0], metadata)
        self.assertEqual(handler.request_payload[1], data)
        if self.dispose_event.is_set():
            self.fail(
                'RSocket disposed prematurely... Most likely keepalive failed')
        rsocket.dispose()


if __name__ == "__main__":
    with subprocess.Popen([
        'mvn',
        'exec:java',
        '-Dexec.mainClass=rsockets.python.tester.pythontester.BinarayRSocketTestService'
    ],
            cwd='../../python-tester',
            shell=True,
            stdout=subprocess.PIPE) as proc:
        while True:
            line = proc.stdout.readline().decode('UTF-8')
            if len(line.strip()) > 0:
                print(line.strip())
            if line.find('Bound new server') != -1:
                break

        proc.kill()
        print("Process killed")
