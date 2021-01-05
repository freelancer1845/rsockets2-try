from rsockets2.core.types import ResponsePayload
import threading
from rsockets2.core.rsocket import RSocket
from rsockets2.core.factories.setup_config import RSocketSetupConfig
import unittest
import subprocess
import time
from rsockets2.core.factories.rsocket_client import rsocket_client
from rsockets2.core.transport.tcp_transport import TcpClientTransport
import logging
import random
import re


class RSocketEchoServerTests(unittest.TestCase):

    logging.basicConfig(format='%(threadName)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

    TEST_PORT = 38464
    proc = None

    dispose_event = threading.Event()
    complete_event = threading.Event()



    @classmethod
    def setUpClass(cls) -> None:
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
                cls.TEST_PORT = int(matches[0])
                break
            proc_status = RSocketEchoServerTests.proc.poll()
            if proc_status != None:
                raise AssertionError('Failed to start test server')

    @classmethod
    def tearDownClass(cls) -> None:
        print("Killing test server")
        RSocketEchoServerTests.proc.kill()
        while RSocketEchoServerTests.proc.poll() != None:
            print("Waiting for process to die...")
            time.sleep(1.0)

    def setUp(self) -> None:
        self.dispose_event.clear()
        self.complete_event.clear()

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
        self.complete_event.wait(1.0)
        if self.dispose_event.is_set():
            self.fail(
                'RSocket disposed prematurely... Most likely keepalive failed')

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
