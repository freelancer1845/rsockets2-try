

from rsockets2.extensions.wellknown_mime_types import WellknownMimeTypes
from rsockets2.core.factories.setup_config import RSocketSetupConfig
import unittest
import logging
import threading
import subprocess
import re
import time
from rsockets2.messages.message_handler import RSocketMessageHandler
import rx
from rsockets2.core.factories.rsocket_client import rsocket_client
from rsockets2.core.transport.tcp_transport import TcpClientTransport

from rsockets2.common import str_codec


class MessageHandlerTests(unittest.TestCase):

    logging.basicConfig(format='%(threadName)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

    TEST_PORT = 61562
    proc = None

    dispose_event = threading.Event()
    complete_event = threading.Event()

    def setUpa(self) -> None:
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
            if len(line.strip()):
                print(line)
            if len(matches) > 0:
                MessageHandlerTests.TEST_PORT = int(matches[0])
                break
            proc_status = MessageHandlerTests.proc.poll()
            if proc_status != None:
                raise AssertionError('Failed to start test server')

    def tearDowna(self) -> None:
        print("Killing test server")
        MessageHandlerTests.proc.kill()
        while MessageHandlerTests.proc.poll() == None:
            print("Waiting for process to die...")
            time.sleep(1.0)

    def test_integration_setup(self):
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

        if completed.wait() == False:
            self.fail('Request Response did not complete')
