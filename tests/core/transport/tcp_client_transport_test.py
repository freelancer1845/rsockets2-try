from rsockets2.core.frames.segmented_frame import FrameSegments
import unittest
from rsockets2.core.transport.tcp_transport import TcpClientTransport
import socket
import threading


class TcpClientTransportTest(unittest.TestCase):

    def test_send_frame(self):
        client_received = threading.Event()

        class Server(object):
            def __init__(self) -> None:
                threading.Thread(name="TestServer", daemon=True,
                                 target=self._loop).start()

            def _loop(self):
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("127.0.0.1", 34235))
                    s.listen()
                    conn, addr = s.accept()
                    with conn:
                        data = bytearray(4195304 + 3)
                        data[0] = 0x40
                        data[1] = 0x03
                        data[2] = 0xE8
                        conn.sendall(data)

        server = Server()
        transport = TcpClientTransport()
        transport.connect("localhost", 34235)

        def handle(frame: bytes):
            self.assertEqual(len(frame), 4195304)
            client_received.set()
        transport.set_on_receive_callback(handle)
        transport.open()
        client_received.wait(10.0)
        self.assertTrue(client_received.is_set())

    def test_recv_frame(self):
        server_received = threading.Event()

        class Server(object):
            def __init__(self) -> None:
                threading.Thread(name="TestServer", daemon=True,
                                 target=self._loop).start()

            def _loop(self):
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("127.0.0.1", 34235))
                    s.listen()
                    conn, addr = s.accept()
                    with conn:
                        received = 0
                        while True:
                            data = conn.recv(512)
                            received += len(data)
                            if received == 1000 + 3:
                                server_received.set()
                                break

        server = Server()
        transport = TcpClientTransport()
        transport.connect("localhost", 34235)
        transport.open()
        frame = FrameSegments()
        data = bytes(500)
        frame.append_fragment(data)
        frame.append_fragment(data)
        transport.send_frame(frame)
        server_received.wait(5)
        self.assertTrue(server_received.is_set())
