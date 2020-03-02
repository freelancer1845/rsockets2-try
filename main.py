import socket
from rsockets2.socket.tcp_socket import RTcpSocket
from rsockets2.frames.parser import FrameParser
from rsockets2.frames.setup import SetupFrame
from rsockets2.frames.error import ErrorFrame
from rsockets2.frames.payload import Payload
from rsockets2.frames.request_stream import RequestStream
from rsockets2.frames.request_response import RequestResponse
from rsockets2.frames.request_n import RequestNFrame
from rsockets2 import RSocket, SocketType
import queue
import logging
import time
import json
import rx
import struct

logging.basicConfig(level=logging.DEBUG)


if __name__ == "__main__":

    socket = RSocket(socket_type=SocketType.TCP_SOCKET, keepalive=10000, maxlive=10000,
                     hostname='localhost', port=24512)

    def request_response_handler(frame: RequestResponse):
        print("Request response received")
        return rx.throw(Exception("Test Exception"))
        # return rx.just("100".encode('ASCII'))
    socket.on_request_response = request_response_handler
    try:
        socket.open()
        time.sleep(1.0)

        total_size = 0

        def adder(x):
            global total_size
            total_size += len(x)
        start = time.time()

        def on_complete():
            global start
            needed = time.time() - start
            print("Received --- {}mb Needed: {}s Speed: {}mb/s".format(total_size /
                                                                       1000000, needed, total_size / 1000000 / needed))
        # socket.request_response(meta_data=b'test.bigdata', data=bytes(0)).subscribe(on_next=lambda x: adder(
            # x), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=on_complete)
        # print("Received --- {}mb".format(total_size / 1000000))
        # for i in range(10):
            # socket.request_stream(meta_data=b'test.bigdatas', data=bytes(0)).subscribe(on_next=lambda x: print(
            # "Received Size: {} mb".format(len(x) / 1000000.0)), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=lambda: print("Complete"))
        # time.sleep(30)
        socket.request_stream(meta_data=b'test.bigdatas', data=bytes(0)).subscribe(on_next=lambda x: print(
            "Received Size: {} mb".format(len(x) / 1000000.0)), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=lambda: print("Complete"))
        # request_data = {
        #     "delay": 100,
        #     "count": 10,
        #     "text": "Ohhh",
        # }
        # socket.request_stream(meta_data=b'test.controller', data=json.dumps(request_data)).subscribe(on_next=lambda x: print(
        #     "Next: {}".format(x.decode('UTF-8'))), on_error=lambda err: err, on_completed=lambda: print("Complete"))
        # request_data = {
        #     "delay": 200,
        #     "count": 10,
        #     "text": "Eyyyy",
        # }
        # socket.request_stream(meta_data=b'test.controller', data=json.dumps(request_data)).subscribe(on_next=lambda x: print(
        #     "Next: {}".format(x.decode('UTF-8'))), on_error=lambda err: err, on_completed=lambda: print("Complete"))
        while True:
            time.sleep(1.0)
    finally:
        socket.close()
