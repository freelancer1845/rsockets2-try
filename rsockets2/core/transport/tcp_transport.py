import enum
from enum import IntEnum, auto, Enum
import socket
from typing import Callable, Union
from queue import Empty, Queue
import threading
from rsockets2.core.frames.segmented_frame import FrameSegments

from .abstract_transport import AbstractTransport


class RecvStatus(IntEnum):
    LENGTH_BYTE_0 = 3
    LENGTH_BYTE_1 = 2
    LENGTH_BYTE_2 = 1
    READING_FRAME = 0


class TcpClientTransport(AbstractTransport):

    def __init__(self) -> None:
        super().__init__()

        self._socket = None

        self._send_queue = Queue()
        self._recv_callback = lambda x: print("No Recv Callback Set")
        self._error_callback = lambda x: print("No Error Callback")
        self._running = True

    def connect(self, host: str, port: int):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(5.0)
        self._socket.connect((host, port))

    def handle(self, socket):
        self._socket = socket

    def _handle_error(self, error):
        if self._running == True:
            self._running = False
            self._error_callback(error)

    def open(self):
        self._recv_thread = threading.Thread(
            name="TcpClientTransportRecv", daemon=True, target=self._recv_loop)
        self._send_thread = threading.Thread(
            name="TcpClientTransportSend", daemon=True, target=self._send_loop, args=(self._socket, self._send_queue, self._error_callback))
        self._send_thread.start()
        self._recv_thread.start()

    def close(self):
        self._running = False
        if self._socket != None:
            self._socket.close()

    def send_frame(self, frame: FrameSegments):
        if self._running == False:
            raise IOError('Socket is closed')
        self._send_queue.put(frame)

    def set_on_receive_callback(self, cb: Callable[[Union[bytes, bytearray, memoryview]], None]):
        self._recv_callback = cb

    def set_on_error_callback(self, cb: Callable[[Exception], None]):
        self._error_callback = cb

    def _send_loop(self, socket, queue, error_callback):
        try:
            while self._running:
                try:
                    frame = queue.get(True, timeout=0.5)
                except Empty:
                    continue
                frame_length = frame.length
                frame_length_bytes = bytearray(3)
                frame_length_bytes[0] = frame_length >> 16 & 0xFF
                frame_length_bytes[1] = frame_length >> 8 & 0xFF
                frame_length_bytes[2] = frame_length & 0xFF
                datasend = 0
                while True:
                    datasend += socket.send(frame_length_bytes[datasend:])
                    if datasend == 3:
                        break
                for segment in frame:
                    datasend = 0
                    segment_length = len(segment)
                    while True:
                        datasend += socket.send(segment[datasend:])
                        if datasend == segment_length:
                            break
        except Exception as err:
            self._handle_error(err)

    def _recv_loop(self):
        data_read = 0
        current_frame_length = 0
        frame_length_buffer = bytearray(3)
        current_frame = bytearray(0)
        recv_status = RecvStatus.LENGTH_BYTE_0
        try:
            while self._running:
                if recv_status == RecvStatus.READING_FRAME:
                    data_received = self._socket.recv_into(
                        memoryview(current_frame)[data_read:])
                    data_read += data_received
                    if data_read == current_frame_length:
                        self._recv_callback(current_frame)
                        data_read = 0
                        recv_status = RecvStatus.LENGTH_BYTE_0

                else:
                    length_bytes_read = self._socket.recv_into(
                        frame_length_buffer, recv_status)
                    for i in range(length_bytes_read):
                        if recv_status == RecvStatus.LENGTH_BYTE_0:
                            current_frame_length = (
                                frame_length_buffer[i] & 0xFF) << 16
                            recv_status = RecvStatus.LENGTH_BYTE_1
                        elif recv_status == RecvStatus.LENGTH_BYTE_1:
                            current_frame_length |= (
                                frame_length_buffer[i] & 0xFF) << 8
                            recv_status = RecvStatus.LENGTH_BYTE_2
                        elif recv_status == RecvStatus.LENGTH_BYTE_2:
                            current_frame_length |= (
                                frame_length_buffer[i] & 0xFF)
                            if current_frame_length == 0:
                                recv_status = RecvStatus.LENGTH_BYTE_0
                            else:
                                recv_status = RecvStatus.READING_FRAME
                                current_frame = bytearray(current_frame_length)
                            break
        except Exception as err:
            self._handle_error(err)
