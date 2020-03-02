import socket
import threading
import time
from queue import Queue
from enum import Enum, auto
import struct
from .socket_abc import Socket_ABC
from abc import abstractmethod


class RecvStatus(Enum):
    LENGTH_BYTE_0 = auto()
    LENGTH_BYTE_1 = auto()
    LENGTH_BYTE_2 = auto()
    READING_FRAME = auto()


class RTcpSocket(Socket_ABC):

    def __init__(self):
        super().__init__()

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_thread = threading.Thread(
            name="RTcpReceiver", daemon=True, target=self._recvloop)
        self._running = False
        self._hostname = ""
        self._port = 0
        self._recvstate = RecvStatus.LENGTH_BYTE_0

        self._recv_handler = None
        self._send_position = 0
        self._queue_send_position = 0
        self._recv_position = 0

        self._queue_lock = threading.Lock()

    def connect(self, hostname: str, port: int):
        self._hostname = hostname
        self._port = port
        self._queue_lock.acquire()
        try:
            self.socket.connect((self._hostname, self._port))
        finally:
            self._queue_lock.release()
        self._running = True
        self.recv_thread.start()

    @abstractmethod
    def set_receive_handler(self, callback):
        self._recv_handler = callback

    @abstractmethod
    def send_frame(self, data) -> int:
        position = 0
        self._queue_lock.acquire(True, 10.0)
        try:
            # self._send_queue.put(data)

            data_to_send = data
            frame_length = len(data_to_send)
            frame_length_bytes = bytearray(3)
            frame_length_bytes[0] = frame_length >> 16 & 0xFF
            frame_length_bytes[1] = frame_length >> 8 & 0xFF
            frame_length_bytes[2] = frame_length & 0xFF
            datasend = 0
            while True:
                datasend += self.socket.send(frame_length_bytes[datasend:])
                if datasend == 3:
                    break
            datasend = 0
            while True:
                datasend += self.socket.send(data_to_send[datasend:])
                if datasend == frame_length:
                    break

            self._send_position += frame_length
            position = self._send_position
        finally:
            self._queue_lock.release()
        return position

    @abstractmethod
    def close(self):
        self._running = False
        self.socket.close()

    @abstractmethod
    def get_recv_position(self) -> int:
        return self._recv_position

    def _recvloop(self):
        current_frame_length = 0
        current_frame = None
        data_read = 0
        read_buffer_size = 256

        while self._running:
            if read_buffer_size < 256:
                read_buffer_size = 256
            try:
                buffer = self.socket.recv(read_buffer_size)
            except ConnectionAbortedError as connectionAbortError:
                if self._running == False:
                    # This is expected as the socket should be closed
                    pass
                else:
                    raise connectionAbortError
            i = 0
            while i < len(buffer):
                # print("Reading value from buffer. State: {}".format(RecvStatus.READING_FRAME))
                if self._recvstate == RecvStatus.LENGTH_BYTE_0:
                    current_frame_length = (buffer[i] & 0xFF) << 16
                    i += 1
                    self._recvstate = RecvStatus.LENGTH_BYTE_1
                elif self._recvstate == RecvStatus.LENGTH_BYTE_1:
                    current_frame_length |= (buffer[i] & 0xFF) << 8
                    i += 1
                    self._recvstate = RecvStatus.LENGTH_BYTE_2
                elif self._recvstate == RecvStatus.LENGTH_BYTE_2:
                    current_frame_length |= (buffer[i] & 0xFF)
                    read_buffer_size = current_frame_length
                    current_frame = bytearray(current_frame_length)
                    data_read = 0
                    i += 1
                    self._recvstate = RecvStatus.READING_FRAME

                elif self._recvstate == RecvStatus.READING_FRAME:
                    # read frame in chunks
                    available = len(buffer) - i
                    if data_read + available > current_frame_length:
                        available = current_frame_length - data_read
                    current_frame[data_read:(
                        data_read + available)] = buffer[i:(i + available)]
                    i += available
                    data_read += available
                    read_buffer_size = current_frame_length - data_read
                    if data_read == current_frame_length:
                        if self._recv_handler != None:
                            self._recv_handler(current_frame)
                        self._recvstate = RecvStatus.LENGTH_BYTE_0
                        print("Frame Received")
