import socket
import threading
import time
from queue import Queue
from enum import Enum, auto
import struct
from .socket_abc import Socket_ABC
from rsockets2.frames import Frame_ABC
from abc import abstractmethod
import logging


class RecvStatus(Enum):
    LENGTH_BYTE_0 = auto()
    LENGTH_BYTE_1 = auto()
    LENGTH_BYTE_2 = auto()
    READING_FRAME = auto()


class RTcpSocket(Socket_ABC):

    def __init__(self, resume_support):
        super().__init__(resume_support)
        self._log = logging.getLogger("rsockets2.socket.RTcpSocket")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_thread = threading.Thread(
            name="RTcpReceiver", daemon=True, target=self._recvloop)
        self._running = False
        self._hostname = ""
        self._port = 0
        self._recvstate = RecvStatus.LENGTH_BYTE_0

        self._socket_closed = True

        self._queue_lock = threading.Lock()

    def connect(self, hostname: str, port: int):
        self._hostname = hostname
        self._port = port
        self._queue_lock.acquire()
        try:
            self.socket.connect((self._hostname, self._port))
            self._socket_closed = False
        finally:
            self._queue_lock.release()
        self._running = True
        self.recv_thread.start()

    def _send_frame_internal(self, data: bytes):
        try:
            if self._socket_closed:
                raise IOError("Socket closed!")
            frame_length = len(data)
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
                datasend += self.socket.send(data[datasend:])
                if datasend == frame_length:
                    break
        except IOError as ioError:
            self._signal_socket_closed(ioError)

    def _close_internal(self):
        self._running = False
        self.socket.close()

    def _recvloop(self):
        current_frame_length = 0
        current_frame = None
        data_read = 0
        read_buffer_size = 256
        try:
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
                        print("Reading Frame With Size: {}".format(current_frame_length))
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
                            self._receive_handler_bytes(current_frame)
                            self._recvstate = RecvStatus.LENGTH_BYTE_0
        except IOError as err:
            self._signal_socket_closed(err)

    def _signal_socket_closed(self, error):
        self._socket_closed = True
        if self._running == False:
            # Socket being closed is expected
            return
        self._running = True
        self._socket_closed_handler(error)
