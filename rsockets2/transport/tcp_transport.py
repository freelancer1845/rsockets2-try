from .abstract_transport import AbstractTransport
import logging
import socket
from enum import Enum, auto


class RecvStatus(Enum):
    LENGTH_BYTE_0 = auto()
    LENGTH_BYTE_1 = auto()
    LENGTH_BYTE_2 = auto()
    READING_FRAME = auto()


class TcpTransport(AbstractTransport):

    def __init__(self, host: str, port: int):
        super().__init__()

        self._log = logging.getLogger("rsockets2.transport.TcpTransport")

        self._host = host
        self._port = port
        self._socket: socket.socket
        self._recv_status = RecvStatus.LENGTH_BYTE_0

        self._buffer = bytes(0)
        self._read_idx = 0
        self._data_read = 0
        self._current_frame_length = 0
        self._current_frame = None
        self._read_buffer_size = 3


    def connect(self):
        self._log.debug("Connecting to {}:{}".format(self._host, self._port))
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self._host, self._port))
        except OSError as error:
            # wrap os error into connection error
            raise ConnectionError(error)

    def disconnect(self):
        self._log.debug("Disconnecting socket at: {}:{}".format(
            self._host, self._port))
        if self._socket == None:
            raise ValueError(
                "Tried to disconnect a socket that was never created!")
        self._socket.close()

    def _send_bytes(self, frameBytes):
        if self._socket == None:
            raise ValueError(
                "Tried to send on a socket that was never created!")
        frame_length = len(frameBytes)
        frame_length_bytes = bytearray(3)
        frame_length_bytes[0] = frame_length >> 16 & 0xFF
        frame_length_bytes[1] = frame_length >> 8 & 0xFF
        frame_length_bytes[2] = frame_length & 0xFF
        datasend = 0
        while True:
            datasend += self._socket.send(frame_length_bytes[datasend:])
            if datasend == 3:
                break
        datasend = 0
        while True:
            datasend += self._socket.send(frameBytes[datasend:])
            if datasend == frame_length:
                break

    def _recv_bytes(self):
        if self._socket == None:
            raise ValueError(
                "Tried to receive on a socket that was never created!")
        while True:
            if self._read_idx == len(self._buffer):
                self._buffer = self._socket.recv(self._read_buffer_size)
                self._read_idx = 0

            while self._read_idx < len(self._buffer):
                if self._recv_status == RecvStatus.LENGTH_BYTE_0:
                    self._current_frame_length = (
                        self._buffer[self._read_idx] & 0xFF) << 16
                    self._read_idx += 1
                    self._recv_status = RecvStatus.LENGTH_BYTE_1
                elif self._recv_status == RecvStatus.LENGTH_BYTE_1:
                    self._current_frame_length |= (
                        self._buffer[self._read_idx] & 0xFF) << 8
                    self._read_idx += 1
                    self._recv_status = RecvStatus.LENGTH_BYTE_2
                elif self._recv_status == RecvStatus.LENGTH_BYTE_2:
                    self._current_frame_length |= (
                        self._buffer[self._read_idx] & 0xFF)
                    self._current_frame = bytearray(self._current_frame_length)
                    self._read_buffer_size = self._current_frame_length
                    self._data_read = 0
                    self._read_idx += 1
                    self._recv_status = RecvStatus.READING_FRAME

                elif self._recv_status == RecvStatus.READING_FRAME:
                    available = len(self._buffer) - self._read_idx
                    if self._data_read + available > self._current_frame_length:
                        available = self._current_frame_length - self._data_read
                    self._current_frame[self._data_read:(
                        self._data_read + available)] = self._buffer[self._read_idx:(self._read_idx + available)]
                    self._read_idx += available
                    self._data_read += available
                    if self._data_read == self._current_frame_length:
                        self._recv_status = RecvStatus.LENGTH_BYTE_0
                        self._read_buffer_size = 3
                        return self._current_frame
                    else:
                        self._read_buffer_size = self._current_frame_length - self._data_read
