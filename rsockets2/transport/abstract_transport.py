from abc import ABC, abstractmethod
from ..frames import Frame_ABC, FrameParser
import logging
import threading
import typing


class AbstractTransport(ABC):

    def __init__(self):
        super().__init__()

        self._log = logging.getLogger("rsockets2.transport.AbstractTransport")

        self._send_lock = threading.Lock()
        self._parser = FrameParser()

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def _recv_bytes(self) -> typing.Union[bytes, bytearray, memoryview]:
        pass

    @abstractmethod
    def _send_bytes(self, frameBytes: bytes):
        pass

    def send_frame(self, frame: Frame_ABC):
        """
            The method is not thread safe! It will block until the method is sent or an error is thrown.
            Concurrent access will throw an Exception!
            Exceptions thrown by the socket need to be handled by the caller!
        """
        success = self._send_lock.acquire(blocking=False)
        if success == True:
            try:
                data = frame.to_bytes()
                logging.debug("Sending Frame: Length '{}' [bytes]. Type: '{}'. StreamID: '{}'".format(
                    len(data), frame.__class__.__name__, frame.stream_id))
                self._send_bytes(data)
            finally:
                self._send_lock.release()
        else:
            raise RuntimeError(
                "The send_lock is already acquired. It is the application designers responsibility to make sure the send_frame method is thread safe!")

    def recv_frame(self) -> Frame_ABC:
        frame_bytes = self._recv_bytes()
        frame = self._parser.parseFrame(frame_bytes)
        logging.debug("Received Frame: Length '{}' [bytes]. Type: '{}' StreamID: '{}'.".format(
            len(frame_bytes), frame.__class__.__name__, frame.stream_id))
        return frame
