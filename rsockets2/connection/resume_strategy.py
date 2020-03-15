from abc import ABC, abstractmethod
from ..frames import Frame_ABC, KeepAliveFrame
from .abstract_connection import AbstractConnection
import rx.operators as op
import threading


class ResumeStrategy(ABC):

    def __init__(self):
        super().__init__()

    @abstractmethod
    def create_resume_frame(self):
        pass

    @abstractmethod
    def push_send_frame(self, pos: int, frame: Frame_ABC):
        pass

    @abstractmethod
    def push_keepalive_frame(self, frame: KeepAliveFrame):
        pass


class ExponentialBackoffResumeStrategy(ResumeStrategy):

    max_store_size = 1000

    def __init__(self):
        super().__init__()
        self._send_store = []

        self._store_lock = threading.Lock()


    def push_send_frame(self, pos: int, frame: Frame_ABC):
        with self._store_lock:
            self._send_store.append((pos, frame))
            if len(self._send_store) > self.max_store_size:
                self._send_store.pop(0)

    def push_keepalive_frame(self, frame: KeepAliveFrame):
        foreign_pos = frame.last_received_position
        to_be_removed = []
        with self._store_lock:
            for idx, element in enumerate(self._send_store):
                if element[0] < foreign_pos:
                    to_be_removed.append(idx)
            for idx in to_be_removed:
                self._send_store.pop(idx)
