from abc import ABC, abstractmethod
import threading
import rx
import rx.operators as op
from ..frames import Frame_ABC


class AbstractConnection(ABC):

    def __init__(self):
        super().__init__()

        self._send_position = 0
        self._last_recv_position = 0
        self._recv_lock = threading.Lock()
        self._send_lock = threading.Lock()

        self._last_stream_id = 0
        self._used_stream_ids = []
        self._stream_id_lock = threading.Lock()

        self.token = bytes(0)

    @abstractmethod
    def queue_frame(self, frame: Frame_ABC):
        pass

    @abstractmethod
    def recv_observable(self):
        pass

    @abstractmethod
    def destroy_observable(self):
        pass

    def recv_observable_filter_type(self, frame_type):
        return self.recv_observable().pipe(
            op.filter(lambda frame: isinstance(frame, frame_type))
        )

    @property
    def last_received_position(self):
        with self._recv_lock:
            return self._last_recv_position

    def increase_recv_position(self, value: int) -> int:
        with self._recv_lock:
            self._last_recv_position += value
            return self._last_recv_position

    @property
    def send_position(self):
        with self._send_lock:
            return self._send_position

    def increase_send_position(self, value: int) -> int:
        with self._send_lock:
            self._send_position += value
            return self._send_position

    def get_new_stream_id(self) -> int:
        self._stream_id_lock.acquire(blocking=True)
        while True:
            if self._last_stream_id >= 2_147_483_647:
                self._last_stream_id = 0
            if self._last_stream_id == 0:
                stream_id = 1
                self._last_stream_id = 1
            else:
                self._last_stream_id += 2
                stream_id = self._last_stream_id
            if stream_id not in self._used_stream_ids:
                self._used_stream_ids.append(stream_id)
                break
        self._stream_id_lock.release()

        return stream_id

    def free_stream_id(self, stream_id: int):
        self._used_stream_ids.remove(stream_id)
