from abc import ABC, abstractmethod
from typing import Callable, Union

from rsockets2.core.frames import FrameSegments


class AbstractTransport(ABC):

    @abstractmethod
    def send_frame(self, frame: FrameSegments):
        pass

    @abstractmethod
    def set_on_receive_callback(self, cb: Callable[[Union[bytes, bytearray, memoryview]], None]):
        pass

    @abstractmethod
    def set_on_error_callback(self, cb: Callable[[Exception], None]):
        pass

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass
