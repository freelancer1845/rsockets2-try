from abc import ABC, abstractmethod


class Frame_ABC(ABC):

    def __init__(self):
        super().__init__()
        self.stream_id = 0

    @abstractmethod
    def to_bytes(self):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        raise NotImplementedError()
