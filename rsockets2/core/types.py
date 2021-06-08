

from typing import Optional, Tuple, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from .rsocket import RSocket
    from rsockets2.core.frames.segmented_frame import FrameSegments

from rx.core.typing import Observable
from abc import ABC, abstractmethod


WriteBuffer = Union[bytearray, memoryview]
ReadBuffer = Union[bytes, memoryview, bytearray]

RequestPayloadTypes = Optional[Union[ReadBuffer, 'FrameSegments']]

ResponsePayload = Tuple[Optional[memoryview], Optional[memoryview]]
'''
    A request payload typically consists of [metadata, data]
'''
RequestPayload = Tuple[RequestPayloadTypes, RequestPayloadTypes]


class RSocketHandler():

    rsocket: 'RSocket' = None

    def set_rsocket(self, rsocket):
        self.rsocket = rsocket

    @abstractmethod
    def on_request_response(self, payload: RequestPayload) -> Observable[ResponsePayload]:
        pass

    @abstractmethod
    def on_request_fnf(self, payload: RequestPayload) -> None:
        pass

    @abstractmethod
    def on_request_stream(self, payload: RequestPayload, initial_requests: int, requests: Observable[int]) -> Observable[ResponsePayload]:
        pass

    @abstractmethod
    def on_request_channel(self):
        pass
