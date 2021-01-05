

from typing import Optional, Tuple, Union

from rsockets2.core.frames.segmented_frame import FrameSegments
from rx.core.typing import Observable

RequestPayloadTypes = Optional[Union[bytes,
                                     bytearray, memoryview, FrameSegments]]

ResponsePayload = Tuple[Optional[memoryview], Optional[memoryview]]
RequestPayload = Tuple[RequestPayloadTypes, RequestPayloadTypes]


class RSocketHandler():

    def on_request_response(self, payload: RequestPayload) -> Observable[ResponsePayload]:
        pass

    def on_request_fnf(self, payload: RequestPayload) -> None:
        pass

    def on_request_stream(self, payload: RequestPayload, initial_requests: int, requests: Observable[int]) -> Observable[ResponsePayload]:
        pass

    def on_request_channel(self):
        pass
