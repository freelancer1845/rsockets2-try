

from typing import Optional, Tuple

from rx.core.typing import Observable

ResponsePayload = Tuple[Optional[memoryview], Optional[memoryview]]
RequestPayload = Tuple[Optional[bytes], Optional[bytes]]


class RSocketHandler():

    def on_request_response(self, payload: RequestPayload) -> Observable[ResponsePayload]:
        pass

    def on_request_fnf(self, payload: RequestPayload) -> None:
        pass

    def on_request_stream(self, payload: RequestPayload, initial_requests: int, requests: Observable[int]) -> Observable[ResponsePayload]:
        pass

    def on_request_channel(self):
        pass
