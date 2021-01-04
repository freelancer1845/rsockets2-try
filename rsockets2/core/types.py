

from typing import Optional, Tuple


class RSocketHandler():

    def on_request_response(self):
        pass

    def on_request_fnf(self):
        pass

    def on_request_stream(self):
        pass

    def on_request_channel(self):
        pass


ResponsePayload = Tuple[Optional[memoryview], Optional[memoryview]]
RequestPayload = Tuple[Optional[bytes], Optional[bytes]]