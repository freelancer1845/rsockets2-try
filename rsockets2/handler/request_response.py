import rx
from rsockets2.socket import Socket_ABC
import rsockets2.frames as frames


class RequestResponseHandler(object):
    def __init__(self, stream_id: int, socket: Socket_ABC):
        super().__init__()
        self.stream_id = stream_id
        self.socket = socket

    def on_next(self, value):
        answer = frames.Payload()
        answer.stream_id = self.stream_id
        answer.follows = False
        answer.complete = True
        answer.next_present = True
        answer.payload = value
        answer.meta_data = bytes(0)
        self.socket.send_frame(answer)

    def on_error(self, error):
        error_frame = frames.ErrorFrame()
        error_frame.stream_id = self.stream_id
        error_frame.error_code = frames.ErrorCodes.APPLICATION_ERROR
        if isinstance(error, Exception):
            error_frame.error_data = str(error).encode("ASCII")
        else:
            error_frame.error_data = error
        self.socket.send_frame(error_frame)

    def on_completed(self):
        pass
