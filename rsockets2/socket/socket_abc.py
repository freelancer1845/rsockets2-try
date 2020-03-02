
import queue


class Socket_ABC:

    def send_frame(self, data: bytes):
        pass

    def set_receive_handler(self, callback):
        pass

    def close(self):
        pass

    def get_recv_position(self) -> int:
        pass