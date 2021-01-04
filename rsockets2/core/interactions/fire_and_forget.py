

from rsockets2.core.types import RequestPayload
from rsockets2.core.connection import DefaultConnection
from rsockets2.core.frames import RequestFNFFrame


def local_fire_and_forget(con: DefaultConnection, stream_id: int, data: RequestPayload):
    frame = RequestFNFFrame.create_new(stream_id, False, data[0], data[1])
    con.queue_frame(frame)
