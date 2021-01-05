

from typing import Callable, Union
from rsockets2.core.types import RequestPayload
from rsockets2.core.connection import DefaultConnection
from rsockets2.core.frames import RequestFNFFrame

import logging


log = logging.getLogger(__name__)


def foreign_fire_and_forget(request_frame: Union[bytes, bytearray, memoryview], handler: Callable[[RequestPayload], None]):
    try:
        if RequestFNFFrame.is_metdata_present(request_frame):
            handler(
                (RequestFNFFrame.metadata(request_frame),
                 RequestFNFFrame.data(request_frame))
            )
        else:
            handler(
                (None,
                 RequestFNFFrame.data(request_frame))
            )
    except Exception as err:
        log.error('Error while handling FNF Request', exc_info=True)


def local_fire_and_forget(con: DefaultConnection, data: RequestPayload):
    stream_id = con.stream_id_generator.new_stream_id()
    frame = RequestFNFFrame.create_new(stream_id, False, data[0], data[1])
    con.queue_frame(frame)
    con.stream_id_generator.free_stream_id(stream_id)
