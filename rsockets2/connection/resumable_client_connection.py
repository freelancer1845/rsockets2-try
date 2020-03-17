from .abstract_connection import AbstractConnection
from ..transport import AbstractTransport
import string
import random
import enum
import logging
import rx
import rx.subject
import rx.operators as op
import rx.scheduler
from queue import PriorityQueue, Queue
import threading
from .keepalive_support import KeepaliveSupport
from ..frames import SetupFrame, ResumeFrame, ResumeOkFrame, ErrorFrame, KeepAliveFrame, ErrorCodes, PositionRelevantFrames
from ..common import RSocketConfig
import time


class ConnectionState(enum.IntEnum):
    CONNECTED = enum.auto()
    RESUMING = enum.auto()
    DISCONNECTED = enum.auto()


class SendCacheEntry(object):

    def __init__(self, pos, time, frame):
        super().__init__()
        self.pos = pos
        self.time = time
        self.frame = frame


class PriorityEntry(object):

    def __init__(self, priority, data):
        self.data = data
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority


class ResumableClientConnection(AbstractConnection):

    token_length = 8 * 16

    def __init__(self, transport: AbstractTransport, config: RSocketConfig):
        super().__init__()
        if config.resume_support == False:
            raise ValueError(
                "Tried to create a ResumableClientConnection without noting resume_support in config!")
        self._log = logging.getLogger(
            "rsockets2.connection.ResumableClientConnection")

        self._scheduler = rx.scheduler.ThreadPoolScheduler(20)
        self._config = config
        self._transport = transport
        self._state = ConnectionState.RESUMING
        self._resume_times = 0
        self._token = None

        self._destroy_publisher = rx.subject.Subject()
        self._recv_publisher = rx.subject.Subject()

        self._send_queue = PriorityQueue()
        self._send_cache = []
        self._send_cache_lock = threading.RLock()

        self._send_and_control_thread = threading.Thread(
            name="RSocketResumableConnectionMain", daemon=True, target=self._send_and_control_loop)
        self._recv_thread = threading.Thread(
            name="RSocketResumableConnectionRecv", daemon=True, target=self._recv_loop)
        self._keepalive_support = None

        self._state_change_condition = threading.Condition()

        self._recv_thread_in_resume_event = threading.Event()

    def queue_frame(self, frame):
        if frame.stream_id == 0:
            self._send_queue.put(PriorityEntry(10, frame))
        else:
            self._send_queue.put(PriorityEntry(100, frame))

    def recv_observable(self):
        return self._recv_publisher.pipe(
            op.observe_on(self._scheduler),
            op.take_until(self.destroy_observable()),
            op.subscribe_on(self._scheduler))

    def destroy_observable(self):
        return self._destroy_publisher

    def open(self):
        self._create_error_logger()
        self._transport.connect()
        self._negotiate_connection()
        self._send_and_control_thread.start()
        self._recv_thread.start()

        self._keepalive_support = KeepaliveSupport(self, self._config)
        self._keepalive_support.start()

        self.recv_observable_filter_type(
            KeepAliveFrame).pipe(
                op.throttle_first(1.0),
                op.take_until(self._destroy_publisher),
                op.map(lambda frame: frame.last_received_position)
        ).subscribe(on_next=lambda x: self._clean_send_cache(last_received_position=x))

    def close(self):
        self._change_state(ConnectionState.DISCONNECTED)
        if self._keepalive_support != None:
            self._keepalive_support.stop()

        self._destroy_publisher.on_completed()
        try:
            self._transport.disconnect()
        except Exception as error:
            self._log.debug(
                "Silent exception while closing transport on resume", exc_info=True)

    def _try_resume(self):
        while self._state == ConnectionState.RESUMING:
            try:
                self._transport.disconnect()
            except Exception as error:
                self._log.debug(
                    "Silent exception while closing transport on resume", exc_info=True)
            time.sleep(1.0)
            try:
                self._transport.connect()

                resumeFrame = ResumeFrame.from_config(
                    self._config, self._token, self.last_received_position, self._send_cache[0].pos)
                self._transport.send_frame(resumeFrame)
                answer = self._transport.recv_frame()
                if isinstance(answer, ResumeOkFrame):
                    self._log.debug("Sending cached frames")
                    for cached_frame in self._send_cache:
                        if cached_frame.pos > answer.last_received_client_position:
                            self._transport.send_frame(cached_frame.frame)
                    self._log.info(
                        "Successfully resumed connection using token: {}".format(self._token))
                    self._change_state(ConnectionState.CONNECTED)
                elif isinstance(answer, ErrorFrame):
                    self._log.info(
                        "Resume Failed! ErrorCode: {} - ErrorData: {}".format(answer.error_code.name, answer.error_data))
                    self._change_state(ConnectionState.DISCONNECTED)
                else:
                    self._log.error(
                        "Unexpected answer while resuming: {}".format(answer))
                    self._change_state(ConnectionState.DISCONNECTED)
            except (ConnectionError, TimeoutError) as error:
                self._log.debug(
                    "Connection error while resuming. Trying again in 1s ...", exc_info=True)
            except Exception as error:
                self._log.error(
                    "Unexpected exception while resuming.", exc_info=True)
                self._change_state(ConnectionState.DISCONNECTED)

    def _negotiate_connection(self):
        setupFrame = SetupFrame.from_config(self._config)
        self._token = self._generate_token()
        setupFrame.resume_identification_token = self._token
        self._transport.send_frame(setupFrame)
        self._change_state(ConnectionState.CONNECTED)

    def _send_and_control_loop(self):
        while self._state != ConnectionState.DISCONNECTED:
            if self._state == ConnectionState.RESUMING:
                try:
                    self._recv_thread_in_resume_event.wait(timeout=5)
                except TimeoutError as error:
                    self._log.debug("Waiting for RecvThread timed out!")
                    self._log.error(
                        "Resuming connection failed... internal error")
                    self._change_state(ConnectionState.DISCONNECTED)
                self._try_resume()

            elif self._state == ConnectionState.CONNECTED:
                try:
                    frame = self._send_queue.get().data

                    if isinstance(frame, PositionRelevantFrames):
                        pos = self.increase_send_position(len(frame))

                        with self._send_cache_lock:
                            self._send_cache.append(
                                SendCacheEntry(pos, time.time(), frame))

                    self._transport.send_frame(frame)
                except (ConnectionError, OSError) as error:
                    if self._state == ConnectionState.CONNECTED:
                        self._log.debug(
                            "Connection Error - Trying to resume: {}".format(error))
                        self._change_state(ConnectionState.RESUMING)
                    elif self._state == ConnectionState.DISCONNECTED:
                        self._log.debug(
                            "Silent socket error because its already closed.", exc_info=True)
                    elif self._state == ConnectionState.RESUMING:
                        self._log.debug(
                            "Exception in send_loop. But already in RESUMING state. Silencing exception!")

    def _recv_loop(self):
        try:
            while self._state != ConnectionState.DISCONNECTED:
                if self._state == ConnectionState.CONNECTED:
                    try:
                        frame = self._transport.recv_frame()
                        if isinstance(frame, PositionRelevantFrames):
                            self.increase_recv_position(len(frame))

                        self._recv_publisher.on_next(frame)
                    except (ConnectionError, OSError) as error:

                        if self._state == ConnectionState.CONNECTED:
                            self._log.debug(
                                "Connection Error - Trying to resume: {}".format(error))
                            self._change_state(ConnectionState.RESUMING)
                        elif self._state == ConnectionState.DISCONNECTED:
                            self._log.debug(
                                "Silent socket error because its already closed.", exc_info=True)
                        elif self._state == ConnectionState.RESUMING:
                            self._log.debug(
                                "Exception in recvloop. But already in RESUMING state. Silencing exception!")
                elif self._state == ConnectionState.RESUMING:
                    self._recv_thread_in_resume_event.set()
                    with self._state_change_condition:
                        self._state_change_condition.wait()
                    self._recv_thread_in_resume_event.clear()

        except Exception as err:
            self._log.error(
                "Error in recv_loop: {}".format(err), exc_info=True)

    def _generate_token(self) -> bytes:
        chars = string.ascii_lowercase + string.digits + string.ascii_uppercase

        return ''.join(random.choice(chars) for i in range(self.token_length)).encode('ASCII')

    def _disconnected_action(self):
        self._destroy_publisher.on_completed()

    def _change_state(self, new_state: ConnectionState):
        if new_state == self._state:
            self._log.debug(
                "Ignoring state change to old state. {}".format(self._state.name))
            return
        self._log.debug("Changeing state from '{}' to '{}'".format(
            self._state.name, new_state.name))
        with self._state_change_condition:
            self._state = new_state
            if new_state == ConnectionState.DISCONNECTED:
                self._disconnected_action()
            self._state_change_condition.notify_all()

    def _clean_send_cache(self, last_received_position: int = -1, time_cut=None):
        try:
            with self._send_cache_lock:
                idx_to_remove = []
                if last_received_position > 0:
                    for idx, cache_entry in enumerate(self._send_cache):
                        if cache_entry.pos < last_received_position:
                            idx_to_remove.append(idx)
                for idx in sorted(idx_to_remove, reverse=True):
                    self._send_cache.pop(idx)
                idx_to_remove.clear()
                if time_cut != None:
                    for idx, cache_entry in enumerate(self._send_cache):
                        if cache_entry.time <= time_cut:
                            idx_to_remove.append(idx)
                for idx in sorted(idx_to_remove, reverse=True):
                    self._send_cache.pop(idx)
                idx_to_remove.clear()
        except Exception as error:  # Sometimes its hard to log errors in rx since it doesnt have a default error handler
            self._log.error("Error in clear cache function...")
            raise error

    def _create_error_logger(self):
        def on_next(error: ErrorFrame):
            if error.error_code == ErrorCodes.UNSUPPORTED_SETUP:
                self._log.error(
                    "Unsupported setup. Message: {}".format(error.error_data))
                self._change_state(ConnectionState.DISCONNECTED)
            else:
                self._log.error(
                    "Received Error Frame. Error Code: {}. Error Data: {}".format(
                        error.error_code, error.error_data))

        self.recv_observable().pipe(
            op.take_until(self._destroy_publisher)
        ).subscribe(on_next=on_next)
