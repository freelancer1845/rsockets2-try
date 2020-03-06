import sys
import time
from rsockets2 import RMessageSocket, SocketType
import logging
import rx
import rx.operators
logging.basicConfig(level=logging.DEBUG)

SPRING_SERVER_HOSTNAME = 'localhost'
SPRING_SERVER_PORT = 24512


"""
    Spring Controller Functions Example

@MessageMapping("test.controller.mono")
public Mono<Map<String, byte[]>> monoTest() {
    ByteBuffer buffer = ByteBuffer.allocate(12 * 1000000);
    for (int i = 0; i < buffer.capacity(); i++) {
        buffer.put((byte) i);
    }
    return Mono.just(Collections.singletonMap("data", "Hello World".getBytes()));
}

@MessageMapping("test.controller.flux")
public Flux<Map<String, byte[]>> fluxTest() {
    ByteBuffer buffer = ByteBuffer.allocate(16 * 1000000);
    for (int i = 0; i < buffer.capacity(); i++) {
        buffer.put((byte) i);
    }
    return Flux.range(0, 5)
                .map(t -> Collections.singletonMap("data", buffer.array()));
}

@MessageMapping("test.controller.triggerfnf")
public Mono<Void> triggerfnf(RSocketRequester requester, String payload) {
    requester.route("test.controller.triggerfnf")
                .data(payload)
                .send()
                .subscribe();
    return Mono.empty();
}


"""

if __name__ == "__main__":
    # Exchange socket_type if necessary
    socket = RMessageSocket(socket_type=SocketType.TCP_SOCKET, keepalive=10000, maxlive=500000,
                            hostname=SPRING_SERVER_HOSTNAME, port=SPRING_SERVER_PORT, url="ws://localhost:8080/rsocket", with_resume_support=True)

    print("This expects Spring MessageMapping 'test.controller.mono' returning a Mono<Map<String, byte[]>>")
    print("This expects Spring MessageMapping 'test.controller.flux' returning a Flux<Map<String, byte[]>>")
    print("This expects Spring MessageMapping 'test.controller.triggerfnf' returning a Mono<Void> and sending a FNF")

    socket.register_fire_and_forget_handler(
        'test.controller.triggerfnf', lambda x: print("Trigger Fnf Called!"))
    try:
        socket.open()
        time.sleep(1.0)

        total_size = 0
        last_size = 0

        def adder(x):
            global total_size
            global last_size
            last_size = sys.getsizeof(x)
            total_size += last_size
            global start
            needed = time.time() - start
            print("Request Response Complete. Total Received --- {}mb Needed: {}s Speed: {}mb/s".format(total_size /
                                                                                                        1000000, needed, last_size / 1000000 / needed))

        start = time.time()

        def continous_request_response():
            global start
            start = time.time()
            return socket.request_response('test.controller.mono')

        rx.timer(0.5).pipe(
            rx.operators.flat_map(lambda x: continous_request_response()),
            rx.operators.repeat()
        ).subscribe(on_next=lambda x: adder(
            x), on_error=lambda err: print("Oh my god it failed: {}".format(err)))

        # Test Request Response Error
        # socket.request_response('test.controller.mono.error').subscribe(
        #     on_error=lambda err: print("Perfect! It failed: {}".format(err)))

        # # Test Request Stream
        # socket.request_stream('test.controller.flux').subscribe(on_next=lambda x: print(
        #     "Received Size: {} mb".format(sys.getsizeof(x) / 1000000.0)), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=lambda: print("Request Stream Complete"))

        # # # Test Fire And Forget
        # socket.request_response(
        #     'test.controller.triggerfnf').subscribe()

        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    except Exception as err:
        print("Unexepected exception {}".format(err))
        raise err
    finally:
        socket.close()
