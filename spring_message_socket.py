import sys
import time
from rsockets2 import RMessageSocket, SocketType
import logging
import rx
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
    socket = RMessageSocket(socket_type=SocketType.WEBSOCKET, keepalive=10000, maxlive=20000,
                            hostname=SPRING_SERVER_HOSTNAME, port=SPRING_SERVER_PORT, url="ws://localhost:8080/rsocket")

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
        start = time.time()

        def on_complete():
            global start
            needed = time.time() - start
            print("Request Response Complete. Total Received --- {}mb Needed: {}s Speed: {}mb/s".format(total_size /
                                                                                                  1000000, needed, last_size / 1000000 / needed))

        def continous_request_response():
            global start
            start = time.time()
            socket.request_response('test.controller.mono').subscribe(on_next=lambda x: adder(
                x), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=on_complete)

        rx.interval(2.0).subscribe(lambda x: continous_request_response())

        # Test Request Response Error
        socket.request_response('test.controller.mono.error').subscribe(on_next=lambda x: adder(
            x), on_error=lambda err: print("Perfect! It failed: {}".format(err)), on_completed=on_complete)

        # Test Request Stream
        socket.request_stream('test.controller.flux').subscribe(on_next=lambda x: print(
            "Received Size: {} mb".format(sys.getsizeof(x) / 1000000.0)), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=lambda: print("Request Stream Complete"))

        # # Test Fire And Forget
        socket.request_response(
            'test.controller.triggerfnf').subscribe()

        time_waited = 0
        while True:
            time.sleep(1.0)
            time_waited += 1
            if time_waited > 30:
                break
    finally:
        socket.close()
