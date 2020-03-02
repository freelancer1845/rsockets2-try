from rsockets2 import RSocket, SocketType
import rx
import time
import rsockets2.frames as frames

SPRING_SERVER_HOSTNAME = 'localhost'
SPRING_SERVER_PORT = 24512

"""
    Spring Controller Functions Example

@MessageMapping("test.controller.mono")
public Mono<byte[]> monoTest() {
    ByteBuffer buffer = ByteBuffer.allocate(12 * 1000000);
    for (int i = 0; i < buffer.capacity(); i++) {
        buffer.put((byte) i);
    }
    return Mono.just(buffer.array());
}

@MessageMapping("test.controller.flux")
public Flux<byte[]> fluxTest() {
    ByteBuffer buffer = ByteBuffer.allocate(16 * 1000000);
    for (int i = 0; i < buffer.capacity(); i++) {
        buffer.put((byte) i);
    }
    return Flux.range(0, 20)
                .map(t -> buffer.array());
}

@MessageMapping("test.controller.triggerfnf")
public Mono<Void> triggerfnf(RSocketRequester requester, String payload) {
    requester.route("test.controller.triggerfnf")
                .data(payload)
                .send()
                .subscribe();
    System.out.println("Send FNF");
    return Mono.empty();
}

"""

if __name__ == "__main__":

    print("This expects Spring MessageMapping 'test.controller.mono' returning a Mono<byte[]>")
    print("This expects Spring MessageMapping 'test.controller.flux' returning a Flux<byte[]>")
    print("This expects Spring MessageMapping 'test.controller.triggerfnf' returning a Mono<Void> and sending a FNF")

    socket = RSocket(socket_type=SocketType.TCP_SOCKET, keepalive=10000, maxlive=10000,
                     hostname=SPRING_SERVER_HOSTNAME, port=SPRING_SERVER_PORT)

    def request_response_handler(frame: frames.RequestResponse):
        return rx.just("5".encode('ASCII'))
    socket.on_request_response = request_response_handler

    def request_stream_handler(frame: frames.RequestStream):
        return rx.of("3".encode('ASCII'), "3".encode('ASCII'), "3".encode('ASCII'))
    socket.on_request_stream = request_stream_handler

    def fire_and_forget_handler(frame: frames.RequestFNF):
        print("Received Fire And Forget! Payload: {}".format(
            frame.request_data.decode("ASCII")))
    socket.on_fire_and_forget = fire_and_forget_handler

    try:
        socket.open()
        time.sleep(1.0)

        total_size = 0

        def adder(x):
            global total_size
            total_size += len(x)
        start = time.time()

        def on_complete():
            global start
            needed = time.time() - start
            print("Request Response Complete. Received --- {}mb Needed: {}s Speed: {}mb/s".format(total_size /
                                                                       1000000, needed, total_size / 1000000 / needed))
        # Test Request Response
        socket.request_response(meta_data=b'test.controller.mono', data=bytes(0)).subscribe(on_next=lambda x: adder(
            x), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=on_complete)

        # Test Request Stream 
        socket.request_stream(meta_data=b'test.controller.flux', data=bytes(0)).subscribe(on_next=lambda x: print(
            "Received Size: {} mb".format(len(x) / 1000000.0)), on_error=lambda err: print("Oh my god it failed: {}".format(err)), on_completed=lambda: print("Request Stream Complete"))

        # Test Fire And Forget
        socket.request_response(
            meta_data=b'test.controller.triggerfnf', data=bytes(0)).subscribe()

        time_waited = 0
        while True:
            time.sleep(1.0)
            time_waited += 1
            if time_waited > 30:
                break
    finally:
        socket.close()
