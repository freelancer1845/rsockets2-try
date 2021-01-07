package rsockets.python.tester.pythontester;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Controller;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Controller
public class TestController {

    @MessageMapping("/route/request-response")
    public Mono<byte[]> testRequestResponse(byte[] data, RSocketRequester requester) {
        return requester.route("/route/request-response").data(data).retrieveMono(byte[].class);
    }

    @MessageMapping("/route/request-stream")
    public Flux<byte[]> testRequestStream(byte[] data, RSocketRequester requester) {
        return requester.route("/route/request-stream").data(data).retrieveFlux(byte[].class);
    }

    @MessageMapping("/route/request-fnf")
    public Mono<Void> requestFnf(byte[] data, RSocketRequester requester) {
        return requester.route("/route/request-fnf").data(data).send();
    }

}
