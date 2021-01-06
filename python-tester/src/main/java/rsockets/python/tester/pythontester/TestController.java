package rsockets.python.tester.pythontester;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Controller;

import reactor.core.publisher.Mono;

@Controller
public class TestController {

    @MessageMapping("/route/request-response")
    public Mono<byte[]> testRequestResponse(byte[] data, RSocketRequester requester) {
        return requester.route("/route/request-response").data(data).retrieveMono(byte[].class);
    }

}
