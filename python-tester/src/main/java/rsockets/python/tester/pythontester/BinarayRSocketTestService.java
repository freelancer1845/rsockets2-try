package rsockets.python.tester.pythontester;

import java.time.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.rsocket.RSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class BinarayRSocketTestService {

    private Disposable disposable;

    @PostConstruct
    protected void postConstruct() {
        var logger = (Logger) LoggerFactory.getLogger("io.rsocket.FrameLogger");
        logger.setLevel(Level.INFO);
        disposable = RSocketServer.create(new SocketAcceptor() {
            @Override
            public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
                return Mono.just(new SocketHandler(sendingSocket));
            }
        }).bind(TcpServerTransport.create(0)).subscribe();

    }

    @PreDestroy
    protected void preDestroy() {
        this.disposable.dispose();
    }

    public static final class SocketHandler implements RSocket {
        private RSocket requester;

        private Payload payloadToReplay;

        public SocketHandler(RSocket sender) {
            this.requester = sender;
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            if (payload.hasMetadata() && payload.getMetadataUtf8().equals("Throw Error")) {
                return Mono.error(new Exception(payload.getDataUtf8()));
            }
            if (payload.hasMetadata() && payload.getMetadataUtf8().equals("ReplayPayload")) {
                return Mono.just(this.payloadToReplay);
            }
            return Mono.just(payload);
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            if (payload.hasMetadata() && payload.getMetadataUtf8().equals("Throw Error")) {
                return Flux.error(new Exception(payload.getDataUtf8()));
            }
            var count = payload.data().readInt();
            return Flux.interval(Duration.ofMillis(1)).take(count).map(c -> payload);
        }

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
            if (payload.hasMetadata()) {
                if (payload.getMetadataUtf8().equals("EchoRequestResponse")) {
                    requester.requestResponse(payload).subscribe(answer -> {
                        this.payloadToReplay = answer;
                    });
                }
                if (payload.getMetadataUtf8().equals("EchoRequestStream")) {
                    requester.requestStream(payload).subscribe(answer -> {
                        this.payloadToReplay = answer;
                    });
                }
                if (payload.getMetadataUtf8().equals("EchoRequestFNF")) {
                    requester.fireAndForget(payload).subscribe();
                }
            }
            return Mono.empty();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        BinarayRSocketTestService service = new BinarayRSocketTestService();
        service.postConstruct();
        try {

            while (true) {
                Thread.sleep(100);
            }
        } catch (Exception ex) {
            service.preDestroy();
        }
    }

}
