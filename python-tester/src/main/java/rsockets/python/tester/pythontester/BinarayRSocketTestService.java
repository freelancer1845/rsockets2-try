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
        disposable = RSocketServer.create(SocketAcceptor.with(new SocketHandler())).bind(TcpServerTransport.create(0))
                .subscribe();

    }

    @PreDestroy
    protected void preDestroy() {
        this.disposable.dispose();
    }

    public static final class SocketHandler implements RSocket {
        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            return Mono.just(payload);
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            var count = payload.data().readInt();
            return Flux.interval(Duration.ofMillis(1)).take(count).map(c -> payload);
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
