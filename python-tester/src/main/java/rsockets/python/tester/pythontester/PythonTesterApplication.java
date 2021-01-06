package rsockets.python.tester.pythontester;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.rsocket.server.RSocketServerCustomizer;
import org.springframework.context.annotation.Bean;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.plugins.SocketAcceptorInterceptor;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class PythonTesterApplication {

	public static void main(String[] args) {
		try {
			SpringApplication.run(PythonTesterApplication.class, args);

		} catch (Exception e) {
			System.out.println(e);
		}
	}

	// @Bean
	// public RSocketServerCustomizer rSocketServerCustomizer() {
	// 	return server -> {
	// 		server.interceptors(configure -> {
	// 			configure.forSocketAcceptor(new SocketAcceptorInterceptor() {

	// 				@Override
	// 				public SocketAcceptor apply(SocketAcceptor t) {
	// 					return new SocketAcceptor() {

	// 						@Override
	// 						public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
	// 							var mimeType = setup.dataMimeType();
	// 							var metaType = setup.metadataMimeType();
								
	// 							return Mono.just(sendingSocket);
	// 						}

	// 					};
	// 				}

	// 			});
	// 		});
	// 	};
	// }

}
