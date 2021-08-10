package guru.springframework.rabbit.rabbitstockquoteservice.config;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import javax.annotation.PreDestroy;


@Configuration
public class RabbitConfig {
    public static final String QUEUE = "quotes";

    @Autowired
    Mono<Connection> connectionMono;

    @Bean
    Mono<Connection> connectionMono(CachingConnectionFactory connectionFactory){
        return Mono.fromCallable(() -> connectionFactory.getRabbitConnectionFactory().newConnection());
    }

    @PreDestroy
    public void close() throws Exception {
        connectionMono.block().close();
    }

    @Bean
    Sender sender(Mono<Connection> mono){
        return RabbitFlux.createSender(new SenderOptions().connectionMono(mono));
    }

    @Bean
    Receiver receiver(Mono<Connection> mono){
        return RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(mono));
    }

    @Bean
    Flux<Delivery> deliveryFlux(Receiver receiver){
        return receiver.consumeAutoAck(QUEUE);
    }
}
