package guru.springframework.rabbit.rabbitstockquoteservice.service;

import com.rabbitmq.client.Delivery;
import guru.springframework.rabbit.rabbitstockquoteservice.config.RabbitConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.Receiver;


import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class QuoteRunner implements CommandLineRunner {

    private final QuoteGeneratorService quoteGeneratorService;
    private final QuoteMessageSender quoteMessageSender;
    private final Receiver receiver;


    @Override
    public void run(String... args) throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(25);

        quoteGeneratorService.fetchQuoteStream(Duration.ofMillis(100))
                             .take(25)
                             .log("Got Quote: ")
                             .flatMap(quoteMessageSender::sendQuoteMessage)
                             .subscribe(result -> {
                                 log.debug("Send Message to Rabbit");
                                 countDownLatch.countDown();
                             }, throwable -> {
                                 log.error("Got Error ", throwable);
                             }, () -> {
                                 log.debug("All done!");
                             });

        countDownLatch.await(1, TimeUnit.SECONDS);

        AtomicInteger receivedCount = new AtomicInteger();

        receiver.consumeAutoAck(RabbitConfig.QUEUE)
                .log("Msg recived")
                .subscribe(msg -> {
                    log.debug("Recieved Message # {} - {}", receivedCount.incrementAndGet(), new String(msg.getBody()));
                }, throwable -> {
                    log.debug("Error Receiving ", throwable);
                }, () -> {
                    log.debug("Complete");
                });
    }
}
