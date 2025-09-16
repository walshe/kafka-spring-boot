package dev.lydech.dispatch.integration;

import dev.lydech.dispatch.DispatchApplication;
import dev.lydech.dispatch.message.OrderCreated;
import dev.lydech.dispatch.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;


@Slf4j
@SpringBootTest(classes = {
        DispatchApplication.class // tells is there spring beans are declared
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS) //spring context not reloaded after each test method, makes test faster
@ActiveProfiles("test") //allows ud to override properties in a new test application.properties file
@EmbeddedKafka(controlledShutdown = true)
public class OrderDispatchIntegrationTest {


    private final static String ORDER_CREATED_TOPIC = "order.created";

    @Autowired
    private KafkaTestListener kafkaTestListener;

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaTestListener kafkaTestListener() {
            return new KafkaTestListener();
        }
    }

    @Slf4j
    public static  class KafkaTestListener{
        AtomicInteger orderCreatedCount = new AtomicInteger(0);

        @KafkaListener(
                topics = ORDER_CREATED_TOPIC,
                groupId = "KafkaIntegrationTest"
        )
        void receiveOrderCreated(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated message) {
            log.info("Test listener received OrderCreated message: {} with key: {}", message, key);
            assertThat(key, notNullValue());
            assertThat(message, notNullValue());
            orderCreatedCount.incrementAndGet();
        }

    }


    @BeforeEach
    public void setup(){
        kafkaTestListener.orderCreatedCount.set(0);
        //partitions must be assigned before the test is executed
        kafkaListenerEndpointRegistry.getListenerContainers().stream().forEach(
                container -> {
                    ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
                }
        );
    }

    @Test
    public void testOrderCreatedFlow() throws Exception{
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(
                java.util.UUID.randomUUID(),
                java.util.UUID.randomUUID().toString()
        );

        String key = java.util.UUID.randomUUID().toString();
        kafkaTemplate.send(MessageBuilder.withPayload(orderCreated).setHeader(KafkaHeaders.KEY, key).setHeader(KafkaHeaders.TOPIC, ORDER_CREATED_TOPIC)
                .build()).get();

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(
                kafkaTestListener.orderCreatedCount::get, equalTo(1));


    }
}
