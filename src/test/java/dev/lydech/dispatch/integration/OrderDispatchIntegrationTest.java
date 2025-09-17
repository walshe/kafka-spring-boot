package dev.lydech.dispatch.integration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dev.lydech.dispatch.DispatchConfiguration;
import dev.lydech.dispatch.integration.WiremockUtils;
import dev.lydech.dispatch.message.OrderCreated;
import dev.lydech.dispatch.message.OrderDispatched;
import dev.lydech.dispatch.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
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

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static dev.lydech.dispatch.integration.WiremockUtils.stubWiremock;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class})
@AutoConfigureWireMock(port = 0) //will use random unused port at runtime
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = {"order.created", "order.created.DLT", "dispatch.tracking", "order.dispatched"})
public class OrderDispatchIntegrationTest {

    private final static String ORDER_CREATED_TOPIC = "order.created";
    private final static String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    private final static String ORDER_CREATED_DLT_TOPIC = "order.created.DLT";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private KafkaTestListener testListener;

    @Configuration
    static class TestConfig {

        @Bean
        public KafkaTestListener testListener() {
            return new KafkaTestListener();
        }
    }

    /**
     * Use this receiver to consume messages from the outbound topics.
     */
    @KafkaListener(groupId = "KafkaIntegrationTest", topics = {ORDER_DISPATCHED_TOPIC, ORDER_CREATED_DLT_TOPIC},
            containerFactory = "kafkaListenerContainerFactory", properties = {"auto.offset.reset=earliest"})
    public static class KafkaTestListener {
        AtomicInteger orderDispatchedCounter = new AtomicInteger(0);
        AtomicInteger orderCreatedDltCounter = new AtomicInteger(0);


        @KafkaHandler
        void receiveOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderDispatched payload) {
            log.debug("Received OrderDispatched key: " + key + " - payload: " + payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            orderDispatchedCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveOrderCreatedDlt(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
            log.debug("Received OrderCreated DLT key: " + key + " - payload: " + payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            orderCreatedDltCounter.incrementAndGet();
        }


    }

    @BeforeEach
    public void setUp() {
        testListener.orderDispatchedCounter.set(0);
        testListener.orderCreatedDltCounter.set(0);

        WiremockUtils.reset();

        // Wait until the partitions are assigned.  The application listener container has one topic and the test
        // listener container has multiple topics, so take that into account when awaiting for topic assignment.
        registry.getListenerContainers().stream()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container,
                        container.getContainerProperties().getTopics().length * embeddedKafkaBroker.getPartitionsPerTopic()));

    }

    /**
     * Send in an order.created event and ensure the expected outbound events are emitted.  The call to the stock service
     * is stubbed to return a 200 Success.
     */
    @Test
    public void testOrderDispatchFlow_Success() throws Exception {
        stubWiremock("/api/stock?item=my-item", 200, "true");

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.orderDispatchedCounter::get, equalTo(1));

        assertThat(testListener.orderCreatedDltCounter.get(), equalTo(0));

    }

    /**
     * The call to the stock service is stubbed to return a 400 Bad Request which results in a not-retryable exception
     * being thrown, so the outbound events are never sent.
     */
    @Test
    public void testOrderDispatchFlow_NotRetryableException() throws Exception {
        stubWiremock("/api/stock?item=my-item", 400, "Bad Request");

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.orderCreatedDltCounter::get, equalTo(1));
        assertThat(testListener.orderDispatchedCounter.get(), equalTo(0));

    }

    /**
     * The call to the stock service is stubbed to initially return a 503 Service Unavailable response, resulting in a
     * retryable exception being thrown.  On the subsequent attempt it is stubbed to then succeed, so the outbound events
     * are sent.
     */
    @Test
    public void testOrderDispatchFlow_RetryThenSuccess() throws Exception {
        stubWiremock("/api/stock?item=my-item", 503, "Service unavailable", "failOnce", STARTED, "succeedNextTime");
        stubWiremock("/api/stock?item=my-item", 200, "true", "failOnce", "succeedNextTime", "succeedNextTime");

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.orderDispatchedCounter::get, equalTo(1));
        assertThat(testListener.orderCreatedDltCounter.get(), equalTo(0));

    }

    @Test
    public void testOrderDispatchFlow_RetryUntilFailure() throws Exception {
        stubWiremock("/api/stock?item=my-item", 503, "Service unavailable");

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.orderCreatedDltCounter::get, equalTo(1));
        assertThat(testListener.orderDispatchedCounter.get(), equalTo(0));

    }

    private void sendMessage(String topic, String key, Object data) throws Exception {
        kafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }
}