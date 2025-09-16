package dev.lydech.dispatch.service;

import dev.lydech.dispatch.message.OrderCreated;
import dev.lydech.dispatch.message.OrderDispatched;
import dev.lydech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DispatchServiceTest {

    private DispatchService service;

    private KafkaTemplate kafkaProducerMock;

    @BeforeEach
    void setUp() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        service = new DispatchService(kafkaProducerMock);
    }

    @Test
    void process_Success() throws Exception {
        String key = UUID.randomUUID().toString();
        when(kafkaProducerMock.send(anyString(), anyString(),any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        OrderCreated event = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID().toString());
        service.process(key, event);
        verify(kafkaProducerMock, times(1)).send(eq(
                "order.dispatched"
        ), eq(key),any(OrderDispatched.class));
    }

    @Test
    void process_ProducerThrowsException() throws Exception {
        String key = UUID.randomUUID().toString();
        OrderCreated event = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID().toString());
        doThrow(new RuntimeException("Kafka error")).when(kafkaProducerMock).send(eq(
                "order.dispatched"
        ), eq(key), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> {
            service.process(key, event);
        });


        verify(kafkaProducerMock, times(1)).send(eq(
                "order.dispatched"
        ), eq(key),any(OrderDispatched.class));

        assertThat(exception.getMessage(), equalTo("Kafka error"
        ));
    }
}