package dev.lydech.dispatch.handler;

import dev.lydech.dispatch.message.OrderCreated;
import dev.lydech.dispatch.service.DispatchService;
import dev.lydech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderCreatedHandlerTest {

    private OrderCreatedHandler handler;

    private DispatchService dispatchServiceMock;


    @BeforeEach
    void setUp() {
        dispatchServiceMock = mock(DispatchService.class);
        handler = new OrderCreatedHandler(dispatchServiceMock);
    }

    @Test
    void listen_Success() throws Exception{
        String key = randomUUID().toString();
        OrderCreated event = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        handler.listen(0, key, event);
        verify(dispatchServiceMock, times(1)).process(key, event);
    }

    @Test
    void listen_ServiceThrowsException() throws Exception{
        String key = randomUUID().toString();
        OrderCreated event = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Service error")).when(dispatchServiceMock).process(key, event);
        handler.listen(0, key, event);
        verify(dispatchServiceMock, times(1)).process(key, event);
    }
}