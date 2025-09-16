package dev.lydech.dispatch.handler;

import dev.lydech.dispatch.message.OrderCreated;
import dev.lydech.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(id="orderConsumerClient", topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "kafkaListenerContainerFactory")
public class OrderCreatedHandler {

    private final DispatchService dispatchService;

    @KafkaHandler
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition ,@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
        log.info("Received OrderCreated event: {} from partition: {} with key: {}", payload, partition, key);
        try {
            dispatchService.process(key, payload);
        } catch (Exception e) {
            log.error("Error processing order: {}", e.getMessage(), e);
        }
    }
}
