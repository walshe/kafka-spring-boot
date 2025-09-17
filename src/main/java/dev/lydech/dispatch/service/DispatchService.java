package dev.lydech.dispatch.service;

import dev.lydech.dispatch.client.StockServiceClient;
import dev.lydech.dispatch.message.OrderCreated;
import dev.lydech.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

import static java.util.UUID.randomUUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DispatchService {

    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";

    private static final UUID APPLICATION_ID = randomUUID();

    private final KafkaTemplate<String, Object> kafkaProducer;

    private final StockServiceClient stockServiceClient;

    public void process(String key, OrderCreated orderCreated) throws Exception {

        String availability = stockServiceClient.checkAvailability(orderCreated.getItem());
        if (Boolean.valueOf(availability)) {


            OrderDispatched orderDispatched = OrderDispatched.builder()
                    .orderId(orderCreated.getOrderId())
                    .processedById(APPLICATION_ID)
                    .notes("Dispatched" + orderCreated.getItem())
                    .build();

            kafkaProducer.send(ORDER_DISPATCHED_TOPIC, key, orderDispatched).get(); //async which is not good

            log.info("Published OrderDispatched event: {} to topic: {}", orderDispatched, ORDER_DISPATCHED_TOPIC);
        } else {
            log.warn("Item: {} is not available in stock for orderId: {}", orderCreated.getItem(), orderCreated.getOrderId());
        }
    }
}
