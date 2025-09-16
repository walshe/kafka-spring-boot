package dev.lydech.dispatch;

import dev.lydech.dispatch.message.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * Doing this programmatically allows us to avoid putting all this stuff in application.properties
 * also allows us to use the same listener container factory for different message types
 * also allows us to set different properties for different message types
 * also gives us compile time checking of message types
 */
@ComponentScan(basePackages = "dev.lydech")
@Configuration
public class DispatchConfiguration {

    private static String TRUSTED_PACKAGES = "dev.lydech.dispatch.message"; //restrict deserialization to only our message classes

    /**
     * ConcurrentKafkaListenerContainerFactory is recommended over KafkaListenerContainerFactory because it supports concurrent message listeners
     *
     * @param consumerFactory
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    /**
     * avoids doing all this stuff in applicaiton.properties
     *
     * @param bootstrapServers
     * @return
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers
    ) {
        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
                        ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class,
                        JsonDeserializer.TRUSTED_PACKAGES, TRUSTED_PACKAGES,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
        );

    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers
    ) {
        return new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
        );
    }
}
