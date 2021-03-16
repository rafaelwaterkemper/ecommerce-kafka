package com.github.rafaelwaterkemper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args)  {
        var consumer = new KafkaConsumer<String, String>(getProperties());

        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
        while (true) {
            System.out.println("passando aqui");
            consumer.poll(Duration.ofMillis(2000L)).forEach(value -> System.out.println(value.value()));
        }
    }

    private static Properties getProperties() {
        var propConsumer = new java.util.Properties();
        propConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, SimpleConsumer.class.getName());
        return propConsumer;
    }
}
