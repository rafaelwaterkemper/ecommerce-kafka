package com.github.rafaelwaterkemper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(Properties.getProperties());
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", "valor", "Testando envio de dados");

        producer.send(record, (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println(data.topic() + ":::partition " + data.partition() + "/ offset" + data.offset() + "/" + data.timestamp());
        }).get();
    }


}
