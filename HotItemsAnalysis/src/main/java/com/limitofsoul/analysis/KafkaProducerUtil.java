package com.limitofsoul.analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class KafkaProducerUtil {

    public static void main(String[] args) throws Exception {
        writeToKafka("hotItems");
    }

    static void writeToKafka(String topic) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.50.16:9091,192.168.50.16:9092,192.168.50.16:9093");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        BufferedReader bufferedReader = new BufferedReader(new FileReader("HotItemsAnalysis/src/main/resources/UserBehavior.csv"));
        String line;
        while ((line=bufferedReader.readLine()) != null){
            kafkaProducer.send(new ProducerRecord<>(topic, line));
        }

        kafkaProducer.close();
    }
}
