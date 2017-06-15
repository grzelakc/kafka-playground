package net.cezar;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by cezargrzelak on 6/12/17.
 */
public class InteractiveProducer {
    public static void main(String... args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<String, String> producer =
                new KafkaProducer(properties, new StringSerializer(), new StringSerializer());

        Scanner in = new Scanner(System.in);
        String input;
        while ("quit".equalsIgnoreCase(input = in.nextLine()) == false) {
            String[] words = input.split("->");
            String key = words[0].trim();
            String value = words[1].trim();
            producer.send(new ProducerRecord<>(Constants.INPUT_TOPIC, null, null, key, value));
            System.out.println("Sent tuple. key = " + key + " value = " + value);
        }
    }
}
