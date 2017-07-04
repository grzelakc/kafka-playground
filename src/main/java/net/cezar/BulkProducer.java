package net.cezar;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by cezargrzelak on 7/1/17.
 */
public class BulkProducer {
    public static void main(String... args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        String[] array = { "foo", "bar", "baz", "qux", "bob", "siv", "sev", "ent", "man", "pyt" };
        List<String> keys = Arrays.asList(array);


        KafkaProducer<String, String> producer =
                new KafkaProducer(properties, new StringSerializer(), new StringSerializer());

        Random random = new Random(123456);

        int n = 0;
        for (n = 0; n < 1_000_000; n++) {
            Collections.shuffle(keys);
            for(String entry : keys) {
                String key = entry + n;
                String value = entry + "Value" + n;
                producer.send(new ProducerRecord<>(Constants.INPUT_TOPIC, null, null, key, value));
                if(n == 0 || n == 3 || n == 5000) {
                    System.out.println("sent: " + key + " - " + value);
                }
            }
        }
        System.out.println("Done sending " + n + " entities.");
    }

}
