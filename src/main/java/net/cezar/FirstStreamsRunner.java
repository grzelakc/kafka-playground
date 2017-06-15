package net.cezar;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by cezargrzelak on 6/12/17.
 */
public class FirstStreamsRunner {

    private static Collection<String> getDeps(String entity) {
        if (entity.startsWith("foo")) {
            return Arrays.asList("bar", "qux");
        } else if (entity.startsWith("bar")) {
            return Arrays.asList("baz");
        } else {
            return Collections.emptyList();
        }
    }

    public static void main(String... args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        System.out.println(getDeps("bar baz"));

        final Serde<String> stringSerde = Serdes.String();


        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source = builder.stream(Constants.INPUT_TOPIC);
        source.to(Constants.SOURCE_COPY_TOPIC);
        source.print();

        KStream<String, String> branches[] = source.branch(
                (key, value) -> getDeps(key).isEmpty(),
                (key, value) -> true
        );

        branches[0].to(Constants.READY_TOPIC);
        //bar -> barValue
        //qux -> quxValue

        branches[0].to(Constants.OUTPUT_TOPIC);

        KStream<String, String> dest = builder.stream(Constants.READY_TOPIC);

        KTable<String, String> resolvedEntities = branches[1]
                .flatMapValues((value) -> getDeps(value))
                //foo - bar
                //foo - qux
                .map((key, value) -> new KeyValue<>(value, key))
                //bar = foo
                //qux - foo
                .join(dest, (value1, value2) -> value1 + "->" + value2,
                        JoinWindows.of(TimeUnit.MINUTES.toMillis(500)), stringSerde, stringSerde, stringSerde)
                //bar - foo->barValue
                //qux - foo->quxValue
                .map((key, value) -> {
                    String[] content = value.split("->");
                    return new KeyValue<>(content[0], content[1]);
                })
                //foo - barValue
                //foo - quxValue
                .groupByKey()
                .reduce((aggValue, newValue) -> aggValue + ":::" + newValue, "resolved-entities");
                //foo -> barValue::quxValue

        KTable<String, String> sourceTable = builder.table(Constants.SOURCE_COPY_TOPIC, "destTableTopic");
        KTable<String, String> output = sourceTable.join(resolvedEntities, (value1, value2) -> value1 + ":::" + value2);
        //foo -> barValue::quxValue::fooValue
        //process here and call is ready on the set of values

        KStream<String, String> outputStream = output.toStream();
//        outputStream.filter(/* call isReady(quxValue, fooValue) on foo*/ );

        //test readiness here
        outputStream.to(Constants.READY_TOPIC);
        outputStream.to(Constants.OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();


        Properties props2 = new Properties();
        props2.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer" + UUID.randomUUID());
        props2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props2);

        consumer.subscribe(Arrays.asList(Constants.OUTPUT_TOPIC));

        boolean run = true;
        while (run) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received:" + record.key() + " - " + record.value());
                if ("quit".equalsIgnoreCase(record.value())) {
                    run = false;
                    break;
                }
            }
        }

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.

        streams.close();

    }


}
