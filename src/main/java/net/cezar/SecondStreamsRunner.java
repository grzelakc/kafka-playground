package net.cezar;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Filter;
import org.rocksdb.Options;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static net.cezar.Constants.INPUT_TOPIC;
import static net.cezar.Constants.OUTPUT_TOPIC;

/**
 * Created by cezargrzelak on 6/29/17.
 */
public class SecondStreamsRunner {

    public static class CustomRocksDBConfig implements RocksDBConfigSetter {

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            BlockBasedTableConfig tableConfig = new org.rocksdb.BlockBasedTableConfig();
            tableConfig.setBlockCacheSize(600 * 1024 * 1024L);
            tableConfig.setBlockSize(32 * 1024L);
            tableConfig.setCacheIndexAndFilterBlocks(true);
            tableConfig.setHashIndexAllowCollision(true);
            Filter filter = new BloomFilter(32);
            tableConfig.setFilter(filter);

            options.setTableFormatConfig(tableConfig);
            options.setMaxWriteBufferNumber(2);
            options.setWriteBufferSize(100 * 1024 * 1024L);
            options.setMaxBytesForLevelBase(100 * 1024 * 1024L);
            options.setWalSizeLimitMB(1000);
            options.setMaxBackgroundCompactions(8);
            options.setAllowMmapReads(false);
            options.setCompactionStyle(CompactionStyle.LEVEL);
        }
    }


    public static void main(String... args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10_000);
//        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);

        MyResolver<String,String> myResolver = new MyResolver<>();
        MyConverter<String,String,String> myConverter = new MyConverter<>();
        final Serde<String> stringSerde = Serdes.String();

        //////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////

        String topologyName = "target";

        KStreamBuilder builder = new KStreamBuilder();

        DependencyTopologyFactory<String, String, String> dtf = new DependencyTopologyFactoryImperative<>();

        KStream<String, String> source = builder.stream(stringSerde, stringSerde, INPUT_TOPIC);

        KStream<String, String> outputStream =
                dtf.createTopology(builder, topologyName, source, myResolver, myConverter,
                        stringSerde, stringSerde, stringSerde);

        outputStream.to(stringSerde, stringSerde, OUTPUT_TOPIC);

        //////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////


        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();


        Properties props2 = new Properties();
        props2.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer" + UUID.randomUUID());
        props2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                DependencyTopologyFactoryFunctional.SetJsonDeserializer.class);
//        props2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, Set<String>> consumer = new KafkaConsumer<>(props2);

        consumer.subscribe(Arrays.asList(OUTPUT_TOPIC));

        int count = 0;
        boolean run = true;
        while (run) {
            ConsumerRecords<String, Set<String>> records = consumer.poll(1000);
            for (ConsumerRecord<String, Set<String>> record : records) {
                if (count % 10_000 == 0 || count < 100) {
                    System.out.print("Received (" + count + "th):" + record.key() + " - " + record.value());
                    System.out.println(" at " + new Date());
                }
                count++;
                if (Thread.currentThread().isInterrupted()) {
                    run = false;
                    break;
                }
            }
        }


        streams.close();

    }


}
