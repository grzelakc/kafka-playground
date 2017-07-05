package net.cezar;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashSet;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static net.cezar.DependencyTopologyFactory.concatenate;

/**
 * Created by cezargrzelak on 6/29/17.
 */
public class DependencyTopologyFactoryFunctional<K, V, R> implements DependencyTopologyFactory<K, V, R> {
    @Override
    public KStream<K, R> createTopology(KStreamBuilder builder, String name, KStream<K, V> source,
            DependencyResolver<K, V> resolver, Converter<K, V, R> converter, Serde<K> keySerde, Serde<V> valueSerde,
            Serde<R> returnSerde) {
        return null;
    }

    //    public KStream<K, R> createTopology(
//            KStreamBuilder builder,
//            String name,
//            String inputTopic,
//            String inputCopyTopic,
//            String outputTopic,
//            DependencyResolver<K, V> resolver,
//            Converter<K, V, R> converter,
//            Serde<K> keySerde,
//            Serde<V> valueSerde,
//            Serde<R> returnSerde
//    ) {
//        KStream<K, V> source = builder.stream(keySerde, valueSerde, inputTopic);
//        source.to(keySerde, valueSerde, inputCopyTopic);
//
//        //declare source table
//        KTable<K, V> sourceTable = builder.table(keySerde, valueSerde, inputCopyTopic, "src-table-for-" + name);
//
//        //construct lookup and reverse lookup
//        KTable<K, Set<K>> lookup = sourceTable.toStream()
//                .map((key, value) -> {
//                    HashSet<K> dependencies = new HashSet<>(resolver.getDependencies(key, value));
//                    return new KeyValue<K, Set<K>>(key, dependencies);
//                })
//                .groupByKey(keySerde, new SetSerde<K>())
//                .reduce((aggValue, newValue) -> concatenate(aggValue, newValue), "lookup-for-" + name);
//
//        KTable<K, Set<K>> reverseLookup = lookup.toStream()
//                .flatMap((key, value) ->
//                        value.stream().map((element) -> new KeyValue<K, K>(element, key)).collect(Collectors.toSet()))
//                .map((key, value) -> new KeyValue<>(key, Collections.singleton(value)))
//                .groupByKey(keySerde, new SetSerde<K>())
//                .reduce((aggValue, newValue) -> concatenate(aggValue, newValue), "reverse-lookup-for-" + name);
//
//
//        KStream<K, Set<K>> joined = sourceTable.toStream()
//                .join(reverseLookup, (leftValue, rightValue) -> rightValue, keySerde, valueSerde);
//
//        joined
//                .flatMap((key, value) ->
//                        value.stream().map((element) -> new KeyValue<K, K>(element, key)).collect(Collectors.toSet()))
//                .join(sourceTable, (leftValue, rightValue) -> rightValue, keySerde, keySerde)
//                .to(keySerde, valueSerde, inputCopyTopic);
//
//        lookup.toStream()
//                .flatMap((key, value) -> {
//                    UUID uuid = UUID.randomUUID();
//                    WrappedKey<K> wrappedKey = new WrappedKey<>(uuid, key);
//                    Set<KeyValue<WrappedKey<K>, K>> retn =
//                            value.stream().map((v) -> new KeyValue<>(wrappedKey, v)).collect(Collectors.toSet());
//                    retn.add(new KeyValue<>(wrappedKey, key));
//                    return retn;
//                })
//                //flatmap to a new key
//                // xyz+foo - foo
//                // xyz+foo - bar
//                // xyz+foo - qux
//
//
//                .map((key, value) -> new KeyValue<K, WrappedKey<K>>(value, key))
//                //flip
//                //foo - xyz+foo
//                //bar - xyz+foo
//                //qux - xyz+foo
//
//
//                .join(lookup, (leftValue, rightValue) ->
//                        new KeyValue<>(leftValue, rightValue), keySerde, new WrappedKeySerde<>())
//                //join with lookup table
//                //foo - xyz+foo, [bar, qux]
//                //bar - xyz+foo, [baz]
//                //qux - xyz+foo, []
//
//
//                .map((key, value) -> value)
//                //map
//                //xyz+foo - [bar, qux]
//                //xyz+fo
//                // o - [baz]
//
//
//                .groupByKey(new WrappedKeySerde<>(), new SetSerde<K>())
//                .reduce((aggValue, newValue) -> concatenate(aggValue, newValue)).toStream()
//                //groupbykey + concatenate
//                //xyz+foo - [bar, qux, baz]
//
//                .flatMap((windowkey, value) -> {
//                    WrappedKey<K> key = windowkey.key();
//                    HashSet<KeyValue<K, WrappedKey<K>>> retn = new HashSet<>();
//                    retn.add(new KeyValue<>(key.getKey(), key));
//                    retn.addAll(
//                            value.stream().map((v) -> new KeyValue<K, WrappedKey<K>>(v, key))
//                                    .collect(Collectors.toSet()));
//                    return retn;
//                })
//                //.flatmap
//                //foo - xyz+foo
//                //bar - xyz+foo
//                //qux - xyz+foo
//                //baz - xyz+foo
//
//                .join(sourceTable, (leftValue, rightValue) -> new WrappedValue<K, V>(leftValue, rightValue),
//                        keySerde, new WrappedKeySerde<>())
//                //join with sourceKTable
//                //foo - fooValue - xyz+foo
//                //bar - barValue - xyz+foo
//                //qux - quxValue - xyz+foo
//                //baz - bazValue - xyz+foo
//
//                .map((key, value) ->
//                        new KeyValue<WrappedKey<K>, Set<KeyValue<K, V>>>(
//                                value.getWrappedKey(),
//                                Collections.singleton(new KeyValue<>(key, value.getWrappedValue()))))
//                //map to flip keys
//                //xyz+foo - foo - fooValue
//                //xyz+foo - bar - barValue
//                //xyz+foo - qux - quxValue
//                //xyz+foo - baz - bazValue
//
//
//                .groupByKey(new WrappedKeySerde<K>(), new SetSerde<KeyValue<K, V>>())
//                .reduce((aggValue, newValue) ->
//                        concatenate(aggValue, newValue), TimeWindows.of(TimeUnit.SECONDS.toMillis(5)))
//                .toStream()
////                .map((key, value) -> new KeyValue<>(key.key(), value))
//                .print();
////                .to(new WrappedKeySerde<K>(), new SetSerde<KeyValue<K, V>>(), outputTopic);
//        //groupByKey + concatenate
//        //xyz+foo - [fooValue, barValue, quxValue, bazValue]
//
//        return null;
//    }

}
