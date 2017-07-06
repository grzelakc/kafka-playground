package net.cezar;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

/**
 * Created by cezargrzelak on 7/2/17.
 */
public class DependencyTopologyFactoryImperative<K, V, R> implements DependencyTopologyFactory<K, V, R> {

    @Override
    public KStream<K, R>
    createTopology(
            KStreamBuilder builder,
            String name,
            KStream<K, V> source,
            DependencyResolver<K, V> resolver,
            Converter<K, V, R> converter,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            Serde<R> returnSerde
    ) {

        StateStoreSupplier reverseLookupStore = Stores.create("reverse-lookup-for-" + name)
                .withKeys(keySerde)
                .withValues(new DependencyTopologyFactory.SetSerde<K>())
                .persistent()
                .build();

        StateStoreSupplier valueStore = Stores.create("values-for-" + name)
                .withKeys(keySerde)
                .withValues(valueSerde)
                .persistent()
                .build();

        builder.addStateStore(reverseLookupStore);
        builder.addStateStore(valueStore);

//        KStream<K, V> source = builder.stream(keySerde, valueSerde, inputTopic);

        MyTransformerSupplier<K, V, R> supplier =
                new MyTransformerSupplier<>(reverseLookupStore.name(), valueStore.name(), resolver, converter);

        return source
                .transform(supplier, reverseLookupStore.name(), valueStore.name())
                .flatMap((key, value) -> value);

                //.to(keySerde, returnSerde, outputTopic);
    }
}
