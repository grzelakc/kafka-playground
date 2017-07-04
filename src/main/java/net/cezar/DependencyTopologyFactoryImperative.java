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
    public void createTopology(
            KStreamBuilder builder,
            String name,
            String inputTopic,
            String inputCopyTopic,
            String outputTopic,
            DependencyResolver<K, V> resolver,
            Converter<K, V, R> converter,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            Serde<R> returnSerde
    ) {

        StateStoreSupplier lookupStore = Stores.create("lookup-for-" + name)
                .withKeys(Serdes.String())
                .withValues(new DependencyTopologyFactory.SetSerde<K>())
                .inMemory()
                .build();

        StateStoreSupplier reverseLookupStore = Stores.create("reverse-lookup-for-" + name)
                .withKeys(Serdes.String())
                .withValues(new DependencyTopologyFactory.SetSerde<String>())
                .inMemory()
                .build();

        StateStoreSupplier valueStore = Stores.create("values-for-" + name)
                .withKeys(Serdes.String())
                .withValues(valueSerde)
                .persistent()
                .build();


        builder.addStateStore(lookupStore);
        builder.addStateStore(reverseLookupStore);
        builder.addStateStore(valueStore);

        KStream<K, V> source = builder.stream(keySerde, valueSerde, inputTopic);

        MyTransformerSupplier<K, V, R> supplier =
                new MyTransformerSupplier<>(lookupStore.name(), reverseLookupStore.name(), valueStore.name(),
                        new MyResolver<>(), converter);

        source
                .transform(supplier, lookupStore.name(), reverseLookupStore.name(), valueStore.name())
                .flatMap((key, value) -> value)
                .to(keySerde, returnSerde, outputTopic);
    }
}
