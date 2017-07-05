package net.cezar;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Created by cezargrzelak on 7/2/17.
 */
public interface DependencyTopologyFactory<K, V, R> {
    static <T> Set<T> concatenate(Set<T> first, Set<T> second) {
        HashSet<T> retn = new HashSet<>();
        if (first != null) {
            retn.addAll(first);
        }
        if (second != null) {
            retn.addAll(second);
        }
        return retn;
    }

    static <T> Set<T> concatenate(T first, Set<T> second) {
        HashSet<T> retn = new HashSet<>();
        if (first != null) {
            retn.add(first);
        }
        if (second != null) {
            retn.addAll(second);
        }
        return retn;
    }

    static <T> Set<T> concatenate(T first, T second) {
        HashSet<T> retn = new HashSet<>();
        retn.add(first);
        retn.add(second);
        return retn;
    }

    KStream<K, R> createTopology(
            KStreamBuilder builder,
            String name,
            KStream<K, V> source,
            DependencyResolver<K, V> resolver,
            Converter<K, V, R> converter,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            Serde<R> returnSerde
    );

    class WrappedValue<K, V> {
        private DependencyTopologyFactoryFunctional.WrappedKey<K> wrappedKey;
        V wrappedValue;

        public WrappedValue(DependencyTopologyFactoryFunctional.WrappedKey<K> wrappedKey, V wrappedValue) {
            this.wrappedKey = wrappedKey;
            this.wrappedValue = wrappedValue;
        }

        public WrappedValue() {
        }

        public DependencyTopologyFactoryFunctional.WrappedKey<K> getWrappedKey() {
            return wrappedKey;
        }

        public V getWrappedValue() {
            return wrappedValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WrappedValue<?, ?> that = (WrappedValue<?, ?>) o;
            return Objects.equals(wrappedKey, that.wrappedKey) &&
                    Objects.equals(wrappedValue, that.wrappedValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(wrappedKey, wrappedValue);
        }

        @Override
        public String toString() {
            return "WrappedValue {" +
                    "wrappedKey=" + wrappedKey +
                    ", wrappedValue=" + wrappedValue +
                    " }";
        }
    }

    class WrappedKey<WK> {
        private UUID uuid;
        private WK key;

        public WrappedKey(UUID uuid, WK key) {
            this.uuid = uuid;
            this.key = key;
        }

        public WrappedKey() {
        }

        @Override
        public String toString() {
            return "WrappedKey { " +
                    "uuid=" + uuid +
                    ", key=" + key +
                    " }";
        }

        public UUID getUuid() {
            return uuid;
        }

        public WK getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WrappedKey<?> that = (WrappedKey<?>) o;
            return Objects.equals(uuid, that.uuid) &&
                    Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, key);
        }
    }

    class WrappedValueSerde<WK, WV> implements Serde<WrappedValue<WK, WV>> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<WrappedValue<WK, WV>> serializer() {
            return new KafkaJsonSerializer<>();
        }

        @Override
        public Deserializer<WrappedValue<WK, WV>> deserializer() {
            return new KafkaJsonDeserializer<WrappedValue<WK, WV>>() {
                WrappedValue<WK, WV> dummy = new WrappedValue<>();

                @Override
                protected Class<WrappedValue<WK, WV>> getType() {
                    return (Class<WrappedValue<WK, WV>>) dummy.getClass();
                }
            };
        }
    }

    class WrappedKeySerde<WK> implements Serde<WrappedKey<WK>> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<WrappedKey<WK>> serializer() {
            return new KafkaJsonSerializer<>();
        }

        @Override
        public Deserializer<WrappedKey<WK>> deserializer() {
            return new KafkaJsonDeserializer<WrappedKey<WK>>() {
                private WrappedKey<WK> wrappedKey = new WrappedKey<>();

                @Override
                protected Class<WrappedKey<WK>> getType() {
                    return (Class<WrappedKey<WK>>) wrappedKey.getClass();
                }
            };
        }
    }

    class KeyValueSerde<KK, VV> implements Serde<KeyValue<KK, VV>> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<KeyValue<KK, VV>> serializer() {
            return new KafkaJsonSerializer<>();
        }

        @Override
        public Deserializer<KeyValue<KK, VV>> deserializer() {
            return new KafkaJsonDeserializer<KeyValue<KK, VV>>() {
                KeyValue<KK, VV> dummy = new KeyValue<>(null, null);

                @Override
                protected Class<KeyValue<KK, VV>> getType() {
                    return (Class<KeyValue<KK, VV>>) dummy.getClass();
                }
            };
        }
    }

    class SetJsonDeserializer<KK> extends KafkaJsonDeserializer<Set<KK>> {
        private final Set<?> dummy = new HashSet<>();

        @Override
        protected Class<Set<KK>> getType() {
            return (Class<Set<KK>>) dummy.getClass();
        }
    }

    class SetSerde<KK> implements Serde<Set<KK>> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<Set<KK>> serializer() {
            return new KafkaJsonSerializer<>();
        }

        @Override
        public Deserializer<Set<KK>> deserializer() {
            return new SetJsonDeserializer();
        }

        public SetSerde() {
        }
    }
}
