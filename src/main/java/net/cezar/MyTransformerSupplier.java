package net.cezar;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static net.cezar.DependencyTopologyFactory.concatenate;

/**
 * Created by cezargrzelak on 7/3/17.
 */
public class MyTransformerSupplier<K, V, R> implements TransformerSupplier<K, V, KeyValue<K, List<KeyValue<K, R>>>> {
    private final String reverseStoreName;
    private final String valueStoreName;
    private final DependencyResolver<K, V> resolver;
    private final Converter<K, V, R> converter;

    MyTransformerSupplier(
            String reverseStoreName,
            String valueStoreName,
            DependencyResolver<K, V> resolver,
            Converter<K, V, R> converter
    ) {
        this.reverseStoreName = reverseStoreName;
        this.valueStoreName = valueStoreName;
        this.resolver = resolver;
        this.converter = converter;
    }

    @Override
    public Transformer<K, V, KeyValue<K, List<KeyValue<K, R>>>> get() {

        return new Transformer<K, V, KeyValue<K, List<KeyValue<K, R>>>>() {

            private KeyValueStore<K, Set<K>> reverseStore;
            private KeyValueStore<K, V> valueStore;

            @Override
            @SuppressWarnings("unchecked")
            public void
            init(ProcessorContext processorContext) {
                reverseStore = (KeyValueStore<K, Set<K>>) processorContext.getStateStore(reverseStoreName);
                valueStore = (KeyValueStore<K, V>) processorContext.getStateStore(valueStoreName);
            }

            @Override
            public KeyValue<K, List<KeyValue<K, R>>>
            transform(K k, V v) {

                ArrayList<KeyValue<K, R>> retn = new ArrayList<>();
                valueStore.put(k, v);

                Set<K> dependencies = resolver.getDependencies(k, v);

                //figure out if we can send this along
                Map<K, V> dependencyMap = new HashMap<>();
                boolean allDepsAvailable = getDependenciesRecursive(dependencies, dependencyMap, 0, 5);
                boolean ready = false;
                if (allDepsAvailable) {
                    ready = resolver.isResolved(k, v, dependencyMap);
                    if (ready) {
                        R converted;
                        if(converter != null) {
                            converted = converter.convert(k,v,dependencyMap);
                        } else {
                            converted = (R) v;
                        }
                        retn.add(new KeyValue<>(k, converted));
                    }
                }

                //update reverse lookup based on this object's dependencies
                for (K dependency : dependencies) {
                    Set<K> reverseDeps = reverseStore.get(dependency);
                    Set<K> newReverseDeps = concatenate(k, reverseDeps);
                    reverseStore.put(dependency, newReverseDeps);
                }

                //see if I can help anyone along
                if (ready) {
                    pushDependantsRecursive(k, v, retn);
                }

                return retn.isEmpty() ? null : new KeyValue<>(null, retn);
            }

            private void
            pushDependantsRecursive(
                    K k,
                    V v,
                    ArrayList<KeyValue<K, R>> retn
            ) {
                Set<K> dependants = reverseStore.get(k);
                if (dependants == null) {
                    return;
                }
                for (K dependant : dependants) {
                    V dependantValue = valueStore.get(dependant);
                    if (dependantValue != null) {
                        Set<K> dependantDependencies = resolver.getDependencies(dependant, dependantValue);
                        Map<K, V> dependantDependencyMap = new HashMap<>();
                        dependantDependencyMap.put(k, v);
                        boolean allDependantDepsAvailable =
                                getDependenciesRecursive(dependantDependencies, dependantDependencyMap, 0, 5);
                        if (allDependantDepsAvailable) {
                            boolean dependantReady =
                                    resolver.isResolved(dependant, dependantValue, dependantDependencyMap);
                            if (dependantReady) {
                                R converted;
                                if(converter != null) {
                                    converted = converter.convert(dependant,dependantValue, dependantDependencyMap);
                                } else {
                                    converted = (R) dependantValue;
                                }
                                retn.add(new KeyValue<>(dependant, converted));
                                pushDependantsRecursive(dependant, dependantValue, retn);
                            }
                        }
                    }
                }
            }

            private boolean
            getDependenciesRecursive(
                    Set<K> dependencies,
                    Map<K, V> map,
                    int depthLevel,
                    int maxDepth
            ) {
                if (depthLevel >= maxDepth) {
                    return true;
                }

                boolean retn = true;

                if (dependencies != null && !dependencies.isEmpty()) {
                    for (K depKey : dependencies) {
                        V depValue = map.get(depKey);
                        if (depValue == null) {
                            depValue = valueStore.get(depKey);
                        }
                        if (depValue == null) {
                            return false;
                        } else {
                            map.put(depKey, depValue);
                            Set<K> innerDeps = resolver.getDependencies(depKey, depValue);
                            retn = getDependenciesRecursive(innerDeps, map, depthLevel + 1, maxDepth);
                            if (retn == false) {
                                return false;
                            }
                        }
                    }
                }

                return retn;
            }

            @Override
            public KeyValue<K, List<KeyValue<K, R>>> punctuate(long l) {
                return null;
            }

            @Override
            public void close() {

            }
        };
    }
}
