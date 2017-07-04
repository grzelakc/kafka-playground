package net.cezar;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by cezargrzelak on 6/29/17.
 */
public interface DependencyResolver<K, V> {

    Set<K> getDependencies(K key, V value);

    boolean isResolved(K key, V value, Map<K,V> dependencies);
}

