package net.cezar;

import java.util.Map;

/**
 * Created by cezargrzelak on 7/4/17.
 */
public class MyConverter<K, V, R> implements Converter<K, V, R> {
    @Override
    public R convert(K key, V value, Map<K, V> values) {
        return (R) value;
    }
}
