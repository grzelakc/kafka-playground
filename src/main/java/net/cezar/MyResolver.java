package net.cezar;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by cezargrzelak on 7/3/17.
 */
class MyResolver<K,V> implements DependencyResolver<K, V> {

    @Override
    public Set<K> getDependencies(K in_key, V value) {
        String key = (String) in_key;
        if (key.startsWith("foo")) {
            String num = key.replace("foo", "");
            return (Set<K>) Sets.newHashSet( "bar" + num , "qux" + num );
        } else if (key.startsWith("bar")) {
            String num = key.replace("bar", "");
            return (Set<K>) Sets.newHashSet("baz" + num);
        } else if (key.startsWith("bob")) {
            String num = key.replace("bob", "");
            return (Set<K>) Sets.newHashSet("qux" + num, "baz" + num);
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public boolean isResolved(K in_key, V value, Map<K,V> values) {
        String key = (String) in_key;
        if (key.startsWith("foo")) {
            String num = key.replace("foo", "");
            return values.containsValue("barValue" + num) && values.containsValue("quxValue" + num);
        } else if (key.startsWith("bar")) {
            String num = key.replace("bar", "");
            return values.containsValue("bazValue" + num);
        } else if(key.startsWith("bob")) {
            String num = key.replace("bob", "");
            return values.containsValue("bazValue" + num) && values.containsValue("quxValue" + num);
        } else
            {
            return true;
        }
    }
}
