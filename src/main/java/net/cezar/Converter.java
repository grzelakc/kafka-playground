package net.cezar;

import java.util.Map;
import java.util.Set;

/**
 * Created by cezargrzelak on 6/30/17.
 */

//K = IxRecordKey
//V = IxRecord
//R = Pair<IxPipelienKey, IxRecord>

public interface Converter<K, V, R> {
    R convert(K key, V value, Map<K, V> values);
}

///public class DelegatingConverter
//implements Converter<IxRecordKey, IxRecord, Pair<IxPipelineKey,IxRecord>>
//{
//
//