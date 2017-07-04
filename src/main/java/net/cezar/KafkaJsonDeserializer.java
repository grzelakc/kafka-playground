package net.cezar;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Generic JSON deserializer.
 */
public abstract class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper;

    /**
     * Default constructor needed by Kafka
     */
    public KafkaJsonDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }


    @Override
    public T deserialize(String ignored, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            return objectMapper.readValue(bytes, getType());
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    protected abstract Class<T> getType();

    @Override
    public void close() {

    }
}