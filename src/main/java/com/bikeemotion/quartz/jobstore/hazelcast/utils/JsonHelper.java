package com.bikeemotion.quartz.jobstore.hazelcast.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

    private JsonHelper() {
    }

    public static <T> String toJson (T value) throws IOException {

        try (StringWriter writer = new StringWriter(1024)) {
            OBJECT_MAPPER.writeValue(writer, value);
            return writer.toString();
        }
    }

    public static <T> JsonEnvelope toEnvelope (T value) throws IOException {

        return toEnvelope(value, null);
    }

    public static <T> JsonEnvelope toEnvelope (T value, String metaData) throws IOException {

        String jsonValue = toJson(value);
        return new JsonEnvelope(metaData, jsonValue);
    }

    public static <T> T toValueType (String jsonValue, Class<T> type) throws IOException {

        return OBJECT_MAPPER.readValue(jsonValue, type);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toPropertyMap (String jsonValue) throws IOException {

        return OBJECT_MAPPER.readValue(jsonValue, Map.class);
    }
}
