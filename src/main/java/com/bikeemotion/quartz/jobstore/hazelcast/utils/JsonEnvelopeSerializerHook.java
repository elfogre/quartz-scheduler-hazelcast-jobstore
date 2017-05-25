package com.bikeemotion.quartz.jobstore.hazelcast.utils;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

public class JsonEnvelopeSerializerHook implements SerializerHook<JsonEnvelope> {

    private static final int JSON_ENVELOPE_TYPE = 1000;

    @Override
    public Class<JsonEnvelope> getSerializationType () {
        return JsonEnvelope.class;
    }

    @Override
    public boolean isOverwritable () {
        return false;
    }

    @Override
    public Serializer createSerializer () {
        return new JsonEnvelopeSerializer();
    }

    private static class JsonEnvelopeSerializer implements StreamSerializer<JsonEnvelope> {

        @Override
        public void write (ObjectDataOutput out, JsonEnvelope object) throws IOException {

            String metaData = object.getMetaData();
            String jsonValue = object.getJsonValue();
            out.writeUTF(metaData);
            out.writeUTF(jsonValue);
        }

        @Override
        public JsonEnvelope read (ObjectDataInput in) throws IOException {

            String metaData = in.readUTF();
            String jsonValue = in.readUTF();
            return new JsonEnvelope(metaData, jsonValue);
        }

        @Override
        public int getTypeId () {
            return JSON_ENVELOPE_TYPE;
        }

        @Override
        public void destroy () {
        }
    }

}
