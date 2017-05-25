package com.bikeemotion.quartz.jobstore.hazelcast.utils;


public class JsonEnvelope {
    
    private final String metaData;
    private final String jsonValue;

    public JsonEnvelope(String metaData, String jsonValue) {
        this.metaData = metaData;
        this.jsonValue = jsonValue;
    }

    public String getMetaData() {
        return metaData;
    }

    public String getJsonValue() {
        return jsonValue;
    }

    @Override
    public String toString() {
        return "JsonEnvelope{" +
                "metaData='" + metaData + '\'' +
                ", jsonValue='" + jsonValue + '\'' +
                '}';
    }
}
