package com.example.KafkaWithSpringBoot.serdes;

public class SerdesFactory {
    public static JsonNodeSerde jsonNodeSerde(){
        return new JsonNodeSerde();
    }

    public static ObjectNodeSerde objectNodeSerde(){
        return new ObjectNodeSerde();
    }
}
