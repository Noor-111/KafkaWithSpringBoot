package com.example.KafkaWithSpringBoot.serdes;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serdes;

public class JsonNodeSerde extends Serdes.WrapperSerde<JsonNode> {

    public JsonNodeSerde() {
        super(new MyJsonSerializer<>(), new MyJsonDeserializer<>(JsonNode.class));
    }
}
