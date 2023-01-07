package com.example.KafkaWithSpringBoot.serdes;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;

public class ObjectNodeSerde extends Serdes.WrapperSerde<ObjectNode> {

    public ObjectNodeSerde() {
        super(new MyJsonSerializer<>(), new MyJsonDeserializer<>(ObjectNode.class));
    }
}
