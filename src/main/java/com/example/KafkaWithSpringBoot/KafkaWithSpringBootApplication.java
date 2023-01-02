package com.example.KafkaWithSpringBoot;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaWithSpringBootApplication {

//	private final Producer producer;
@Bean
NewTopic quotes(){
	return new NewTopic("quotes", 3, (short)3);
}
	@Bean
	NewTopic counts(){
		return new NewTopic("counts", 3, (short)3);
	}

	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(KafkaWithSpringBootApplication.class);
		application.setWebApplicationType(WebApplicationType.NONE);
		application.run(args);
	}

//	@Bean
//	public CommandLineRunner CommandLineRunnerBean() {
//		return (args) -> {
//			for (String arg : args) {
//				switch (arg) {
//					case "--producer":
//						this.producer.sendMessage("awalther", "t-shirts");
//						this.producer.sendMessage("htanaka", "t-shirts");
//						this.producer.sendMessage("htanaka", "batteries");
//						this.producer.sendMessage("eabara", "t-shirts");
//						this.producer.sendMessage("htanaka", "t-shirts");
//						this.producer.sendMessage("jsmith", "book");
//						this.producer.sendMessage("awalther", "t-shirts");
//						this.producer.sendMessage("jsmith", "batteries");
//						this.producer.sendMessage("jsmith", "gift card");
//						this.producer.sendMessage("eabara", "t-shirts");
//						break;
//					case "--consumer":
//						MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("myConsumer");
//						listenerContainer.start();
//						break;
//					default:
//						break;
//				}
//			}
//		};
//	}
//
//	@Autowired
//	KafkaWithSpringBootApplication(Producer producer) {
//		this.producer = producer;
//	}
//
//	@Autowired
//	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

}

@Component
class Processor {

	@Autowired
	public void process(final StreamsBuilder builder) {

		final Serde<String> stringSerde = Serdes.String();

		final KTable<String, Long> kTable = builder.stream("quotes", Consumed.with(stringSerde, stringSerde))
				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.groupBy(((key, value) -> value), Grouped.with(stringSerde, stringSerde))
				.count();
		final KStream<String, Long> counts = kTable
				.toStream();
		counts.print(Printed.toSysOut());

		counts.to("counts");

		// Kstreams joins test
		KStream<String, String> userRegions = builder.stream("streams-test1-input-topic-1", Consumed.with(Serdes.String(), Serdes.String()));
//		KStream<String, Long> regionMetrics = builder.stream("streams-test1-input-topic-2", Consumed.with(Serdes.String(), Serdes.Long()));
		KStream<String, String> regionMetrics = builder.stream("streams-test1-input-topic-2", Consumed.with(Serdes.String(), Serdes.String()));



		KStream<String,String> out = regionMetrics.join(userRegions,
				(regionValue, metricValue) -> regionValue + "/" + metricValue,
				JoinWindows.of(Duration.ofMinutes(5L)),
				StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
		);
		out.print(Printed.toSysOut());
		out.to("streams-test1-output-topic-1");


	}

}