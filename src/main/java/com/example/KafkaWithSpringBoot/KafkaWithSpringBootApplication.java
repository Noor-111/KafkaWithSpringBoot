package com.example.KafkaWithSpringBoot;

import com.example.KafkaWithSpringBoot.serdes.SerdesFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

//		final KTable<String, Long> kTable = builder.stream("quotes", Consumed.with(stringSerde, stringSerde))
//				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
//				.groupBy(((key, value) -> value), Grouped.with(stringSerde, stringSerde))
//				.count();
//		final KStream<String, Long> counts = kTable
//				.toStream();
//		counts.print(Printed.toSysOut());
//
//		counts.to("counts");

		// Kstreams joins test
//		KStream<String, String> userRegions = builder.stream("streams-test1-input-topic-1", Consumed.with(Serdes.String(), Serdes.String()));
////		KStream<String, Long> regionMetrics = builder.stream("streams-test1-input-topic-2", Consumed.with(Serdes.String(), Serdes.Long()));
//		KStream<String, String> regionMetrics = builder.stream("streams-test1-input-topic-2", Consumed.with(Serdes.String(), Serdes.String()));



//		KStream<String,String> out = regionMetrics.join(userRegions,
//				(regionValue, metricValue) -> regionValue + "/" + metricValue,
//				JoinWindows.of(Duration.ofMinutes(5L)),
//				StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
//		);
//		out.print(Printed.toSysOut());
//		out.to("streams-test1-output-topic-1");
//		int count = 10;
		final Serde<JsonNode> jsonSerde = SerdesFactory.jsonNodeSerde();//Serdes.serdeFrom(new JsonSerializer<JsonNode>(), new JsonDeserializer<JsonNode>());
		final Serde<ArrayNode> arrayNodeSerde = Serdes.serdeFrom(new JsonSerializer<ArrayNode>(), new JsonDeserializer<ArrayNode>());
		final Serde<ObjectNode> objectNodeSerde = SerdesFactory.objectNodeSerde();//Serdes.serdeFrom(new JsonSerializer<ObjectNode>(), new JsonDeserializer<ObjectNode>());

		final KTable<String,ObjectNode> employerTable =
				builder.stream("EMPLOYER",Consumed.with(Serdes.String(),objectNodeSerde)).toTable();
		final KStream<String,JsonNode> employeeStream =
				builder.stream("EMPLOYEE",Consumed.with(Serdes.String(),jsonSerde));
		final KTable<String, JsonNode> employeeTable =
				employeeStream.groupByKey(Grouped.with(Serdes.String(), jsonSerde))
						.aggregate(
								() -> {
										ObjectMapper mapper = new ObjectMapper();
										return mapper.createArrayNode();
								},
								(key,val,aggrJson) -> {
									return ((ArrayNode)aggrJson).add(val);
								},
								Materialized.<String,JsonNode,KeyValueStore<Bytes, byte[]>>
										as("EMPLOYEES_AGGR_MV")
										.withKeySerde(Serdes.String())
										.withValueSerde(jsonSerde)

						);

		final KTable<String,ObjectNode> resultKTable =
				employerTable.join(employeeTable,
						//value joiner
						(employer,employeeAggr) -> {
							if(employeeAggr != null){
								employer.putArray("employees").addAll((ArrayNode)employeeAggr);
							}
							return employer;
						},
						Materialized.<String,ObjectNode,KeyValueStore<Bytes, byte[]>>
										as("EMPLOYER_RES_MV")
								.withKeySerde(stringSerde)
								.withValueSerde(objectNodeSerde)

				).filter((k,v)-> isAggregateComplete(v));//completeness condition
//						(v.get("employees") == v.get("count").asInt());//completeness condition

		resultKTable.
				toStream()
				.peek((key,val)-> System.out.println("key= "+key +"val= "+val.toPrettyString()))
				.to("EMPLOYER_WITH_EMPLOYEES",Produced.with(stringSerde,objectNodeSerde));

//		-------------------------String AGGR Example Start----------------------------------------
//		final KTable<String,String> oneTable =
//				builder.stream("streams-test1-input-topic-2",Consumed.with(Serdes.String(),Serdes.String())).toTable();
//
//		final KTable<String, String> aggrTable =
//				builder.stream("streams-test1-input-topic-1",Consumed.with(Serdes.String(),Serdes.String()))
//						.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//						.aggregate(
//								String::new,
//								(key,val,aggregate) -> {
////									count++;
//									return aggregate.concat(val);
//								},
//								Materialized.as("AGGR-MV")
//						);
//
//		KStream<String,String> aggrStream = aggrTable.toStream().filter((key,val)->val.contains("val-28"));
//		aggrStream.print(Printed.toSysOut());
//
//		KTable<String,String> aggrTable2 = aggrStream.toTable();
//
//		final KTable<String, String > resultTable =
//				oneTable.leftJoin(aggrTable2,
//						(result,aggr) -> {
//							if(aggr != null)
//								return result+aggr;
//							return null;
//						}
//				);
//		resultTable.toStream().filter((key,val)->val!=null).to("streams-test1-output-topic-1");
//------------------------------------STRING AGGR EX END-----------------------------------------------------------------------
//		aggrStream.to("streams-test1-output-topic-1");

//        final KTable<Integer, EmploymentHistoryAggregationDto> employmentHistoryAggr =
//                builder.stream("EMPLOYMENT-HISTORY",
//                                Consumed.with(Serdes.Integer(), MySerdesFactory.employeeHistorySerde()))
//                        .selectKey((key,empHist) -> empHist.getEmpId())
//                        .groupByKey(Grouped.with(Serdes.Integer(), MySerdesFactory.employeeHistorySerde()))
//                        .aggregate(
//                                // Initialized Aggregator
//                                EmploymentHistoryAggregationDto::new,
//                                //Aggregate
//                                (empId, empHist, empHistAggr) -> {
//                                    empHistAggr.setEmpId(empId);
//                                    empHistAggr.add(empHist.getEmployerName());
//                                    return empHistAggr;
//                                },
//                                // store in materialied view EMPLOYMENT-HIST-AGGR-MV
//                                Materialized.<Integer, EmploymentHistoryAggregationDto, KeyValueStore<Bytes, byte[]>>
//                                                as("EMPLOYMENT-HIST-AGGR-MV")
//                                        .withKeySerde(Serdes.Integer())
//                                        .withValueSerde(MySerdesFactory.employmentHistoryAggregationSerde())
//                        );


	}

	/**
	 *
	 * @param v
	 * @return
	 * Checks for the completeness condition
	 * The JSON node is considered complete if the "employees" array field has the
	 * number of elements same as the value of "count" field
	 */
	private static boolean isAggregateComplete(ObjectNode v) {
		if(v == null)
			return false;

		int count =0;
		int requiredNo = v.get("count").asInt();
		JsonNode nodeArray = v.get("employees");
		if(nodeArray.isArray()){
			for (JsonNode node: nodeArray) {
				count++;
			}
		}

		if(count == requiredNo )
			return true;
		return false;
	}

}