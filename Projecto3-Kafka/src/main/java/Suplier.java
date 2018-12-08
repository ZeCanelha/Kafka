

/*
 * 
 * Define suplier as a kafka streaming
 * 
 * Read from re-order
 * Send for shipments topic
 * 
 */

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;


public class Suplier {
	
	public static void main(String[] args) {
		
		final String readFromTopic = "ReorderTopic";
		final String writeToTopic = "ShipmentsTopic";
		final String bootstrapServers = "127.0.0.1:9092";
		
		final Properties streamsConfiguration = new Properties();
		// The name must be unique in the Kafka cluster against which the application is run.
	    
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "suplier-id");
	    // Where to find Kafka broker(s).
	    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    // Specify default (de)serializers for record keys and for record values.
	    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
	    
	    StreamsBuilder builder = new StreamsBuilder();
	    KStream<String, Long> stream = builder.stream(readFromTopic);
	    
	    KTable<String, Long> outlines = stream.groupByKey().count();

	    outlines.mapValues(v -> "" + v).toStream().to(writeToTopic, Produced.with(Serdes.String(), Serdes.String()));
	       
	    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
	    
	    streams.start();
	    System.out.println("Reading stream from topic " + readFromTopic);
	    

	}
	

}
