import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;


public class SimpleStreamsExercises {

	 public static void main(String[] args) throws InterruptedException, IOException {
		 
		  String topicName = args[0].toString();
		  String outtopicname = "resultstopic";
		
		  java.util.Properties props = new Properties();
		  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
		  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		  
		  //Specify default (de)serializers for record keys and for record values.
		  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
		  
		  
		  StreamsBuilder builder = new StreamsBuilder();
		  
		  
		  //  Get the messages< Key, Value> from the topic 
		  
		  KStream<String, Long> lines = builder.stream(topicName);
		
		  	
		  
		  KTable<String, Long> outlines = lines.groupByKey().count();
		
		  
		  // Write the message to the output topic 
		  
		  // Serdes -> Set up serializers and deserializers, in this case both of them are overwritten to be string type
		  
		  outlines.mapValues(v -> "" + v).toStream().to(outtopicname, Produced.with(Serdes.String(), Serdes.String()));
		     
		  KafkaStreams streams = new KafkaStreams(builder.build(), props);
		  streams.start();
		  
		  System.out.println("Reading stream from topic " + topicName);
		  
	 }
}