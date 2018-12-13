package Rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;



@Path("/students")
public class StudentsKeeper {
 
	
 
 	@Path("numberItemsEverSold")
 	@GET
 	public void NumberItemsSold()
 	{
 		
 		/* TODO: Print key.equals("Accepted");
 		 * 
 		 */
 		Properties props = new Properties();
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	  	props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_SSL");
	  	props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		KTable<String,Long> tables = topicStream.groupByKey().count();
 		
 		
 		
 		tables.toStream().foreach(new ForeachAction<String, Long>() {
 		    public void apply(String key, Long value) {
 		        System.out.println(key + ": " + value);
 		    }
 		 });
 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();

 	}
 	
 	@Path("numberItemsSoldEach")
 	@GET
 	public void NumberItemsSoldEach()
 	{
 		Properties props = new Properties();
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		KStream<String,String> newStream = topicStream.map((k,v) -> KeyValue.pair(v.split(",")[0],v.split(",")[1]));
 		
 		KTable<String, String> newLines = newStream.groupByKey()
 				.reduce((oldval,newval) -> Long.toString(Long.parseLong(oldval) + Long.parseLong(newval)) ,Materialized.as("MaxAmountItems"));
 		
 		
 		newStream.foreach(new ForeachAction<String, String>() {
 			
 		    public void apply(String key, String value) {
 		    	System.out.println(key + ":" + value);
 		        
 		    }  
 		 });
 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	}
 	
 	
 	
 	
 

}