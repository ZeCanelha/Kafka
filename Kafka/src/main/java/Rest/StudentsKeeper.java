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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;



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
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
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
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		
 		topicStream.foreach(new ForeachAction<String, String>() {
 			
 		    public void apply(String key, String value) {
 		    	System.out.println(key + ":" + value);
 		        
 		    }  
 		 });
 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	}
 	
 	
 	
 	
 

}