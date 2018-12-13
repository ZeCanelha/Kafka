package Rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.json.JSONObject;



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
 	public JSONObject NumberItemsSoldEach()
 	{
 		Properties props = new Properties();
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		KStream<String,Long> newStream = topicStream.map((k,v) -> KeyValue.pair(v.split(",")[0],Long.parseLong(v.split(",")[1])));
 		
 		KTable<String, Long> newLines = newStream.groupByKey()
 				.reduce((oldval,newval) -> oldval + newval ,Materialized.as("MaxAmountItems"));
 		

 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	   JSONObject obj = new JSONObject();
 	   
 	    
 	    try {
			Thread.sleep(2000);
			ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store("MaxAmountItems", QueryableStoreTypes.keyValueStore());
			KeyValueIterator<String, Long> range = keyValueStore.all();
			   
			
			while (range.hasNext()) {
			   KeyValue<String, Long> next = range.next();
			   
			   System.out.println("Product" + next.key + "Items sold: " + next.value);
			   obj.put(next.key,next.value);
			   
			}
			range.close();
			
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	    
 	   return obj;

 	}
 	
 	
 	
 	
 

}