package Rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.json.JSONObject;



@Path("/students")
public class StudentsKeeper {
 	
 	@Path("numberItemsEverSold")
 	@GET
 	public List<String> NumberItemsSold()
 	{
 		
 		/* TODO: Return count
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
 		
 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	    List<String> ret = new ArrayList<>();
 	    
 	   try {
			Thread.sleep(2000);
			ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("MaxAmountItems", QueryableStoreTypes.keyValueStore());
			
			ret.add(keyValueStore.get("Accepted"));
			
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	   
 	   return ret;
 	    

 	}
 	
 	@Path("numberItemsSoldEach")
 	@GET
 	public List<String> NumberItemsSoldEach()
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
 		

 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	   List<String> obj = new ArrayList<>();
 	   
 	   	
 	    try {
			Thread.sleep(2000);
			ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("MaxAmountItems", QueryableStoreTypes.keyValueStore());
			KeyValueIterator<String, String> range = keyValueStore.all();
			   
			
			while (range.hasNext()) {
			   KeyValue<String, String> next = range.next();
			   obj.add("Product: " + next.key + "\nItems sold:" + next.value);
			   
			}
			range.close();
			
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	    
 	   return obj;

 	}
 	
 	@Path("maximumPrice")
 	@GET
 	public List<String> MaximumPrice()
 	{
 		Properties props = new Properties();
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		KStream<String,String> newStream = topicStream.map((k,v) -> KeyValue.pair(v.split(",")[0],v.split(",")[2]));
 		
 		KTable<String, String> newLines = newStream.groupByKey()
 				.reduce((oldval,newval) -> Long.toString(Max(Long.parseLong(oldval), Long.parseLong(newval))) ,Materialized.as("MaxPrice"));
 		

 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	   List<String> obj = new ArrayList<>();
 	   
 	    
 	    try {
			Thread.sleep(2000);
			ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("MaxPrice", QueryableStoreTypes.keyValueStore());
			KeyValueIterator<String, String> range = keyValueStore.all();
			   
			
			while (range.hasNext()) {
			   KeyValue<String, String> next = range.next();
			   
;
			   obj.add("Product: " + next.key + "\n Maximum Price sold:" + next.value);
			   
			}
			range.close();
			
			
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
 	    
 	   return obj;

 	}
 	
 	private long Max(long a, long b)
 	{
 		if ( a > b)
 			return a;
 		return b;
 	}
 	
 	@Path("revenueProfit")
 	@GET
 	public List<String> RevenueProfit()
 	{
 		Properties props = new Properties();
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("");
 		
 		KStream<String,String> newStream = topicStream.map((k,v) -> KeyValue.pair(v.split(",")[0],v.split(",")[2]));
 		
 		List<String> list = new ArrayList<>();
 		
 		return list;

 	}
 	
 	
 	
 

}