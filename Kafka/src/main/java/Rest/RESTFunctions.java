package Rest;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

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




@Path("/admin")
public class RESTFunctions {
	
	private Properties props = new Properties();
	private static List<String> newList;
	
	public RESTFunctions() {
		this.props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	this.props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	  	this.props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_SSL");
	  	this.props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	  	this.props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
	}
	
	
 	
 	@Path("numberItemsEverSold")
 	@GET
 	@Produces(MediaType.APPLICATION_JSON)
 	public List<String> NumberItemsSold()
 	{
 		
 		/* TODO: Return count
 		 * 
 		 */
 				
 		StreamsBuilder builder = new StreamsBuilder();
 		
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		KStream<String,String> newStream = topicStream.map((k,v) -> KeyValue.pair("Number",String.valueOf(1)));
 		KTable<String, String> newLines = newStream.groupByKey()
 				.reduce((oldval,newval) -> Long.toString(Long.parseLong(oldval) + Long.parseLong(newval)) ,Materialized.as("ItemsSold"));
 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	    List<String> ret = new ArrayList<>();
 	    
 	   try {
			Thread.sleep(2000);
			ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("ItemsSold", QueryableStoreTypes.keyValueStore());
			
			ret.add(keyValueStore.get("Number"));
			System.out.println("Sold: " + keyValueStore.get("Number"));
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	   
 	   return ret;
 	    

 	}
 	
 	@Path("numberItemsSoldEach")
 	@GET
 	@Produces(MediaType.APPLICATION_JSON)
 	public List<String> NumberItemsSoldEach()
 	{
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		KStream<String,String> newStream = topicStream.map((k,v) -> KeyValue.pair(v.split(",")[0],v.split(",")[1]));
 		
 		KTable<String, String> newLines = newStream.groupByKey()
 				.reduce((oldval,newval) -> Long.toString(Long.parseLong(oldval) + Long.parseLong(newval)) ,Materialized.as("MaxAmountItems"));
 		

 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 	    
 	    List<String> obj = new ArrayList<>();
 	   
		ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("MaxAmountItems", QueryableStoreTypes.keyValueStore());
		KeyValueIterator<String, String> range = keyValueStore.all();
		
		
		while (range.hasNext()) {
		   KeyValue<String, String> next = range.next();
		   obj.add("Product: " + next.key + "\nItems sold:" + next.value);
		   System.out.println("Product:" + next.key + "nItems sold:" + next.value);
		   
		}
		range.close();
			
 	   	return obj;
 	   
 	}
 	
 	@Path("maximumPrice")
 	@GET
 	@Produces(MediaType.APPLICATION_JSON)
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
	   
		ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("MaxPrice", QueryableStoreTypes.keyValueStore());
		KeyValueIterator<String, String> range = keyValueStore.all();
	   
		
		while (range.hasNext()) {
			KeyValue<String, String> next = range.next();
	   
			obj.add("Product: " + next.key + "\n Maximum Price sold:" + next.value);
		   
		}
		range.close();
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
 	@Produces(MediaType.APPLICATION_JSON)
 	public HashMap<String,String> RevenueProfit()
 	{
 		HashMap<String,String> obj = new HashMap<>();
 		
 		Properties props = new Properties();
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("purchases");
 		
 		KTable<String, String> purchasesStream = topicStream.groupByKey()
 				.reduce((oldval,newval) -> Long.toString(Long.parseLong(oldval) + Long.parseLong(newval)) ,Materialized.as("economics"));
 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 		

		ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("economics", QueryableStoreTypes.keyValueStore());
		KeyValueIterator<String, String> range = keyValueStore.all();
		   
		int spent = 0, revenue = 0;
		while (range.hasNext()) {
		   KeyValue<String, String> next = range.next();
		   

		   if (next.key.equalsIgnoreCase("revenue")) {
			   obj.put(next.key, next.value);
			  
		   } else {
			   obj.put(next.key, next.value);
		  }
		}
		range.close();
		
		if (obj.containsKey("Spent"))
		{
			spent = Integer.parseInt(obj.get("Spent"));
		}
		if (obj.containsKey("Revenue"))
		{
			revenue = Integer.parseInt(obj.get("Revenue"));
		}
		
		System.out.println("Revenue: " +revenue+"\nSpent: " + spent + "\nProfit: " + (revenue - spent));
		

 		return obj;

 	}
 	
 	@Path("averagesSalesPrice")
 	@GET
 	@Produces(MediaType.APPLICATION_JSON)
 	public List<String> averageSales()
 	{
 		List<String> string = new ArrayList<>();
 		
 		
 		Properties props = new Properties();
 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Rest-Service");
	  	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
 		StreamsBuilder builder = new StreamsBuilder();
 		KStream<String,String> topicStream = builder.stream("reply");
 		
 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
 	    streams.start();
 		
 	    
 	    
 	    
 		return string;
 	}
 	
 	
 	
 	
 

}