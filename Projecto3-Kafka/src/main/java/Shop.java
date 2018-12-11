import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.xml.crypto.Data;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

import database.Database;



public class Shop {
	
	
	
	public static void main(String[] args) {
		
		
		/* Topics List */
		List<String> topics = new ArrayList<>();
		topics.add("purchasestopic");
		topics.add("shipmentstopic");
		
		/* 
		 * 
		 * Shop Producer to ReplyTopic
		 * 
		 */
		
		Properties props = new Properties();
		//Assign localhost id
		props.put("bootstrap.servers", "localhost:9092");
		//Set acknowledgements for producer requests.      
		props.put("acks", "all");
		//If the request fails, the producer can automatically retry,
		props.put("retries", 0);
		//Specify buffer size in config
		props.put("batch.size", 16384);
	  	//Reduce the no of requests less than 0   
	  	props.put("linger.ms", 1);
	  	//The buffer.memory controls the total amount of memory available to the producer for buffering.   
	  	props.put("buffer.memory", 33554432);
	  	// types of values
	  	props.put("key.serializer", 
	    "org.apache.kafka.common.serialization.StringSerializer");
	  	props.put("value.serializer", 
	    "org.apache.kafka.common.serialization.StringSerializer");
	  	
	  	/*
	  	 * 
	  	 * Shop Consumer
	  	 * 
	  	 */
	  	
	  	
	  	Properties ccprops = new Properties();
	      
	  	ccprops.put("bootstrap.servers", "localhost:9092");	      
	  	ccprops.put("group.id", "test");	      
	  	ccprops.put("enable.auto.commit", "true");	      
	  	ccprops.put("auto.commit.interval.ms", "1000");	      
	  	ccprops.put("session.timeout.ms", "30000");      
	  	ccprops.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");    
	  	ccprops.put("value.deserializer","org.apache.kafka.common.serialization.LongDeserializer");
	  	
	  	
	  	/* 
	  	 * 
	  	 * Initialization
	  	 * 
	  	 */
	  	
	  	Database db = new Database();
	  	Long amount;
	  	
	  	while(true)
	  	{
	  		KafkaConsumer<String, Long> shopConsumer = new KafkaConsumer<>(props);
	  		shopConsumer.subscribe(topics);
	  		
	  		ConsumerRecords<String, Long> records = shopConsumer.poll(100);
	         
	  		
	  		for (ConsumerRecord<String, Long> record : records) {
    			
    			
	  			if ( record.topic().equals("purchasestopic"))
	  			{
	  				String product = record.key();
	  				amount = record.value();
	  				
	  				/* check storage  and reply accordingly */
	  				
	  				if ( db.checkStorageAmount(product) > amount)
	  				{
	  					/* send to customer */
	  					
	  					
	  				}
	  				
	  				else
	  				{
	  					/* request more items from suplier */
	  				}

	  			}
	  			
	  			if ( record.topic().equals("shipmentstopic"))
	  			{
	  				
	  			}
	  				
	         
    			
    		}
	  		
	  	}
	  	

	  	
	
		
	}
}
	

