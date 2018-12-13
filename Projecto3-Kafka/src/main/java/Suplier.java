import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Suplier {
	
	public static void main(String[] args) 
	{
		
		/*
		 * 
		 * Suplier properties
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
	  	 * Producer properties
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
	  	
	  	
	  	
	  	
	  	
	  	String consumeTopic = "reordertopic";
	  	String produceTopic = "shipmentstopic";
	  	
	  	
	  	KafkaConsumer<String, Long> suplierConsumer = new KafkaConsumer<>(ccprops);
	  	suplierConsumer.subscribe(Arrays.asList(consumeTopic));
	  	
	  	while (true) 
	  	{
	  		
	  		
	  		ConsumerRecords<String, Long> records = suplierConsumer.poll(100); 
	  		
	  		for ( ConsumerRecord<String, Long> record : records )
	  		{
	  			
	  			/* 
	  			 * Read the new order, set the price automatically or by input
	  			 */
	  			readNewOrder(produceTopic, record.key(), record.value(), props);
	  			
	  			
	  			
	  			
	  		}
			
		}
		
	}
	
	static void readNewOrder(String toTopic, String key, Long value, Properties props)
	{
		
		System.out.println("Define price: ");
		Scanner sc = new Scanner(System.in);
		String price = sc.next();
		
		KafkaProducer<String, String> produceNewShipment = new KafkaProducer<>(props);
		String message = String.valueOf(value) + "-" + price;
		/* Send information */
		produceNewShipment.send(new ProducerRecord<String, String>(toTopic, key, message));
		System.out.println("Sending products...");
		produceNewShipment.close();

	}

}
