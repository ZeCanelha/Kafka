import java.io.InterruptedIOException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Owner {
	
	public static void main(String[] args) 
	{
		
		String topic = "reordertopic";
		
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
	    "org.apache.kafka.common.serialization.LongSerializer");
	  	
	  	Producer<String, Long> ownerProducer = new KafkaProducer<>(props);
	  	
	  	Scanner sc = new Scanner(System.in);
	  	String keyboardInput;
	  	Long amount;
	  	

	  	
		System.out.println("Order new produts");
		System.out.println("Product name: ");
	  	keyboardInput = sc.next();
	  	System.out.println("Product amount: ");
	  	amount = sc.nextLong();
	  	
	  	ownerProducer.send(new ProducerRecord<String, Long>(topic,keyboardInput, amount));

  		ownerProducer.close();
	  		
		
		
	}

}
