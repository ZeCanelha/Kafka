//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;
import java.util.Scanner;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

public class Owner {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		String produceTopic = "ReorderTopic";
		Scanner keybordIn = new Scanner(System.in);
	  	String reorderMessage;
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
	  	props.put("buffer.memory", 33554432);
	  
	  	// types of values

	  	props.put("key.serializer", 
	    "org.apache.kafka.common.serialization.StringSerializer");

	  	props.put("value.serializer", 
	    "org.apache.kafka.common.serialization.StringSerializer");
	  	
	  	
	  	Producer<String, String> producer = new KafkaProducer<>(props);
	  	
	  	/* 
	  	 * 
	  	 * TODO: Perguntar se este actor tem q estar sempre on e.g. num while ou
	  	 * só correr qd for preciso mandar um pedido.
	  	 * TL:DR Sempre ativo ou um pedido de cada vez
	  	 * 
	  	 */
	  	
	  	
	  	System.out.println("Place your reorder message here: ");
	  	reorderMessage = keybordIn.next();
	  	producer.send(new ProducerRecord<String, String>(produceTopic,reorderMessage));
	  	
	  	producer.close();
	  	
	  	
	  	
	  	
	  	
		
		
	}

}
