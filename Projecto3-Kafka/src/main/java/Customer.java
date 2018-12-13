import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import database.Database;

public class Customer {
	
	public static void main(String[] args) {
		
		/*
		 * 
		 * Customer Producer properties
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
	  	 * Customer Customer properties
	  	 * 
	  	 */
	  	
	  	Properties ccprops = new Properties();
	      
	  	ccprops.put("bootstrap.servers", "localhost:9092");
	      
	  	ccprops.put("group.id", "test");
	      
	  	ccprops.put("enable.auto.commit", "true");
	      
	  	ccprops.put("auto.commit.interval.ms", "1000");
	      
	  	ccprops.put("session.timeout.ms", "30000");
	      
	  	ccprops.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	      
	  	ccprops.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	  	
	  	
	  	/* Create a topic for response */
	  	
	  	 byte[] array = new byte[7]; // length is bounded by 7
	     new Random().nextBytes(array);
	     String generatedTopic = new String(array, Charset.forName("UTF-8"));
	  
	     //System.out.println("Topic for replying: " + generatedTopic);
	  	
	  	/*
	  	 *  Creating Customer-Customer thread
	  	 *  
	  	 */
	  	
	  	Thread CustumerCustthread = new Thread(new CustomerCustomer(ccprops,"reply"));
	  	
	  	CustumerCustthread.start();
	  	
	  	/*
	  	 *  Creating Customer-Producer thread
	  	 */
	  	
	  	Thread CustumerProdthread = new Thread(new CustomerProducer(props,"reply"));
	  	
	  	CustumerProdthread.start();
	  	
	  	
	  	
		
	}
	
}

class CustomerProducer implements Runnable
{
	private final String produceTopic = "purchasestopic";
	private final String customerKey = "PURCHASE";
	private String topicToRespond;
	private Properties props;
	private Database db;
	
	public CustomerProducer(Properties prop, String topicRandom) {
		System.out.println("Thread create to send messages to " + produceTopic);
		this.props = prop;
		this.db = new Database();
		this.topicToRespond = topicRandom;
	}

	@SuppressWarnings("resource")
	@Override
	public void run() {
		
		
		Producer<String, String> producer = new KafkaProducer<>(this.props);
		System.out.println("To see whats in storage type in product: view ");
		Scanner keyboardIn = new Scanner(System.in);
		String productName, amount ,message;
		
		while(true)
		{
			System.out.println("Product:");
			productName = keyboardIn.next();
			
			
			if ( productName.equalsIgnoreCase("producer_close"))
				break;
			
			else if(productName.equalsIgnoreCase("view"))
			{
				System.out.println(db.itemList());
				
			}
			else
			{
				System.out.println("Amount:");
				amount = keyboardIn.next();
				
				message = amount + "," + topicToRespond;
				producer.send(new ProducerRecord<String, String>(this.produceTopic,productName,message));
				
				System.out.println("\nMessage sent successfully to topic " + this.produceTopic);
			}

			
		}

		producer.close();
		
		
	}
}

class CustomerCustomer implements Runnable
{
	
	/*
	 * TODO: Reply topic for diffe
	 * 
	 */
	
	private final String consumeTopic;
	private Properties props;
	
	CustomerCustomer(Properties prop, String consume)
	{
		System.out.println("Waiting messages from " + consume);
		this.props = prop;
		this.consumeTopic = consume;
		
	}

	@SuppressWarnings({ "resource", "deprecation" })
	@Override
	public void run() {
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		      
		//Kafka Consumer subscribes list of topics here.
    	consumer.subscribe(Arrays.asList(this.consumeTopic));
	      
    	//print the topic name
    	System.out.println("Subscribed to topic " + this.consumeTopic);
	     
	      
    	String receivedMessage;
      
    	while (true) {
	    	  
    	  /*
    	   * Poll(long timeout/ms)
    	   * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs
    	   * 
    	   * When one consumer is scubscribed to one or more topic use record.topic to know what to do
    	   * 
    	   */
    		
			ConsumerRecords<String, String> records = consumer.poll(100);
	         
    		for (ConsumerRecord<String, String> record : records) {
    			
    			receivedMessage = record.value();

    			
    			System.out.println("Message received: " + receivedMessage + "\n");
	         
    			
    		}
    		
	      }    
		
	}
	
}
