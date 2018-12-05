import java.util.Arrays;
import java.util.Properties;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

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
	  	
	  	/*
	  	 *  Creating Customer-Customer thread
	  	 *  
	  	 */
	  	
	  	Thread CustumerCustthread = new Thread(new CustomerCustomer(ccprops));
	  	
	  	CustumerCustthread.start();
	  	
	  	/*
	  	 *  Creating Customer-Producer thread
	  	 */
	  	
	  	Thread CustumerProdthread = new Thread(new CustomerProducer(props));
	  	
	  	CustumerProdthread.start();
	  	
	  	
	  	
		
	}
	
}

class CustomerProducer implements Runnable
{
	private final String produceTopic = "purchaseTopic";
	private Properties props;
	
	public CustomerProducer(Properties prop) {
		System.out.println("Thread create to send messages to " + produceTopic);
		this.props = prop;
	}

	@Override
	public void run() {
		
		// TODO wait for user input to write on a while loop and create a protocol 
		Producer<String, String> producer = new KafkaProducer<>(this.props);

		for(int i = 0; i < 1000; i++) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			producer.send(new ProducerRecord<String, String>(this.produceTopic, Integer.toString(i), Integer.toString(i)));
		}
	  
		System.out.println("\nMessage sent successfully to topic " + this.produceTopic);
		
		producer.close();
		
	}
}

class CustomerCustomer implements Runnable
{
	private final String  consumeTopic = "purchaseTopic";
	private Properties props;
	
	CustomerCustomer(Properties prop)
	{
		System.out.println("Waiting messages from " + consumeTopic);
		this.props = prop;
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		      
		//Kafka Consumer subscribes list of topics here.
    	consumer.subscribe(Arrays.asList(this.consumeTopic));
	      
    	//print the topic name
    	System.out.println("Subscribed to topic " + this.consumeTopic);
	     
    	/*
    	 * 
    	 * TODO: Check the method with the partition for topic
    	 * 
    	 * public ConsumerRecord(string topic,int partition, long offset,K key, V value)
    	 */
	      
      
    	while (true) {
	    	  
    	  /*
    	   * Poll(long timeout/ms)
    	   * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs
    	   * 
    	   */
    		ConsumerRecords<String, String> records = consumer.poll(100);
	         
    		for (ConsumerRecord<String, String> record : records) {
	         
    			// print the offset,key and value for the consumer records.
    			System.out.printf("offset = %d, key = %s, value = %s\n", 
	            record.offset(), record.key(), record.value());
    		}
    		
	      }    
		
	}
	
}