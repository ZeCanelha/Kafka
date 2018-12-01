import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import database.Database;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class SimpleConsumer {
	
	
	
   public static void main(String[] args) throws Exception {
	  
      
      
	   
	   Database db = new Database();
	   System.out.println(db.itemList());
	   db.close();
	   
	  /*
	   * 
	   * Kafka consumer configuration settings
	   * 
	   */
	   
      String topicName = "resultstopic";
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "localhost:9092");
      
      props.put("group.id", "test");
      
      props.put("enable.auto.commit", "true");
      
      props.put("auto.commit.interval.ms", "1000");
      
      props.put("session.timeout.ms", "30000");
      
      props.put("key.deserializer", 
    		    "org.apache.kafka.common.serialization.StringDeserializer");
      
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      
      
      
      //KafkaConsumer Map 
      
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);
     
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
         
         for (ConsumerRecord<String, String> record : records)
         
         // print the offset,key and value for the consumer records.
         System.out.printf("offset = %d, key = %s, value = %s\n", 
            record.offset(), record.key(), record.value());
      }    
   }

}