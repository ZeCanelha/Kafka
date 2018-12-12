import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
	  	ccprops.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	  	
	  	
	  	/* 
	  	 * 
	  	 * Initialization
	  	 * 
	  	 */
	  	
	  	Database db = new Database();
	  	String message;
	  	HashMap<String, String> map;
	  	
	  	KafkaConsumer<String, String> shopConsumer = new KafkaConsumer<>(ccprops);
  		shopConsumer.subscribe(topics);
	  	
	  	while(true)
	  	{
	  		
	  		ConsumerRecords<String, String> records = shopConsumer.poll(100);
	         
	  		
	  		
	  		for (ConsumerRecord<String, String> record : records) {
    			
    			
	  			if ( record.topic().equals("purchasestopic"))
	  			{
	  				
	  				try
	  				{
	  					String product = record.key();
		  				map = parser(record.value());

		  				/* check storage  and reply accordingly guardar o amount para actualizar na bd */
		  				
		  				HashMap<String,String> storageCheck = db.checkStorageAmount(product);
		  				
		  				int clientAmont = Integer.parseInt(map.get("Amount"));
		  				int storageAmount = Integer.parseInt(storageCheck.get("Amount"));
		  				int ivalue = Integer.parseInt(storageCheck.get("Ivalue"));

		  				
		  				/*
		  				 *  Se houver stock disponivel enviar mensagem para o cliente
		  				 *  Se depois da compra o stock baixar para - de 25% reabastecer
		  				 */
		  				if (  storageAmount >= clientAmont  )
		  				{
		  					/* TODO: send to customer */
		  					
		  					
		  					
		  					/* check percentage 
		  					 * TODO: check if float is better */
		  					int percentage = (int) (ivalue * 0.25);
		  					
		  					if ((storageAmount - clientAmont) < percentage )
		  					{
		  						/* Reorder */
		  						
		  						
		  					}
		  					else
		  					{
		  						/* update values on database */
		  						db.updateStorage(product, String.valueOf(storageAmount-clientAmont));
		  						
		  						
		  					}
		  					
		  				}
		  				
		  				else
		  				{
		  					
		  					/* wait/notify
		  					 * 
		  					 */
		  					
		  					/* TODO: E dar wait ate q responda request more items from suplier */
		  				}
	  				}catch(NumberFormatException nfe)
	  				{
	  					System.out.println(nfe.getMessage());
	  				}
	  				
	  				

	  			}
	  			
	  			if ( record.topic().equals("shipmentstopic"))
	  			{
	  				
	  				/* TODO: Update items in database
	  				 * TODO: Se for preciso passar na mensagem se é uma ordem nova ou se é reabastecimento 
	  				 *  
	  				*/
	  				
	  				String product = record.key();
	  				StringTokenizer str = new StringTokenizer(record.value(),"-");
	  				
	  				String amount = str.nextToken();
	  				String price = str.nextToken();
	  				
	  				/* Verificar se o produto existe na base de dados */
	  				
	  				if (db.hasProduct(product)) 
	  				{
	  					/* TODO: verificar a cena do preço */
	  					
	  					
	  					
	  				}
	  				else
	  				{
	  					System.out.println("Inserindo : " + product);
	  					/* Produto novo */
	  					db.setStorage(product, amount, price);
	  					
	  				}
	  				
	  				
	  				
	  				
	  				
	  			}
	  				
	         
    			
    		}
	  		
	  	}
		
	}
	
	static HashMap<String,String> parser (String message)
	{
		
		StringTokenizer str = new StringTokenizer(message, ",");
		
		
		HashMap<String, String> map = new HashMap<>();
		
		int i = 0;
		
		while(str.hasMoreTokens())
		{
			if ( i == 0)
			{
				map.put("Amount", str.nextToken());
			}
			else
				map.put("Price", str.nextToken());
			i++;
		}
		
		
		System.out.println(map.toString());
		return map;
		
	}
}
	

