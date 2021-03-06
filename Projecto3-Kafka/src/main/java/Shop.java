import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import database.Database;



public class Shop {
	
	static boolean onHold = false;
	
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
	  	String replyTopic;
	  	HashMap<String, String> map;
	  	Thread waitForReply;
	  	
	  	
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
		  				map.put("Product", product);
		  				map.put("Price", db.getPrice(product));
		  				replyTopic = map.get("ReplyTopic");

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
		  					System.out.println("Entrei");

		  					sendReply(props, map, replyTopic);
		  					
		  					int percentage = (int) (ivalue * 0.25);
		  					
		  					if ((storageAmount - clientAmont) < percentage )
		  					{
		  						System.out.println("Low storage, reordering");

		  						int newOrder = ivalue;
		  						map.put("Amount", String.valueOf(newOrder));
		  						
		  						sendReorderRequest(map, "reordertopic");
		  						
		  						
		  					}
		  					else
		  					{
		  						
		  						/* update values on database */
		  						db.updateStorage(product, String.valueOf(storageAmount-clientAmont), map.get("Price"));

		  					}
		  					
		  				}
		  				
		  				else
		  				{
		  					// Reorder and re-send
		  					
		  					
		  					
		  					
		  				}
	  				}catch(NumberFormatException nfe)
	  				{
	  					System.out.println(nfe.getMessage());
	  				}
	  		
	  			}
	  			
	  			if ( record.topic().equals("shipmentstopic"))
	  			{
	  				
	  				String product = record.key();
	  				StringTokenizer str = new StringTokenizer(record.value(),"-");
	  				
	  				String amount = str.nextToken();
	  				String price = str.nextToken();
	  				
	  				/* Verificar se o produto existe na base de dados */
	  				
	  				if (db.hasProduct(product)) 
	  				{
	  					db.updateStorage(product, amount,price);
	  					System.out.println("Storage updated!");

	  				}
	  				else
	  				{
	  					System.out.println("Inserindo : " + product);
	  					
	  					/* Produto novo com taxa */
	  					
	  					int newPrice = Integer.parseInt(price);
	  					newPrice = (int) (newPrice * 1.3);
	  					db.setStorage(product, amount, String.valueOf(newPrice));
	  					
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
			else if ( i == 1)
				map.put("ReplyTopic", str.nextToken());		
				
			i++;
		}
		
		
		System.out.println(map.toString());
		return map;
		
	}
	
	static void sendReply(Properties props, HashMap<String,String> map, String topic )
	{
		String message = map.get("Product") +"," +map.get("Amount") + "," + map.get("Price");
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		producer.send(new ProducerRecord<String, String>("reply","Accepted",message));
		producer.send(new ProducerRecord<String, String>(topic,"Accepted",message));
		
		
		int revenue = Integer.parseInt(map.get("Amount")) * Integer.parseInt(map.get("Price"));
		producer.send(new ProducerRecord<String, String>("purchases", "Revenue", String.valueOf(revenue)));
		
		producer.close();
		
		System.out.println("Produtos enviados");
	}
	
	static void sendReorderRequest(HashMap<String,String> map, String topic)
	{
		Properties newProps = new Properties();
		newProps.put("bootstrap.servers", "localhost:9092");
		newProps.put("acks", "all");
		newProps.put("retries", 0);
		newProps.put("batch.size", 16384);
		newProps.put("linger.ms", 1);
		newProps.put("buffer.memory", 33554432);
		newProps.put("key.serializer", 
	    "org.apache.kafka.common.serialization.StringSerializer");
		newProps.put("value.serializer", 
	    "org.apache.kafka.common.serialization.LongSerializer");
	  	
	  	long amount = Long.parseLong(map.get("Amount"));
	  	
		Producer<String, Long> producer = new KafkaProducer<>(newProps);
		
		producer.send(new ProducerRecord<String, Long>(topic, map.get("Product"),amount));
		producer.close();
		
		System.out.println("Produtos em falta pedidos");
		
	}
}


