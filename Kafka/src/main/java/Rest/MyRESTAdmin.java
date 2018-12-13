package Rest;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;



public class MyRESTAdmin {
	public static void main(String[] args) {

		java.util.Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

		String message;
		Client client = ClientBuilder.newClient();


		WebTarget webTarget = client.target("http://localhost:9998/adminStuff");


		Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);

		//String response = builder.get(String.class);


		Scanner keyboardIn = new Scanner(System.in);
		//webTarget.request().post(Entity.entity("Jose", MediaType.TEXT_PLAIN));
		while(true){



			
			System.out.println("-----------------------------Admin Console-----------------------------------");
			System.out.println("Options:");
			System.out.println("1. Number of items ever sold.");
			System.out.println("2. Number of units sold of each item1.");
			System.out.println("3. Maximum price of each item sold so far.");
			System.out.println("4. Average number of purchases per order of supplies (for each item).");
			System.out.println("6. Revenue, expenses, and profit of the shop so far.");
			System.out.println("7. Item providing the highest profit over the last x minutes");
			System.out.println("8. Query over a range of products for average sales price.");
			System.out.println("0. Exit the program.");
			System.out.println("");
			System.out.print("Please select an option from 0-8\r\n");
			message = keyboardIn.next();


			
				//------------------ ADMIN ----------------//
				if(message.equalsIgnoreCase("quit")) {
					keyboardIn.close();
					break;
				}


				//Number of items ever sold.
				else if(message.equalsIgnoreCase("1"))
				{
					webTarget.path("numberItemsEverSold").request().get(String.class);
					//String result = webTarget.path("numberItemsEverSold").request().get(String.class);
					//System.out.println("Res:"+result);
				}

				//Number of units sold of each item1.
				else if(message.equalsIgnoreCase("2"))
				{
					// webTarget.request().post(Entity.entity("Jose", MediaType.TEXT_PLAIN));
					//System.out.println("Choose the time:");
					//int x = Integer.valueOf(keyboardIn.next());
					//System.out.println("Tempo:"+ x);
					
					
					//webTarget.path("numberItemsSoldEach").request().get(Entity.entity(x, MediaType.TEXT_PLAIN));

				}

				//Maximum price of each item sold so far.
				else if(message.equalsIgnoreCase("3"))
				{

				}

				//Average number of purchases per order of supplies (for each item).
				else if(message.equalsIgnoreCase("4"))
				{

				}

				//Revenue, expenses, and profit of the shop so far.
				else if(message.equalsIgnoreCase("5"))
				{

				}

				//Item providing the highest profit over the last x minutes.
				else if(message.equalsIgnoreCase("6"))
				{

				}

				//Query over a range of products for average sales price.
				else if(message.equalsIgnoreCase("7"))
				{

				}

				try {
					@SuppressWarnings("unchecked")
					List<String> response = builder.get(List.class);
					
					response.forEach(System.out::println);
				} catch (Exception e) {
					// TODO: handle exception
				}
				

				


			};
		
	}
}