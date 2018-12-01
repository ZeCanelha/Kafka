package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Database {
	
	
	private static  Connection connect = null;
    private static  PreparedStatement preparedStatement = null;
    private static  ResultSet resultSet = null;
    
    public Database()
    {
    	connect();
    }

	private void connect() {
		
		try
		   {
			   try {
				   
				   Class.forName("com.mysql.cj.jdbc.Driver");
				
			   } catch (ClassNotFoundException e) {
				
				   e.printStackTrace();
			   }
			   
	           // Setup the connection with the DB
			   
			   String connectionURL = "jdbc:mysql://localhost:3306/integration";
			   
			   connect = DriverManager.getConnection(connectionURL, "root", "administrator");
	           
	           
		   }catch(SQLException e)
		   {
			   System.out.println("Error connecting to database");
			   System.out.println(e.getMessage());
			   
		   }
	
		
	}
	public void close(){
		
        if(connect != null){
        	
            try {
                connect.close();
                
            } catch (SQLException e) {
            	
                e.printStackTrace();
            }
        }
    }
	
	public String itemList()
	{
		String sqlRequest = "SELECT * FROM SHOP";
		String return_string = " ";
		
		try {
			preparedStatement = connect.prepareStatement(sqlRequest);
			
			resultSet = preparedStatement.executeQuery();
			
			while( resultSet.next())
			{
				return_string += "Object Name: " + resultSet.getString("OBJECT_NAME") + "\n";
				return_string += "Price: " + resultSet.getString("PRICE") + "€\n";
				return_string += "Amount: " + resultSet.getString("Amount") + "\n";
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return return_string;
		
	}

}
