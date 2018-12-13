package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

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
            	System.out.println("Database already closed");
                
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
				return_string += "Amount: " + resultSet.getString("AMOUNT") + "\n";
			}
			preparedStatement.close();
			resultSet.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return return_string;
		
	}
	
	public void updateStorage(String prodcutName, String amount)
	{
		String sqlRequest = "UPDATE SHOP "
				+ "SET AMOUNT = ? "
				+ "WHERE OBJECT_NAME = ?";
		try {
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(2, prodcutName);
			preparedStatement.setString(1, amount);
			
			preparedStatement.executeUpdate();
			preparedStatement.close();
			
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void setStorage( String productName, String amount, String price  )
	{
		
		String sqlRequest = "INSERT INTO SHOP(OBJECT_NAME,AMOUNT,PRICE,IVALUE) VALUES(?,?,?,?)";
		
		try {
			
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(1, productName);
			preparedStatement.setString(2, amount);
			preparedStatement.setString(3, price);
			preparedStatement.setString(4, amount);
			
			preparedStatement.executeUpdate();
			preparedStatement.close();
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}	
	}
	
	public HashMap<String,String> checkStorageAmount(String productName)
	{
		HashMap<String,String> map = new HashMap<>();
		String sqlRequest = "SELECT AMOUNT, IVALUE FROM SHOP "
				+ "WHERE OBJECT_NAME = ?";
		
		try {
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(1, productName);
			
			resultSet = preparedStatement.executeQuery();
			
			if(resultSet.next())
			{
				map.put("Amount", resultSet.getString(1) );
				map.put("Ivalue", resultSet.getString(2));
			}
			
			preparedStatement.close();
			resultSet.close();
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
		
		return map;
	}
	
	public String getPrice( String productName )
	{
		String sqlRequest = "SELECT PRICE FROM SHOP WHERE OBJECT_NAME = ?";
		String price = null;
		
		try {
			
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(1, productName);
			
			resultSet = preparedStatement.executeQuery();
			
			if ( resultSet.next())
				price = resultSet.getString(1);
			
			preparedStatement.close();
			resultSet.close();
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
		return price;
	}
	
	public boolean hasProduct(String product)
	{
		String sqlRequest = "SELECT * FROM SHOP WHERE OBJECT_NAME = ? ";
		
		try {
			
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(1, product);
			resultSet = preparedStatement.executeQuery();
			
			if ( resultSet.next())
				return true;
			
		} catch (SQLException e) {
			System.out.println("Aqui ?" + e.getMessage());
			
		}
		
		return false;
	}
	

}
