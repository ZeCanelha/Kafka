package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Database {
	
	/*
	 * TODO: Add logger to debug
	 */
	
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
				return_string += "Amount: " + resultSet.getInt("AMOUNT") + "\n";
			}
			preparedStatement.close();
			resultSet.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return return_string;
		
	}
	
	public void updateStorage(String prodcutName, int amount)
	{
		String sqlRequest = "UPDATE SHOP "
				+ "SET AMOUNT = ? "
				+ "WHERE OBJECT_NAME = ?";
		try {
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(1, prodcutName);
			preparedStatement.setInt(2, amount);
			
			preparedStatement.executeUpdate(sqlRequest);
			preparedStatement.close();
			
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void setStorage( String productName, int amount, String price  )
	{
		
		String sqlRequest = "INSERT INTO SHOP(OBJECT_NAME,AMOUNT,PRICE) "
				+ "VALUES(?,?,?)";
		
		try {
			
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(1, productName);
			preparedStatement.setInt(2, amount);
			preparedStatement.setString(3, price);
			
			preparedStatement.executeUpdate(sqlRequest);
			preparedStatement.close();
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}	
	}
	
	public int checkStorageAmount(String productName)
	{
		int amount = 0;
		String sqlRequest = "SELECT AMOUNT FROM SHOP "
				+ "WHERE OBJECT_NAME = ?";
		
		try {
			preparedStatement = connect.prepareStatement(sqlRequest);
			preparedStatement.setString(1, productName);
			
			resultSet = preparedStatement.executeQuery();
			
			if(resultSet.next())
			{
				amount = resultSet.getInt(1);
			}
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
		
		return amount;
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
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
		return price;
	}

}
