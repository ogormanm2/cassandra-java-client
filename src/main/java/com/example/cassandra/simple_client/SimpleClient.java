package com.example.cassandra.simple_client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class SimpleClient {
	private Cluster cluster;

	   public Cluster connect(String node) {
	      cluster = Cluster.builder()
	            .addContactPoint(node)
	            .build();
	      Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n", 
	            metadata.getClusterName());
	      for ( Host host : metadata.getAllHosts() ) {
	         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
	               host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	      
	      return cluster;
	   }

	   public void performcrud(Cluster cluster) {
		    // Connect to the cluster and keyspace "ogormanm"
	        //final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
	        //        .build();       
	        final Session session = cluster.connect("ogormanm");
	         
	        System.out.println("*********Cluster Information *************");
	        System.out.println(" Cluster Name is: " + cluster.getClusterName() );
	        System.out.println(" Driver Version is: " + cluster.getDriverVersion() );
	        System.out.println(" Cluster Configuration is: " + cluster.getConfiguration() );
	        System.out.println(" Cluster Metadata is: " + cluster.getMetadata() );
	        System.out.println(" Cluster Metrics is: " + cluster.getMetrics() );        
	         
	        // Retrieve all User details from Users table
	        System.out.println("\n*********Retrive User Data Example *************");        
	        getUsersAllDetails(session);
	         
	        // Insert new User into users table
	        System.out.println("\n*********Insert User Data Example *************");        
	        session.execute("INSERT INTO contact (email, name, phone, address, city) VALUES ('test@org.net', 'User Test', '860-326-1444', '123 Park Ln', 'Stamford')");
	        getUsersAllDetails(session);
	         
	        // Update user data in users table
	        System.out.println("\n*********Update User Data Example *************");        
	        session.execute("update contact set address = '120 Park Ln.' where email = 'test@org.net'");
	        getUsersAllDetails(session);
	         
	        // Delete user from users table
	        System.out.println("\n*********Delete User Data Example *************");        
	        session.execute("delete FROM contact where email = 'test@org.net'");
	        getUsersAllDetails(session);
	         
	        // Close Cluster and Session objects
	        System.out.println("\nIs Cluster Closed :" + cluster.isClosed());
	        System.out.println("Is Session Closed :" + session.isClosed());     
	        //cluster.close();
	        session.close();
	        //System.out.println("Is Cluster Closed :" + cluster.isClosed());
	        System.out.println("Is Session Closed :" + session.isClosed());
	   }
	   
	   private static void getUsersAllDetails(final Session inSession){        
	        // Use select to get the users table data
	        ResultSet results = inSession.execute("SELECT * FROM contact");
	        for (Row row : results) {
	            System.out.format("%s %s %s\n", row.getString("name"),
	                    row.getString("email"), row.getString("address"));
	        }
	    }
	   
	   public void close() {
	      cluster.close();
	   }

	   public static void main(String[] args) {
	      // Basic testing
		  SimpleClient client = new SimpleClient();
	      Cluster cluster = client.connect("127.0.0.1");
	            
	      // More comprehensive testing
	      client.performcrud(cluster);
	      
	      // all done close connection
	      cluster.close();
	      System.out.println("Is Cluster Closed :" + cluster.isClosed());
	   }
}
