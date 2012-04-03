package nl.kaninefaten.cassandra.tutorial.t1;

import java.io.File;


/**
 * Demonstrates start en stop of the internal server.
 * <p>
 * Note:
 * Stopping the server like this will not stop the jvm.<br/>
 * See tutorial 2 for starting and stopping the server in test cases.
 * 
 * 
 * @author Patrick van Amstel
 * @date 2012 04 03
 *
 */
public class HelloWorldCassandra {
	
	public static void main (String [] args){
		System.out.println("Start HelloWorldCassandra");
		try {

			File yamlFile = new File("./src/main/resources/cassandra.yaml");
			
			// Read the configuration startup of cassandra
			EmbeddedTestServerHelper helper = new EmbeddedTestServerHelper(yamlFile );
			// Setup the embedded server
			helper.setup();
			// Server is started
			// See yaml file for port bindings and host names
		
			// Here you can do database actions.
			// Normally you will not write a program like this
			// Cassandra server is running elsewere as a server
			// and for test cases the setup of cassandra as a External Resource is a better approach.
			
			
			// Tear down the server
			EmbeddedTestServerHelper.teardown();
			
			// Clean up database files
			EmbeddedTestServerHelper.cleanup();
		} catch (Throwable t) {
			t.printStackTrace();
		} 
		
		System.out.println("End HelloWorldCassandra");
	}
	
	
	
	
}
