package nl.kaninefaten.cassandra.tutorial.t1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import me.prettyprint.hector.testutils.EmbeddedSchemaLoader;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merely a copy of.
 * https://github.com/rantav/hector/blob/master/test/src/main/java/me/prettyprint/hector/testutils/EmbeddedServerHelper.java
 * 
 * P.S.
 * With some minor adjustments to work in the HelloWorld Cassandra class
 * 
 * @author Ran Tavory (rantav@gmail.com)
 */
public class EmbeddedTestServerHelper 
{
	private static Logger log = LoggerFactory.getLogger(EmbeddedTestServerHelper.class);


	private static final Logger LOG = LoggerFactory.getLogger( EmbeddedTestServerHelper.class );

	private File _cassandraTmpDatabaseFolder = null;
	private File _yamlFile = null;

	private static CassandraDaemon cassandraDaemon;

	
	private static File createTempDirectory()
		    throws IOException
		{
		    final File temp;
		    temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

		    if(!(temp.delete()))
		    {
		        throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
		    }

		    if(!(temp.mkdir()))
		    {
		        throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
		    }

		    return (temp);
		}
	
	public EmbeddedTestServerHelper(File yamlFile) throws IOException{
		this._yamlFile = yamlFile;
		this._cassandraTmpDatabaseFolder = createTempDirectory();
		
	}
	
	public EmbeddedTestServerHelper(File yamlFile , File tmpDbFolder)
	{
		this._yamlFile = yamlFile;
		this._cassandraTmpDatabaseFolder = tmpDbFolder;
	}

	private static ExecutorService executor = Executors.newSingleThreadExecutor();

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void setup()
		throws TTransportException, IOException, InterruptedException, ConfigurationException
	{
		// delete tmp dir first
		rmdir(_cassandraTmpDatabaseFolder);
		// make a tmp dir and copy cassandra.yaml and log4j.properties to it
		//copy("/log4j.properties", TMP);
		copy(_yamlFile, _cassandraTmpDatabaseFolder);
		System.setProperty("cassandra.config", "file:" + _cassandraTmpDatabaseFolder.getAbsolutePath() + "/" + _yamlFile.getName());
		System.setProperty("log4j.configuration", "file:" + _cassandraTmpDatabaseFolder.getAbsolutePath() + "/log4j.properties");
		System.setProperty("cassandra-foreground", "true");

		cleanupAndLeaveDirs();
		//loadSchemaFromYaml();
		// loadYamlTables();
		log.info("Starting executor");

		executor.execute(new CassandraRunner());
		log.info("Started executor");
		try {
			TimeUnit.SECONDS.sleep(3);
			log.info("Done sleeping");
		}
		catch (InterruptedException e) {
			throw new AssertionError(e);
		}
	}

	public static void teardown()
	{
		executor.shutdown();
		executor.shutdownNow();
	}

	private static void rmdir(File dir)
		throws IOException
	{
		if (dir.exists()) {
			FileUtils.deleteRecursive(dir);
		}
	}

	/**
	 * Copies a resource from within the jar to a directory.
	 * 
	 * @param resource
	 * @param directory
	 * @throws IOException
	 */
	private static void copy(File yamlFile, File runFolder)
		throws IOException
	{
		runFolder.mkdirs();
		String fileName = yamlFile.getName();
		File file = new File(runFolder , fileName);

		FileInputStream is = new FileInputStream(yamlFile);
		OutputStream out = new FileOutputStream(file);
		byte buf[] = new byte[1024];
		int len;
		while ((len = is.read(buf)) > 0) {
			out.write(buf, 0, len);
		}
		out.close();
		is.close();
	}


	public static void cleanupAndLeaveDirs()
	{
		mkdirs();
		cleanup();
		mkdirs();
		CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this brings it back to safe state
	}

	public static void cleanup()
	{
		
		
		
		File cachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());
		if (!cachesDir.exists())
			throw new RuntimeException("No such directory: " + cachesDir.getAbsolutePath());
		try {
			FileUtils.deleteRecursive(cachesDir);
		}catch(IOException e){
			LOG.error( "Could not cleanup."+ DatabaseDescriptor.getSavedCachesLocation() );
		}
		
		// clean up commitlog
		String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
		for (String dirName : directoryNames) {
			File dir = new File(dirName);
			if (!dir.exists())
				throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
			try {
				FileUtils.deleteRecursive(dir);
			}
			catch (IOException e) {
				LOG.error( "Could not cleanup.", e );
			}
		}

		// clean up data directory which are stored as data directory/table/data files
		for (String dirName : DatabaseDescriptor.getAllDataFileLocations()) {
			File dir = new File(dirName);
			if (!dir.exists())
				throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
			try {
				FileUtils.deleteRecursive(dir);
			}
			catch (IOException e ){
				LOG.error( "Could not cleanup.", e );
			}
		}
	}

	public static void mkdirs()
	{
		try {
			DatabaseDescriptor.createAllDirectories();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void loadSchemaFromYaml()
	{
		EmbeddedSchemaLoader.loadSchema();
	}

	class CassandraRunner
		implements Runnable
	{
		@Override
		public void run()
		{
			cassandraDaemon = new CassandraDaemon();
			cassandraDaemon.activate();
			
		}

	}
}