package queries;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SparkSession;

public class QueryForTest {

	public static void main(String[] args) {
		try {
	        InputStream input = QueryMain.class.getClassLoader().getResourceAsStream("log4j.properties");
	        Properties prop = new Properties();
	        prop.load(input);
	        PropertyConfigurator.configure(prop);
        } catch (IOException e) {
        	QueryMain.log.error(e);
        }
		
		int n_test = 10;
		for (int i = 0; i < n_test; i++) {
			
			SparkSession spark = SparkSession
	                .builder()
	                .appName("SABD_Project1_test")
	                .config("spark.master", "local")
	                .getOrCreate(); 
			
			QueryMain.log.info("Starting Queries ...");
			
			Query1.run(spark);
			Query2.run(spark);
			Query3.run(spark);
			
			spark.close();
		}

	}

}
