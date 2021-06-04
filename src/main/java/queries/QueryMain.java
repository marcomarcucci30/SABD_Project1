package queries;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SparkSession;
import utils.HBaseQueries;

public class QueryMain {
	
	public static boolean DEBUG = true;
	
	public static Logger log = Logger.getLogger(QueryMain.class);
	public static LocalDate FIRST_JUNE = LocalDate.parse("2021-06-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	public static LocalDate FIRST_FEBRUARY = LocalDate.parse("2021-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	public static LocalDate LAST_DECEMBER = LocalDate.parse("2020-12-31", DateTimeFormatter.ofPattern("yyyy-MM-dd"));

	
	public static void main(String[] args) { 
		
		try {
	        InputStream input = QueryMain.class.getClassLoader().getResourceAsStream("log4j.properties");
	        Properties prop = new Properties();
	        prop.load(input);
	        PropertyConfigurator.configure(prop);
        } catch (IOException e) {
        	log.error(e);
        }
		
		SparkSession spark = SparkSession
                .builder()
                .appName("SABD_Project1")
                .getOrCreate(); 	
        
		DEBUG = Boolean.valueOf(args[0]);
		log.info("Starting Queries ...");
		
		Query1.run(spark);
		Query2.run(spark);
		Query3.run(spark);
		
		if (DEBUG) {
			spark.close();
			return;
		}
				
		log.info("Creating tables in HBase ...");
		HBaseQueries hbq = new HBaseQueries(spark);
		hbq.createTable(HBaseQueries.QUERY1_TABLE, HBaseQueries.COL_FAM_1);
		hbq.createTable(HBaseQueries.QUERY2_TABLE, HBaseQueries.COL_FAM_2);
		hbq.createTable(HBaseQueries.QUERY3_TABLE_RESULTS, HBaseQueries.COL_FAM_3_RESULTS);
		hbq.createTable(HBaseQueries.QUERY3_TABLE_PERFORMANCE, HBaseQueries.COL_FAM_3_PERFORMANCE);
		hbq.createTable(HBaseQueries.QUERY3_TABLE_CLUSTER, HBaseQueries.COL_FAM_3_CLUSTER);
		
		log.info("Exporting data to HBase ...");
		hbq.query1_hbase();
		hbq.query2_hbase();
		hbq.query3_hbase();
		
		log.info("Stopping ...");
        
        spark.close();

	}

}
