package utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import queries.QueryMain;

public class HdfsUtility {
	
	public final static String URL_HDFS = "//localhost";
	public final static String PORT_HDFS = "9871";
	public final static String INPUT_HDFS = "/data";
	public final static String OUTPUT_HDFS = "/output";
	
	public final static String QUERY1_DIR = "/Query1_results";
	public final static String QUERY2_DIR = "/Query2_results";
	public final static String QUERY3_RESULTS_DIR = "/Query3_results";
	public final static String QUERY3_PERFORMANCE_DIR = "/Query3_performance";
	public final static String QUERY3_CLUSTER_DIR = "/Query3_cluster";

	public static void write(Dataset<Row> dataset, String dir, SaveMode mode, boolean multipart, String newName) {
		dataset.write()
	        .format("parquet")
	        .option("header", true)
	        .mode(mode)
	        .save("hdfs:"+URL_HDFS+":"+PORT_HDFS+OUTPUT_HDFS+dir);
		FileSystem fs;
		if (mode == SaveMode.Append) {
			return;
		}
		if (multipart) {// eliminazione della cartella di supporto contenente i singoli file  
	        try {
				fs = FileSystem.get(new URI("hdfs:"+HdfsUtility.URL_HDFS+":" +HdfsUtility.PORT_HDFS), new Configuration());
				fs.delete(new Path(HdfsUtility.OUTPUT_HDFS+HdfsUtility.QUERY3_CLUSTER_DIR+"_Support"), true);
			} catch (IOException e) {
				QueryMain.log.error(e);
			} catch (URISyntaxException e) {
				QueryMain.log.error(e);
			}
		}
		
		try {// modifica del nome del file
			fs = FileSystem.get(new URI("hdfs:"+HdfsUtility.URL_HDFS+":" +HdfsUtility.PORT_HDFS), new Configuration());			
			String old = fs.globStatus(new Path(OUTPUT_HDFS+dir+"/part*.parquet"))[0].getPath().getName();
			fs.rename(new Path(OUTPUT_HDFS+dir+"/"+old), new Path(OUTPUT_HDFS+dir+"/"+newName));
		} catch (IOException e) {
			QueryMain.log.error(e);
		} catch (URISyntaxException e) {
			QueryMain.log.error(e);
		}
	}
	
	public static void writeForTest(Dataset<Row> dataset, String dir, SaveMode mode, boolean multipart, String newName) {
		dataset.write()
	        .format("csv")
	        .option("header", true)
	        .mode(mode)
	        .save("../debug_results"+dir);
	}
	
	public static Dataset<Row> read(SparkSession spark, String fileName, String dir) {

		Dataset<Row> dataset = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" +HdfsUtility.PORT_HDFS+
        		dir+"/"+fileName);
		return dataset;
	}

}
