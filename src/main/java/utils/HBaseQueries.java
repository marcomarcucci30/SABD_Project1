package utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.protobuf.ServiceException;

import queries.QueryMain;

public class HBaseQueries {
	/**
	 * 
	 */
	public static final String SEP = "/";
	public static final String QUERY1_TABLE = "Query1_table";
	public static final String QUERY2_TABLE = "Query2_table";
	public static final String QUERY3_TABLE_RESULTS = "Query3_table_results";
	public static final String QUERY3_TABLE_PERFORMANCE = "Query3_table_performance";
	public static final String QUERY3_TABLE_CLUSTER = "Query3_table_cluster";
	
	public static final String COL_FAM_1 = "cf1";
	public static final String COL_FAM_2 = "cf2";
	public static final String COL_FAM_3_RESULTS = "cf3_results";
	public static final String COL_FAM_3_PERFORMANCE = "cf3_performance";
	public static final String COL_FAM_3_CLUSTER = "cf3_cluster";
	
	
	public static final String MEDIA_VACCINATI = "media_vaccinati";
	public static final String MESE = "mese";
	public static final String AREA = "regione";
	public static final String FASCIA_ETA = "fascia_eta";
	
	public static final String PREDIZIONE_VACCINATI = "predizione_vaccinati";
	
	public static final String PREDIZIONE_PERCENTUALE_VACCINATI_TOTALE = "predizione_vaccinati_totale";
	public static final String COST = "cost";
	public static final String PERFORMANCE = "performance_ms";
	public static final String CLUSTER = "cluster";
	public static final String MODEL = "modello";
	public static final String N_CLUSTER = "n_cluster";
	
	public HBaseQueries(SparkSession spark) {
		this.spark = spark;
	}
	
	HBaseClient hbc = new HBaseClient();
	SparkSession spark = null;
	
	
	public void createTable(String tableName, String... columnFamilies) {
		try {
			if (hbc.exists(tableName)) {
				TableName tn = TableName.valueOf(tableName);
				hbc.getConnection().getAdmin().disableTable(tn);
				hbc.getConnection().getAdmin().deleteTable(tn);
			}
			hbc.createTable(tableName, columnFamilies);
		} catch (IOException | ServiceException e) {
			QueryMain.log.error(e);
		}
	}
	
	
	public void query1_hbase() {
		Dataset<Row> dsResults = HdfsUtility.read(spark, "query1_results.parquet", HdfsUtility.OUTPUT_HDFS+HdfsUtility.QUERY1_DIR);
        JavaRDD<Row> rawResults = dsResults.toJavaRDD();
        
        List<Row> line =  rawResults.collect();
        for (Row row:line) {
        	hbc.put(QUERY1_TABLE, row.getString(0)+SEP+row.getString(1), COL_FAM_1, MEDIA_VACCINATI, String.valueOf(row.getLong(2)));
        	hbc.put(QUERY1_TABLE, row.getString(0)+SEP+row.getString(1), COL_FAM_1, AREA, String.valueOf(row.getString(1)));
        	hbc.put(QUERY1_TABLE, row.getString(0)+SEP+row.getString(1), COL_FAM_1, MESE, String.valueOf(row.getString(0)));
		}
        
        /*rawResults.map(row -> {
        	
        	return null;
        });*/
	}
	public void query2_hbase() {
		Dataset<Row> dsResults = HdfsUtility.read(spark, "query2_results.parquet", HdfsUtility.OUTPUT_HDFS+HdfsUtility.QUERY2_DIR);
        JavaRDD<Row> rawResults = dsResults.toJavaRDD();
        
        List<Row> line =  rawResults.collect();
        for (Row row:line) {
        	hbc.put(QUERY2_TABLE, row.getString(0)+SEP+row.getString(1)+SEP+row.getString(2), COL_FAM_2, PREDIZIONE_VACCINATI, String.valueOf(row.getInt(3)));
        	hbc.put(QUERY2_TABLE, row.getString(0)+SEP+row.getString(1)+SEP+row.getString(2), COL_FAM_2, MESE, String.valueOf(row.getString(0)));
        	hbc.put(QUERY2_TABLE, row.getString(0)+SEP+row.getString(1)+SEP+row.getString(2), COL_FAM_2, FASCIA_ETA, String.valueOf(row.getString(1)));
        	hbc.put(QUERY2_TABLE, row.getString(0)+SEP+row.getString(1)+SEP+row.getString(2), COL_FAM_2, AREA, String.valueOf(row.getString(2)));
        }
        
	}
	
	public void query3_hbase() {
		Dataset<Row> dsResults = HdfsUtility.read(spark, "query3_results.parquet", HdfsUtility.OUTPUT_HDFS+HdfsUtility.QUERY3_RESULTS_DIR);
        JavaRDD<Row> rawResults = dsResults.toJavaRDD();
        
        List<Row> line =  rawResults.collect();
        for (Row row:line) {
        	hbc.put(QUERY3_TABLE_RESULTS, row.getString(0)+SEP+row.getString(1), COL_FAM_3_RESULTS, 
        			PREDIZIONE_PERCENTUALE_VACCINATI_TOTALE, String.valueOf(row.getDouble(2)));
        	hbc.put(QUERY3_TABLE_RESULTS, row.getString(0)+SEP+row.getString(1), COL_FAM_3_RESULTS, 
        			AREA, String.valueOf(row.getString(0)));
        	hbc.put(QUERY3_TABLE_RESULTS, row.getString(0)+SEP+row.getString(1), COL_FAM_3_RESULTS, 
        			MESE, String.valueOf(row.getString(1)));
        }
        
        Dataset<Row> dsPerformance = HdfsUtility.read(spark, "query3_performance.parquet", HdfsUtility.OUTPUT_HDFS+HdfsUtility.QUERY3_PERFORMANCE_DIR);
        JavaRDD<Row> rawPerformance = dsPerformance.toJavaRDD();
        
        line =  rawPerformance.collect();
        for (Row row:line) {
        	hbc.put(QUERY3_TABLE_PERFORMANCE, row.getString(0)+SEP+String.valueOf(row.getInt(1)), COL_FAM_3_PERFORMANCE, 
        			PERFORMANCE, String.valueOf(row.getLong(2)));
        	hbc.put(QUERY3_TABLE_PERFORMANCE, row.getString(0)+SEP+String.valueOf(row.getInt(1)), COL_FAM_3_PERFORMANCE, 
        			COST, String.valueOf(row.getDouble(3)));
        	hbc.put(QUERY3_TABLE_PERFORMANCE, row.getString(0)+SEP+String.valueOf(row.getInt(1)), COL_FAM_3_PERFORMANCE, 
        			MODEL, String.valueOf(row.getString(0)));
        	hbc.put(QUERY3_TABLE_PERFORMANCE, row.getString(0)+SEP+String.valueOf(row.getInt(1)), COL_FAM_3_PERFORMANCE, 
        			N_CLUSTER, String.valueOf(row.getInt(1)));
        }
        
        
        
        
        Dataset<Row> dsCluster = HdfsUtility.read(spark, "query3_cluster.parquet", HdfsUtility.OUTPUT_HDFS+HdfsUtility.QUERY3_CLUSTER_DIR);
        JavaRDD<Row> rawCluster = dsCluster.toJavaRDD();
        
        line =  rawCluster.collect();
        for (Row row:line) {
        	hbc.put(QUERY3_TABLE_CLUSTER, row.getString(0)+SEP+String.valueOf(row.getInt(1))+SEP+row.getString(2), COL_FAM_3_CLUSTER, 
        			MODEL, String.valueOf(row.getString(0)));
        	hbc.put(QUERY3_TABLE_CLUSTER, row.getString(0)+SEP+String.valueOf(row.getInt(1))+SEP+row.getString(2), COL_FAM_3_CLUSTER, 
        			N_CLUSTER, String.valueOf(row.getInt(1)));
        	hbc.put(QUERY3_TABLE_CLUSTER, row.getString(0)+SEP+String.valueOf(row.getInt(1))+SEP+row.getString(2), COL_FAM_3_CLUSTER, 
        			AREA, String.valueOf(row.getString(2)));
        	hbc.put(QUERY3_TABLE_CLUSTER, row.getString(0)+SEP+String.valueOf(row.getInt(1))+SEP+row.getString(2), COL_FAM_3_CLUSTER, 
        			PREDIZIONE_PERCENTUALE_VACCINATI_TOTALE, String.valueOf(row.getDouble(3)));
        	hbc.put(QUERY3_TABLE_CLUSTER, row.getString(0)+SEP+String.valueOf(row.getInt(1))+SEP+row.getString(2), COL_FAM_3_CLUSTER, 
        			CLUSTER, String.valueOf(row.getInt(4)));
        }
        
        
        
	}

	public static void main(String[] args) {
		HBaseClient hbc = new HBaseClient();
		//
		try {
			if (hbc.exists("query1")) {
				String name = "query1";
				TableName tableName = TableName.valueOf(name);
				hbc.getConnection().getAdmin().disableTable(tableName);
				hbc.getConnection().getAdmin().deleteTable(tableName);
			}
			
			hbc.createTable("query1", "query1_family");
			hbc.put("query1", "row2", "query1_family", "mese", "gennaio", "query1_family", "area", "Basilicata", "query1_family", "num_vacc", "16");
			hbc.put("query1", "row1", "query1_family", "mese", "gennaio", "query1_family", "area", "Basilicata", "query1_family", "num_vacc", "15");
		} catch (IOException | ServiceException e) {
			QueryMain.log.error(e);
		}
		System.out.println(hbc.get("query1", "row2", "query1_family", "num_vacc"));
		System.out.println(hbc.describeTable("query1"));
		
		return;

	}

}
