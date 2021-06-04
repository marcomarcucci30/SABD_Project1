package queries;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import utils.HdfsUtility;
import utils.Query1Comparator;

public class Query1 {
	
		public static void run(SparkSession spark) {
			
		
			Dataset<Row> datasetSummary = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
	        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-summary-latest.parquet");
	        
	        Dataset<Row> datasetType = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
	        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/punti-somministrazione-tipologia.parquet");


	        Instant start = Instant.now();
	        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();
	        JavaRDD<Row> rawType = datasetType.toJavaRDD();
	        
	        /*[Area_completa, Vaccini_per_area]
	        Rappresenta il numero totale di centri vaccinazione, per regione*/
	        JavaPairRDD<String, Long> centriCount = rawType.mapToPair((row -> { 
	        	return new Tuple2<>( row.getString(row.length()-1), (long) 1);
	        })).reduceByKey((x, y) -> x+y);
	        
	        	        
	        /*ordinamento e filtraggio somministrazioni-vaccini-summary-latest*/
	        JavaPairRDD<LocalDate, Tuple2<String, Long>> parsedSummary = rawSummary.mapToPair((row -> {
	            LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	            return new Tuple2<>(date, new Tuple2<>(row.getString(row.length()-1), Long.valueOf(row.getInt(2))));
	        })).filter(row -> {
	        	return row._1.isAfter(QueryMain.LAST_DECEMBER) && row._1.isBefore(QueryMain.FIRST_JUNE);
	        }).sortByKey(true);
	        
	        
	        
	        /* [[Mese, Area], Vaccini]
	         * Rappresenta il numero totale di vaccinazioni effettuate, per uno specifico mese e regione*/
	        JavaPairRDD<Tuple2<Month, String>, Long> monthAreaTotal = parsedSummary.mapToPair((row -> {
	            Month month = row._1.getMonth();
	            return new Tuple2<>(new Tuple2<>(month, row._2._1), row._2._2);
	        })).reduceByKey((x, y) -> y+x);
	        
	        //Preprocessing per effettuare il join tra i due dataset, attraverso la regione
	        JavaPairRDD<String, Tuple2<Month, Long>> monthAreaTotalForJoin = monthAreaTotal.mapToPair(row -> {
	        	return new Tuple2<> (row._1._2, new Tuple2<>(row._1._1, row._2));
	        });
	        
	        //Join
	        JavaPairRDD<String, Tuple2<Tuple2<Month, Long>, Long>> monthAreaTotalJoin = monthAreaTotalForJoin.join(centriCount);

	       
	        /*[[Mese, Area], Vaccinazioni]
	         * Rappresenta il numero di vaccinazioni giornaliere per centro vaccinale, considerando uno specifico mese e regione*/
			JavaPairRDD<Tuple2<Month, String>, Long> monthAreaTotalPerDay = monthAreaTotalJoin.mapToPair((row -> {
	            Month month = row._2._1._1;
	            LocalDate date = LocalDate.of(Year.now().getValue(), month, 1);
	            int lenghtOfMonth = date.lengthOfMonth();
	            
	            return new Tuple2<>(new Tuple2<>(month, row._1), row._2._1._2/(lenghtOfMonth*row._2._2));
	        })).sortByKey(new Query1Comparator<Month, String>(Comparator.<Month>naturalOrder(), Comparator.<String>naturalOrder()));
	        
			//monthAreaTotalPerDay.collect();
	        
	        JavaRDD<Row> resultJavaRDD = monthAreaTotalPerDay.map((Function<Tuple2<Tuple2<Month, String>, Long>, Row>) row -> {
				return RowFactory.create(StringUtils.capitalize(row._1()._1().getDisplayName(TextStyle.FULL,Locale.ITALIAN)), row._1()._2(), row._2);
	        });
	        
	        List<StructField> resultFields = new ArrayList<>();
	        resultFields.add(DataTypes.createStructField("mese", DataTypes.StringType, false));
	        resultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
	        resultFields.add(DataTypes.createStructField("numero_medio_vaccini", DataTypes.LongType, false));
	        StructType resultStruct = DataTypes.createStructType(resultFields);
	        
	        Dataset<Row> dataset = spark.createDataFrame(resultJavaRDD, resultStruct);
	        
	        HdfsUtility.write(dataset, HdfsUtility.QUERY1_DIR, SaveMode.Overwrite, false, "query1_results.parquet");
	        Instant end = Instant.now();
			QueryMain.log.info("Query 1 completed in " + Duration.between(start, end).toMillis() + "ms");
	        
	        if (QueryMain.DEBUG) {
	        	HdfsUtility.writeForTest(dataset, HdfsUtility.QUERY1_DIR, SaveMode.Overwrite, false, "query1_results.csv");	 
	        	List<Row> list =  resultJavaRDD.collect();
	        	QueryMain.log.info("QUERY1 RESULTS:");
	            for (Row l: list) {
	            	QueryMain.log.info(l);
	            }
	        }
	        
		}
		public static void main(String[] args) {
			SparkSession spark = SparkSession
	                .builder()
	                .appName("Test")
	                .config("spark.master", "local")
	                .getOrCreate();
			Query1.run(spark);
		}

}

