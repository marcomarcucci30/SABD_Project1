 package queries;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
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

public class Query3 {
	
	public static void run(SparkSession spark) {
		
		Dataset<Row> datasetVaccine = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-summary-latest.parquet");
		
		Dataset<Row> datasetPopulation = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/totale-popolazione.parquet");
		
        Instant start = Instant.now();
        JavaRDD<Row> rawVaccine = datasetVaccine.toJavaRDD().filter(row ->{
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return date.isBefore(QueryMain.FIRST_JUNE);
        }).cache();
        JavaRDD<Row> rawPopulation = datasetPopulation.toJavaRDD();
        
        /*[Area, [Data, Vaccinazioni], ...]
         * Raggruppamento per Area di tutti i giorni di vaccinazione*/
        JavaPairRDD<String, Iterable<Tuple2<LocalDate, Long>>> groupByAreaSorted = rawVaccine.mapToPair(row -> {
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return new Tuple2<>(row.getString(row.length()-1), new Tuple2<>(date, Long.valueOf(row.getInt(2))));
        }).groupByKey();
        
        /*[Area, Vaccinazioni]
         * Regressione e predizione al 1 giugno, considerando una specifica Area*/
        JavaPairRDD<String, Integer> areaRegression = groupByAreaSorted.mapToPair(row -> {
        	SimpleRegression regression = new SimpleRegression();
        	for (Tuple2<LocalDate, Long> point: row._2) {
        		int dateInt = point._1().getDayOfYear();
				regression.addData(dateInt, point._2());
			}
        	
        	double prediction = regression.predict(QueryMain.FIRST_JUNE.getDayOfYear());
        	if (prediction<0.0) {
        		prediction = 0.0;
        	}
        	return new Tuple2<>(row._1(), (int) prediction);
        	
        });
        
        /*Preprocessing Dataset totale-popolazione per via di un errore nel nome della Valle D'Aosta */
        JavaPairRDD<String, Long> population = rawPopulation.mapToPair(row -> {
        	String area = row.getString(0);
        	if (area.contains("Valle")) {
				area = "Valle d'Aosta / Vallée d'Aoste";
			}
        	return new Tuple2<>(area, Long.valueOf(row.getInt(1)));
        });

        /*[[Area, Mese], Percentuale_Vaccinazioni]
         * 1. Somma delle vaccinazioni effettuate fino al 31 maggio, considerando una specifica Regione
         * 2. Join con l'RDD areaRegression
         * 3. Somma delle previsioni effettuate in areaRegression con il totale delle vaccinazioni calcolate al punto 1, considerando una specifica Regione
         * 4. Join con l'RDD population
         * 5. Divisione tra il risultato al punto 3, con il totale della popolazione, considerando una specifica Regione*/
        JavaPairRDD<Tuple2<String, String>, Double> predictionPercentage = rawVaccine.mapToPair(row -> {
        	return new Tuple2<>(row.getString(row.length()-1), Long.valueOf(row.getInt(2)));
        }).reduceByKey((x, y) -> (x+y)).mapToPair(row -> {
        	return new Tuple2<>(row._1(), row._2());
        }).join(areaRegression).mapToPair(row -> {
        	return new Tuple2<>(row._1(), row._2()._1() + row._2()._2());
        }).join(population).sortByKey().mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._1(), "1 " + StringUtils.capitalize(QueryMain.FIRST_JUNE.getMonth().getDisplayName(TextStyle.FULL,Locale.ITALIAN))), Double.valueOf(row._2()._1()) / Double.valueOf(row._2()._2()));
        });
        
        
        //Clustering

        JavaRDD<Vector> training = predictionPercentage.map(row -> {
        	return Vectors.dense(row._2());
        });
        
        ArrayList<Row> listPerformance = new ArrayList<>();
        ArrayList<JavaRDD<Row>> listJavaRDD = new ArrayList<>();
        
        for (int k = 2; k <= 5; k++) {
        	//K-Means
            Instant startKMeans = Instant.now();
        	KMeansModel clusterKmeans = KMeans.train(training.rdd(), k, 100);
        	Instant endKMeans = Instant.now();
            Long KMeansTrainPerformance = Duration.between(startKMeans, endKMeans).toMillis();
            Double KMeansTrainCost = clusterKmeans.computeCost(training.rdd());
            
            listPerformance.add(RowFactory.create("K-MEANS", k, KMeansTrainPerformance, KMeansTrainCost));
            
            //K-Means Bisecting
            Instant startKMeansBisectiong = Instant.now();
        	KMeansModel clusterBisectiong = KMeans.train(training.rdd(), k, 100);
        	Instant endKMeansBisectiong = Instant.now();
            Long KMeansTrainPerformanceBisectiong = Duration.between(startKMeansBisectiong, endKMeansBisectiong).toMillis();
            Double KMeansTrainCostBisectiong = clusterBisectiong.computeCost(training.rdd());
            
            listPerformance.add(RowFactory.create("K-MEANS-BISECTING", k, KMeansTrainPerformanceBisectiong, KMeansTrainCostBisectiong));
            
            
            int k_support= k;
            JavaRDD<Row> areaBelongToKmeans = predictionPercentage.map(row -> {
            	return RowFactory.create("K-MEANS", k_support, row._1()._1(), row._2, clusterKmeans.predict(Vectors.dense(row._2())));
            });
            JavaRDD<Row> areaBelongToBisecting = predictionPercentage.map(row -> {
            	return RowFactory.create("K-MEANS-BISECTING", k_support, row._1()._1(), row._2, clusterBisectiong.predict(Vectors.dense(row._2())));
            });
            
            listJavaRDD.add(areaBelongToBisecting);
            listJavaRDD.add(areaBelongToKmeans);
            
		}
        
        List<StructField> resultFields = new ArrayList<>();
        resultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("giorno", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("predizione_percentuale_vaccini", DataTypes.DoubleType, false));
        StructType resultStruct = DataTypes.createStructType(resultFields);
        
        // Saving results
        JavaRDD<Row> resultJavaRDD = predictionPercentage.map(row -> {
			return RowFactory.create(row._1()._1(), row._1()._2(), row._2);
        });
        Dataset<Row> dataset_results = spark.createDataFrame(resultJavaRDD, resultStruct);
        HdfsUtility.write(dataset_results, HdfsUtility.QUERY3_RESULTS_DIR, SaveMode.Overwrite, false, "query3_results.parquet");

        
        // Saving performance 
        List<StructField> performanceFields = new ArrayList<>();
        performanceFields.add(DataTypes.createStructField("modello", DataTypes.StringType, false));
        performanceFields.add(DataTypes.createStructField("k", DataTypes.IntegerType, false));
        performanceFields.add(DataTypes.createStructField("performance_ms", DataTypes.LongType, false));
        performanceFields.add(DataTypes.createStructField("cost_WSSSE", DataTypes.DoubleType, false));
        StructType performanceStruct = DataTypes.createStructType(performanceFields);
        
        
        Dataset<Row> dataset_performance = spark.createDataFrame(listPerformance, performanceStruct);
        HdfsUtility.write(dataset_performance, HdfsUtility.QUERY3_PERFORMANCE_DIR, SaveMode.Overwrite, false, "query3_performance.parquet");
        
        // Saving cluster
        List<StructField> clusterResultFields = new ArrayList<>();
        clusterResultFields.add(DataTypes.createStructField("modello", DataTypes.StringType, false));
        clusterResultFields.add(DataTypes.createStructField("k", DataTypes.IntegerType, false));
        clusterResultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        clusterResultFields.add(DataTypes.createStructField("percentuale_vaccinati", DataTypes.DoubleType, false));
        clusterResultFields.add(DataTypes.createStructField("cluster", DataTypes.IntegerType, false));
        StructType clusterResultStruct = DataTypes.createStructType(clusterResultFields);
        
        Dataset<Row> dataset_cluster = null;
        for (JavaRDD<Row> rdd : listJavaRDD) {
        	 dataset_cluster = spark.createDataFrame(rdd, clusterResultStruct);
        	HdfsUtility.write(dataset_cluster, HdfsUtility.QUERY3_CLUSTER_DIR+"_Support", SaveMode.Append, false, "_Support");
        	if (QueryMain.DEBUG) {
            	HdfsUtility.writeForTest(dataset_cluster, HdfsUtility.QUERY3_CLUSTER_DIR, SaveMode.Append, false, "query3_cluster.csv");
        	}
		}
        Dataset<Row>df = HdfsUtility.read(spark, HdfsUtility.QUERY3_CLUSTER_DIR+"_Support", HdfsUtility.OUTPUT_HDFS);
        Dataset<Row> df_output=df.coalesce(1);
        HdfsUtility.write(df_output, HdfsUtility.QUERY3_CLUSTER_DIR, SaveMode.Overwrite, true, "query3_cluster.parquet");
        
        Instant end = Instant.now();
		QueryMain.log.info("Query 3 completed in " +Duration.between(start, end).toMillis()+ "ms");
        
        
        if (QueryMain.DEBUG) {
            HdfsUtility.writeForTest(dataset_performance, HdfsUtility.QUERY3_PERFORMANCE_DIR, SaveMode.Overwrite, false, "query3_performance.csv");
            HdfsUtility.writeForTest(dataset_results, HdfsUtility.QUERY3_RESULTS_DIR, SaveMode.Overwrite, false, "query3_results.csv");
        }
        
        if (QueryMain.DEBUG) {
        	List<Row> list =  resultJavaRDD.collect();
        	QueryMain.log.info("QUERY3 RESULTS:");
            for (Row l: list) {
            	QueryMain.log.info(l);
            }
            
        	QueryMain.log.info("QUERY3 PERFORMANCE:");
            for (Row l: listPerformance) {
            	QueryMain.log.info(l);
            }            
            
            QueryMain.log.info("QUERY3 CLUSTER:");
            for (JavaRDD<Row> rdd : listJavaRDD) {
            	List<Row> list2 =  rdd.collect();
                for (Row l: list2) {
                	QueryMain.log.info(l);
                }
			}
            
        }
	}
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
		Query3.run(spark);;
	}

}
