package queries;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import avro.shaded.com.google.common.collect.Iterables;
import scala.Tuple2;
import scala.Tuple3;
import utils.Query2Comparator;
import utils.HdfsUtility;
import utils.Query1Comparator;

public class Query2 {
	public static void run(SparkSession spark) {
        
        Dataset<Row> datasetVaccine = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-latest.parquet");
        
        
        Instant start = Instant.now();
        JavaRDD<Row> rawVaccine = datasetVaccine.toJavaRDD();
        
        //Eliminiamo i valori precedenti a Febbraio e successivi al 31 maggio
        JavaRDD<Row> selectRow = rawVaccine.filter(row ->{
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return !date.isBefore(QueryMain.FIRST_FEBRUARY) && date.isBefore(QueryMain.FIRST_JUNE);
        });
        
        
        /*[[Data, Area, Age], Vaccini]
         * Rappresenta il totale di vaccinazioni per una specifica Data, Regione e Fascia d'età, considerando tutte le aziende farmaceutiche di vaccini*/
        JavaPairRDD<Tuple3<LocalDate, String, String>, Long> sumOvervaccine = selectRow.mapToPair(row -> {
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return new Tuple2<>(new Tuple3<>(date, row.getString(row.length()-1), row.getString(3)), Long.valueOf(row.getInt(5)));
        }).reduceByKey((x, y)-> x+y);
        
        
        //Ordinamento in base alla Data di vaccinazione
        JavaPairRDD<LocalDate, Tuple3<String, String, Long>> sumOvervaccineSort = sumOvervaccine.mapToPair(row -> {
        	return new Tuple2<>(row._1._1(), new Tuple3<>(row._1._2(), row._1._3(), row._2));
        }).sortByKey();
        
        /*[[Area, Age],[Data, Vaccini]] 
         * Raggrumaneto di tutti i giorni di vaccinazione, per una data Regione e Fascia d'età*/
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<LocalDate, Long>>> regionAge = sumOvervaccineSort.mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._2._1(), row._2._2()), new Tuple2<>(row._1, row._2._3()));
        }).groupByKey();
        
        
        
        /*[Area, Age, Mese][[data, Vaccinazioni],...]
         * Raggrumaneto di tutti i giorni di vaccinazione, per una data Regione, Fascia d'età e Mese.
         * 
         * */
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Integer, Long>>> regionAgeMonth = regionAge.flatMapToPair((PairFlatMapFunction<Tuple2<Tuple2<String, String>, 
        		Iterable<Tuple2<LocalDate, Long>>>, Tuple3<String, String, String>,  Tuple2<Integer, Long>>) row -> {
        			
        	Iterable<Tuple2<LocalDate, Long>> dayVaccines = row._2;
        	ArrayList<Tuple2<Tuple3<String, String, String>, Tuple2<Integer, Long>>> list = new ArrayList<>();
        	ArrayList<Tuple2<Tuple3<String, String, String>, Tuple2<Integer, Long>>> listSupport = new ArrayList<>();
        	boolean firstLap = true;
        	int monthOld = 0;
        	
        	/*Rappresenta il numero di giorni di vaccinazioni utili, considerata una regione, fascia d'età e mese specifico*/
        	int n_vaccinationsDays = 0;
        	
        	/*Controlla che, per ogni regione, fascia d'età, mese, ci siano almeno due giorni 
        	 * in cui almeno una donna sia stata vaccinata*/
        	for(Tuple2<LocalDate, Long> vaccineDay : dayVaccines) {

        		listSupport.add(new Tuple2<>(new Tuple3<>((String)row._1._1, (String)row._1._2, vaccineDay._1().getMonth().name()), 
    					new Tuple2<>(Integer.valueOf(vaccineDay._1().getDayOfYear()), vaccineDay._2)));
        		
        		if (firstLap) {
            		monthOld = vaccineDay._1.getMonthValue();
        			if (vaccineDay._2 != 0) {
        				n_vaccinationsDays++;
        			}
        			firstLap = false;
        			continue;
        		}
        		
        		int month = vaccineDay._1.getMonthValue();
        		
        		if (month == monthOld) {
        			if (vaccineDay._2 != 0) {
            			n_vaccinationsDays++;
        			}

        		}else {
					if (n_vaccinationsDays >= 2) {
						list.addAll(listSupport);
					}else {
						if (QueryMain.DEBUG)
							QueryMain.log.info("Fascia d'età, Regione e Mese da scartare: "+row.toString());
					}
					listSupport.clear();
					n_vaccinationsDays = 0;
					
					/*Controlla che nel primo giorno diposnibile del primo mese sia stato effettuato almeno un vaccino*/
					if (vaccineDay._2 != 0) {
        				n_vaccinationsDays++;
        			}

				}
        		monthOld = month;
        	}
        	/*Stessi controlli anche per l'ultimo mese della lista*/
			if (n_vaccinationsDays>=2) {
				list.addAll(listSupport);
			}else {
				if (QueryMain.DEBUG)
					QueryMain.log.info("Fascia d'età e regione da scartare: "+row.toString());
			}
			listSupport.clear();		
        	return list.iterator();  	
        }).groupByKey();
        
        
        /* [Area, Age, Mese][[Data, Vaccinazioni], ..]
         * Scansione delle vaccinazioni, per una data Area, fascia d'età e regione, per aggiungere i giorni mancanti settando il numero di vaccini a 0*/
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Integer, Long>>> regionAgeMonthFill = regionAgeMonth.mapToPair(row -> {
        	List<Tuple2<Integer, Long>> list = new ArrayList<Tuple2<Integer, Long>>();
        	Iterable<Tuple2<Integer, Long>> vaccineDaysPerMonth = row._2;
        	
        	LocalDate day =  Year.now().atDay(vaccineDaysPerMonth.iterator().next()._1);
        	
        	int lenghtOfMonth = day.lengthOfMonth();
        	int month = day.getMonth().getValue();
        	
        	for (int i = lenghtOfMonth; i > 0; i--) {
        		
    			LocalDate dayOfMonth = LocalDate.of(Year.now().getValue(), month, i);
    			int dayOfYear = dayOfMonth.getDayOfYear();
        		boolean find = false;
        		
        		for (Tuple2<Integer, Long> line : vaccineDaysPerMonth) {
        			if (line._1 == dayOfYear) {
        				find = true;
						break;
					}
				}
        		if (!find) {
            		list.add(new Tuple2<Integer, Long>(dayOfYear, Long.valueOf(0)));
        		}
        		
			}
        	if (QueryMain.DEBUG) {
				QueryMain.log.info("Regione: "+row._1._1()+"; Fascia d'età: "+row._1._2()+"; Mese: "+row._1._3()+"; Lista giorni mancanti: "+list);

        	}
        	Iterables.addAll(list, vaccineDaysPerMonth);
        	return new Tuple2<>(new Tuple3<String, String, String>(row._1._1(), row._1._2(), row._1._3()), vaccineDaysPerMonth);
        	
        });
        
        
        
        /* [[Mese, Age, Area], Vaccinazioni]
         * Predizione, attraverso la regressione lineare, del numero di vaccinazioni al primo giorno del mese successivo*/
        JavaPairRDD<Tuple3<Month, String, String>, Integer> regionAgeMonthRegression = regionAgeMonthFill.mapToPair(row -> {
        	SimpleRegression regression = new SimpleRegression();
        	int last = 0;
        	for (Tuple2<Integer, Long> point: row._2) {
				regression.addData(point._1, point._2);
				last = point._1;
			}
        	LocalDate lastDayFor =  Year.now().atDay(last);
        	LocalDate ld = lastDayFor.withDayOfMonth(lastDayFor.lengthOfMonth());
        	Month month = ld.plusDays(1).getMonth();
        	double prediction = regression.predict(ld.plusDays(1).getDayOfYear());
        	if (prediction<0.0) {
        		prediction = 0.0;
        	}
        	return new Tuple2<>(new Tuple3<>(month, row._1._2(), row._1._1()), (int) prediction);
        	
        });
        
        
        /*[[Mese, Age, Regione], Vaccinazioni]
         * - Ordinamento rispetto all'Area e al numero di vaccinazioni, considerando una specifica Fascia d'età e Mese
         * - Ordinamento rispetto al Mese e Fascia d'età */
        JavaPairRDD<Tuple3<String, String, String>, Integer> result = regionAgeMonthRegression.mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._1._1(), row._1._2()), new Tuple2<>(row._1._3(), row._2));
        }).groupByKey().mapToPair(row -> {
        	
        	/*list: [[Area, Vaccinazioni], ...]*/
        	List<Tuple2<String, Integer>> list = new ArrayList<>();
        	row._2.forEach(list::add);
        	
        	list.sort(new Query2Comparator<String, Integer>(Comparator.reverseOrder()));
        	ArrayList<Tuple2<String, Integer>> listOrdered = new ArrayList<Tuple2<String, Integer>>(list.subList(0, 5));
        	Iterable<Tuple2<String, Integer>> i = listOrdered;
        	return new Tuple2<>(new Tuple2<>(row._1._1(), row._1._2()), i);
        	
        }).sortByKey(new Query1Comparator<Month, String>(Comparator.<Month>naturalOrder(), Comparator.<String>naturalOrder())).flatMapToPair(row ->{
        	ArrayList<Tuple2<Tuple3<String, String, String>, Integer>> list = new ArrayList<>();
        	for (Tuple2<String, Integer> regVac : row._2) {
        		String month = "1 "+ StringUtils.capitalize(row._1._1().getDisplayName(TextStyle.FULL,Locale.ITALIAN));
				list.add(new Tuple2<>(new Tuple3<>(month, row._1._2(), regVac._1), regVac._2));
			}
        	return list.iterator();
        });
        
        JavaRDD<Row> resultJavaRDD = result.map(row -> {
			return RowFactory.create(row._1()._1(), row._1()._2(), row._1()._3(), row._2);
        });       
        List<StructField> resultFields = new ArrayList<>();
        resultFields.add(DataTypes.createStructField("giorno", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("fascia_eta", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("predizione_vaccini", DataTypes.IntegerType, false));
        StructType resultStruct = DataTypes.createStructType(resultFields);
        
        Dataset<Row> dataset = spark.createDataFrame(resultJavaRDD, resultStruct);
        HdfsUtility.write(dataset, HdfsUtility.QUERY2_DIR, SaveMode.Overwrite, false, "query2_results.parquet");
        Instant end = Instant.now();
		QueryMain.log.info("Query 2 completed in " + Duration.between(start, end).toMillis() + "ms");
        if (QueryMain.DEBUG) {
            HdfsUtility.writeForTest(dataset, HdfsUtility.QUERY2_DIR, SaveMode.Overwrite, false, "query2_results.csv");
            List<Row> list =  resultJavaRDD.collect();
        	QueryMain.log.info("QUERY2 RESULTS:");
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
                .config("spark.cores.max", 6)
                .getOrCreate();
		Query2.run(spark);
		
	}

}
