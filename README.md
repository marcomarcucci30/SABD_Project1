# SABD 2020/2021 Progetto 1
Marco Marcucci, Giuseppe Lasco

## Prerequisiti
* Installare Apache Spark
* Eseguire `export SPARK_HOME=<spark-path> `
* Installare Docker
* Inserire in */etc/hosts* `127.0.0.1	hbase`
## Ambiente Docker
Nella directory `/Docker` sono presenti gli script che permettono il setup automatico dei framework. Lo script `start-dockers.sh` permette di avviare i servizi (NiFi, HDFS, HBASE, Docker network) e attendere che il framework di data ingestion si avvii e carichi tutti i dati necessari su HDFS. Lo script `start-project.sh` esegue le medesime operazioni del precedente ed inoltre sottomette l'applicazione a Spark appena l'ambiente è pronto.
Lo script `stop-dockers.sh` permette, infine, di arrestare tutti i servizi precedentemente avviati.

Al primo avvio del progetto è necessario configurare NiFi ed eseguire il build del progetto, perciò è utile seguire i seguenti passi:

* Avviare lo script `maven-package.sh` nella directory `/spark_scripts`
* Avviare lo script `start-dockers.sh` o `start-project.sh`
* Collegarsi all'indirizzo <http://localhost:9880>
* Caricare il template `nifi_template.xml` contenuto nella directory `/Docker/nifi_template`
* Abilitare i controller services *CSVReader* e *CSVRecordSetWriter*, accedendo alle impostazioni del *process group*
* Avviare il *process group*

Nel momento in cui NiFi avrà importato tutti i file correttamente, lo script darà conferma della corretta configurazione dell'ambiente.
È possibile controllare lo stato dell' HDFS collegandosi al seguente indirizzo <http://localhost:9870/explorer.html#/>
## Spark script
Nella directory `/spark_scripts` sono presenti diversi script che permettono l'avvio dell'applicazione in varie modalità. Gli script devono essere avviati quando l'ambiente docker è pronto. Inoltre vi è lo script `maven-package.sh`, il quale permette il build e il package dell'applicazione in formato `jar`. In modalità `debug` l'applicazione genera una serie di stampe sul terminale sfruttando il log `log4j` di Apache e la directory `/debug_results` contenente i risultati delle query, utili all'attività di debug.

## Struttura dell'applicazione
L'applicazione si compone di due package `queries` e `utils`. Nel primo sono presenti le classi che implementano le query e i main di avvio dell'applicazione nelle varie modalità:

- `Query1.java`
- `Query2.java`
- `Query3.java`
- `QueryMain.java`
- `QueryForTest.java`

Il secondo package contiene le classi di utilità, le quali permettono la comunicazione con HDFS, con HBase e implementano i comparator utili alle attività di sorting:

- `HdfsUtility.java`
- `HBaseQueries.java`
- `HBaseClient.java`
- `Query1Comparator.java`
- `Query2Comparator.java`

## Directory
La cartella `Report` contiene la relazione relativa al progetto. Nella cartella `Results` si trovano i risultati delle query, ottenute dai dati aggiornati al 04/06/2021 11:00 AM, in formato `csv` come richiesto dalla specifica .


