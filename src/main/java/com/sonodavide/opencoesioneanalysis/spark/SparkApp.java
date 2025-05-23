package com.sonodavide.opencoesioneanalysis.spark;
import com.sonodavide.opencoesioneanalysis.utils.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SparkApp {
    private SparkSession spark;

    public SparkApp() {
        spark = SparkSession.builder()
                .appName("SparkApp")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
    }

    public Dataset<Row> loadCsv(String path){
         return spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(path);
    }

    /*
        query groupy by di prova, sia su postgres che su spark
     */
    public void benchmarkGroupByPostgres(SparkApp sparkApp, Dataset<Row> df) {

        long startTime = System.currentTimeMillis();
        String groupColumn = "DESCR_PRIORITA_INVEST"; // Colonna su cui fare il GROUP BY
        Dataset<Row> groupedDF = df.groupBy(groupColumn)
                .count()
                .withColumnRenamed("count", "totale")
                .repartition(2);
        long sparkTime = System.currentTimeMillis() - startTime;

        long startTime2 = System.currentTimeMillis();

        Dataset<Row> groupedDF2 = df.groupBy(groupColumn)
                .count()
                .withColumnRenamed("count", "totale")
                .repartition(1);
        long sparkTime2 = System.currentTimeMillis() - startTime;

        Dataset<Row> groupedDF3 = df.groupBy(groupColumn)
                .count()
                .withColumnRenamed("count", "totale")
                .repartition(3);
        long sparkTime3 = System.currentTimeMillis() - startTime;

        String jdbcUrl = "jdbc:postgresql://localhost:5432/prova";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "admin");
        connectionProperties.put("driver", "org.postgresql.Driver");




        // query postgresql
        startTime = System.currentTimeMillis();
        Dataset<Row> dbGroupedDF = spark.read()
                .jdbc(jdbcUrl,
                        "(SELECT \"" + groupColumn + "\", COUNT(*) AS totale_db " +
                                "FROM progetti_esteso GROUP BY \"" + groupColumn + "\") AS subquery",
                        connectionProperties);
        long dbTime = System.currentTimeMillis() - startTime;


        spark.stop();
        System.out.println("Tempo Spark: " + sparkTime + "ms");
        System.out.println("Tempo PostgreSQL: " + dbTime + "ms");
        System.out.println("Tempo Spark2: " + sparkTime2 + "ms");
        System.out.println("Tempo Spark3: " + sparkTime3 + "ms");

    }


     /*
        metodo per sapere quanti null ci sono per ogni colonna
     */
    public Dataset<Row> generateColumnsStats(List<String> columns, Dataset<Row> df) {
        List<Row> results = new ArrayList<>();
        List<String> problematicColumns = new ArrayList<>();

        for (String column : columns) {
            System.out.println("\n\n\n\nCOLONNA " + column + "\n\n\n\n");

            try {
                // Usa trim() e length() per evitare problemi di cast
                // Questo approccio è più robusto per tutti i tipi di dati
                Dataset<Row> counts = df.agg(
                        sum(when(col(column).isNotNull()
                                .and(length(trim(col(column))).gt(0)), 1)
                                .otherwise(0)).alias("populated"),
                        sum(when(col(column).isNull()
                                .or(length(trim(col(column))).equalTo(0)), 1)
                                .otherwise(0)).alias("empty")
                );

                Row countRow = counts.first();
                long populatedCount = countRow.getLong(0);
                long emptyCount = countRow.getLong(1);

                results.add(RowFactory.create(column, populatedCount, emptyCount, "SUCCESS"));

            } catch (Exception e) {
                System.err.println("ERRORE nella colonna " + column + ": " + e.getMessage());
                problematicColumns.add(column);

                // Prova un approccio alternativo più semplice
                try {
                    long totalCount = df.count();
                    long notNullCount = df.filter(col(column).isNotNull()).count();
                    long nullCount = totalCount - notNullCount;

                    // Per le colonne problematiche, contiamo solo NULL vs NOT NULL
                    results.add(RowFactory.create(column, notNullCount, nullCount, "PARTIAL_SUCCESS"));
                    System.out.println("Usato approccio alternativo per la colonna " + column);

                } catch (Exception fallbackException) {
                    System.err.println("ERRORE CRITICO nella colonna " + column + ": " + fallbackException.getMessage());
                    results.add(RowFactory.create(column, -1L, -1L, "ERROR"));
                }
            }
        }

        // Stampa le colonne problematiche
        if (!problematicColumns.isEmpty()) {
            System.out.println("\n\n=== COLONNE PROBLEMATICHE ===");
            for (String col : problematicColumns) {
                System.out.println("- " + col);
            }
            System.out.println("Totale colonne problematiche: " + problematicColumns.size());
        }

        // Schema aggiornato con campo status
        StructType schema = new StructType(new StructField[]{
                new StructField("colonna", DataTypes.StringType, false, Metadata.empty()),
                new StructField("righe_popolate", DataTypes.LongType, false, Metadata.empty()),
                new StructField("righe_non_popolate", DataTypes.LongType, false, Metadata.empty()),
                new StructField("status", DataTypes.StringType, false, Metadata.empty())
        });

        return spark.createDataFrame(results, schema);
    }

    public void contaProgettiInRitardoPerFase(Dataset<Row> df){
        List<String> temporalColumns = Arrays.asList(

                "DATA_INIZIO_PREV_STUDIO_FATT",
                "DATA_FINE_PREV_STUDIO_FATT",
                "DATA_INIZIO_EFF_STUDIO_FATT",
                "DATA_FINE_EFF_STUDIO_FATT",

                "DATA_INIZIO_PREV_PROG_PREL",
                "DATA_FINE_PREV_PROG_PREL",
                "DATA_INIZIO_EFF_PROG_PREL",
                "DATA_FINE_EFF_PROG_PREL",

                "DATA_INIZIO_PREV_PROG_DEF",
                "DATA_FINE_PREV_PROG_DEF",
                "DATA_INIZIO_EFF_PROG_DEF",
                "DATA_FINE_EFF_PROG_DEF",

                "DATA_INIZIO_PREV_PROG_ESEC",
                "DATA_FINE_PREV_PROG_ESEC",
                "DATA_INIZIO_EFF_PROG_ESEC",
                "DATA_FINE_EFF_PROG_ESEC",

                "DATA_INIZIO_PREV_AGG_BANDO",
                "DATA_FINE_PREV_AGG_BANDO",
                "DATA_INIZIO_EFF_AGG_BANDO",
                "DATA_FINE_EFF_AGG_BANDO",

                "DATA_INIZIO_PREV_STIP_ATTRIB",
                "DATA_FINE_PREV_STIP_ATTRIB",
                "DATA_INIZIO_EFF_STIP_ATTRIB",
                "DATA_FINE_EFF_STIP_ATTRIB",

                "DATA_INIZIO_PREV_ESECUZIONE",
                "DATA_FINE_PREV_ESECUZIONE",
                "DATA_INIZIO_EFF_ESECUZIONE",
                "DATA_FINE_EFF_ESECUZIONE",

                "DATA_INIZIO_PREV_COLLAUDO",
                "DATA_FINE_PREV_COLLAUDO",
                "DATA_INIZIO_EFF_COLLAUDO",
                "DATA_FINE_EFF_COLLAUDO")
                ;
        int len = temporalColumns.size();
        if(len%4!=0) throw new IllegalStateException();
        Dataset<Row> dfConverted = convertDates(df, temporalColumns);
        HashMap<String, Pair<Long>> map = new HashMap<>();
        for(int i=0;i<len;i+=4){
            String inizio_prev = temporalColumns.get(i);
            String fine_prev = temporalColumns.get(i+1);
            String inizio_eff = temporalColumns.get(i+2);
            String fine_eff = temporalColumns.get(i+3);
            String nomeColonnaPulito = inizio_prev.replace("_INIZIO_PREV_", "");
            Dataset<Row> dfRitardabile = calcolaRitardabili(dfConverted, fine_prev, inizio_eff, fine_eff);
            long calcolabili = dfRitardabile.count();
            Dataset<Row> dfRitardi = calcolaRitardi(dfRitardabile, fine_prev, inizio_eff, fine_eff);
            long ritardo = dfRitardi.count();
            map.put(nomeColonnaPulito, new Pair(calcolabili, ritardo));
        }
        StructType structType = new StructType()
                .add("fase", DataTypes.StringType)
                .add("contabili", DataTypes.LongType)
                .add("ritardi", DataTypes.LongType);

        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, Pair<Long>> entry : map.entrySet()) {
            String fase = entry.getKey();
            Long contabili = entry.getValue().x;
            Long ritardi = entry.getValue().y;
            rows.add(RowFactory.create(fase, contabili, ritardi));
        }
        Dataset<Row> resultDF = spark.createDataFrame(rows, structType);

        saveDatasetJson(resultDF, "out", "ritardi_per_fase.json");
    }

    /*
        Vecchio metodo di prova
     */
    public void contaProgettiInRitardoPerFaseTestSingolo(Dataset<Row> df) {
        List<String> temporalColumns = Arrays.asList(

                "DATA_INIZIO_PREV_STUDIO_FATT",
                "DATA_FINE_PREV_STUDIO_FATT",
                "DATA_INIZIO_EFF_STUDIO_FATT",
                "DATA_FINE_EFF_STUDIO_FATT");




        //df.groupBy(col("CUP_COD_NATURA"), col("CUP_DESCR_NATURA")).count().show();
        //df.groupBy(col("CUP_COD_TIPOLOGIA"), col("CUP_DESCR_TIPOLOGIA")).count().show();
        //df.select(col("CUP_DESCR_TIPOLOGIA")).distinct().show(500);
        //List<String>temporalColumsExtra = Arrays.asList("OC_DATA_INIZIO_PROGETTO", "OC_DATA_FINE_PROGETTO_PREVISTA", "OC_DATA_FINE_PROGETTO_EFFETTIVA");
        // Converti in array di Column
        //Column[] selectedCols = temporalColumns.stream()
        //        .map(functions::col)
        //        .toArray(Column[]::new);
        //df.select(selectedCols).show(200);
        int len = temporalColumns.size();

        String inizio_prev = temporalColumns.get(0);
        String fine_prev = temporalColumns.get(1);
        String inizio_eff = temporalColumns.get(2);
        String fine_eff = temporalColumns.get(3);
        System.out.println(df.columns());


        if(len%4!=0) throw new IllegalStateException("il numero è strano");

        Dataset<Row> dfConverted = convertDates(df, temporalColumns);

        // trova righe su cui si può calcolare il ritardo
        Dataset<Row> dfRitardabile = calcolaRitardabili(dfConverted, fine_prev, inizio_eff, fine_eff);

        System.out.println("ritardabile: " + dfRitardabile.count());


        // calcola ritardo
        long ritardo = dfRitardabile
                .filter(
                        (
                                col(fine_prev).isNotNull()
                                        .and(col(fine_eff).isNotNull())
                                        .and(
                                                col(fine_prev).lt(col(fine_eff))
                                                        .or(col(fine_prev).lt(col(inizio_eff)))
                                        )
                        )
                                .or(
                                        col(fine_eff).isNull()
                                                .and(col(inizio_eff).isNotNull())
                                                .and(col(fine_prev).lt(current_date()))
                                )
                )
                .count();
        System.out.println("Ritardo: " + ritardo);




    }

    /*
        Metodo per ottenere la colonna con il testo. Utile per le colonne coppie descrizione-codice
        malformattate in cui compaiono numeri da entrambe le parti.
     */
    public Dataset<Row> selectNonNumericColumnValue(Dataset<Row> df, String colA, String colB, String outputCol) {
        // Regex per controllare se c'è almeno un numero
        String containsDigitRegex = ".*\\d.*";

        Column colAHasDigit = col(colA).rlike(containsDigitRegex);
        Column colBHasDigit = col(colB).rlike(containsDigitRegex);

        // Se colA NON ha numeri, prendiamo colA
        // altrimenti, se colB NON ha numeri, prendiamo colB
        // altrimenti null
        Column selected = when(not(colAHasDigit), col(colA))
                .when(not(colBHasDigit), col(colB))
                .otherwise(lit(null));

        Dataset<Row> dfResult = df.withColumn(outputCol, selected).groupBy(outputCol).count();
        saveDatasetJson(dfResult, "out", "nature");
        long nNull= df.withColumn(outputCol, selected).select(outputCol)
                .filter(col(outputCol).isNull().or(col(outputCol).like("")))
                .count();
        System.out.println("NULL: "+nNull);
        return df.withColumn(outputCol, selected);
    }

    public Dataset<Row> contaProgettiRitardo(Dataset<Row> df){
        String inizio_eff = "OC_DATA_INIZIO_PROGETTO";
        String fine_prev = "OC_DATA_FINE_PROGETTO_PREVISTA";
        String fine_eff = "OC_DATA_FINE_PROGETTO_EFFETTIVA";

        Dataset<Row> dfConverted = convertDates(df, Arrays.asList(inizio_eff, fine_prev, fine_eff));

        // trova righe su cui si può calcolare il ritardo
        Dataset<Row> dfRitardabile = calcolaRitardabili(dfConverted, fine_prev, inizio_eff, fine_eff);

        long calcolabili = dfRitardabile.count();
        Dataset<Row> dfRitardo = calcolaRitardi(dfRitardabile, fine_prev, inizio_eff, fine_eff);
        long ritardo = dfRitardo.count();


        HashMap<String, Pair<Long>> map = new HashMap<>();
        StructType structType = new StructType()
                .add("fase", DataTypes.StringType)
                .add("contabili", DataTypes.LongType)
                .add("ritardi", DataTypes.LongType);

        map.put("Ritardi:", new Pair(calcolabili, ritardo));

        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, Pair<Long>> entry : map.entrySet()) {
            String fase = entry.getKey();
            Long contabili = entry.getValue().x;
            Long ritardi = entry.getValue().y;
            rows.add(RowFactory.create(fase, contabili, ritardi));
        }
        Dataset<Row> resultDF = spark.createDataFrame(rows, structType);
        saveDatasetJson(resultDF, "out", "ritardi.json");
        return resultDF;
    }

    public Dataset<Row> ritardiPerNatura(Dataset<Row> df){
        Dataset<Row> dfCalcNatura = selectNonNumericColumnValue(df, "CUP_COD_NATURA", "CUP_DESCR_NATURA", "NATURA" );
        Dataset<Row> dfResult =  contaProgettiRitardo(dfCalcNatura).groupBy(col("NATURA")).count();
        saveDatasetJson(dfResult, "out", "ritardi_per_natura.json");

        return dfResult;
    }

    public static void main(String[] args) {

        SparkApp app = new SparkApp();

        Dataset<Row> df = app.loadCsv("data/opencoesione/progetti_esteso_20250228.csv");
        // app.totalRows(df);
        app.ritardiPerNatura(df);
        //app.contaProgettiRitardo();
        //app.contaProgettiInRitardoPerFase();

        // app.addNaturaDerivataClassifier();


        //app.selectNonNumericColumnValue("CUP_COD_NATURA", "CUP_DESCR_NATURA", "NATURA");

        //app.contaProgettiInRitardoPerFase();
        /*
        List<String> headers = Arrays.asList(app.df.columns());

        Dataset<Row> stats = app.generateColumnsStats(headers);
        stats.show();
        stats.write().mode("overwrite").json("statsColonne.json");

         */

    }

    private Dataset<Row> convertDates(Dataset<Row> df, List<String> colums) {
        Column filterCondition = lit(true); // punto di partenza: sempre vero
        for (String colName : colums) {
            Column cond = col(colName).rlike("^\\d{8}$").or(col(colName).isNull());
            filterCondition = filterCondition.and(cond);
        }
        Dataset<Row> dfFiltered = df.filter(filterCondition);

        Dataset<Row> dfConverted = dfFiltered;
        for (String colName : colums) {
            dfConverted = dfConverted.withColumn(colName, to_date(col(colName), "yyyyMMdd"));
        }
        return dfConverted;
    }



    private void saveDatasetJson(Dataset<Row> df, String destinationFolder, String jsonFileName)  {
        // Assicura che il nome del file abbia estensione .json
        if (!jsonFileName.endsWith(".json")) {
            jsonFileName = jsonFileName + ".json";
        }

        // Crea directory temporanea con timestamp per evitare conflitti
        String tempDir = destinationFolder + "/temp_" + System.currentTimeMillis();

        try {
            // Salva il DataFrame come singolo file JSON nella directory temporanea
            df.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .json(tempDir);

            // Crea la cartella di destinazione se non esiste
            Files.createDirectories(Paths.get(destinationFolder));

            // Percorso completo del file di destinazione
            String destFilePath = destinationFolder + "/" + jsonFileName;

            // Trova il file JSON generato nella directory temporanea
            File tempFolder = new File(tempDir);
            File[] files = tempFolder.listFiles((dir, name) -> name.startsWith("part-") && name.endsWith(".json"));

            if (files != null && files.length > 0) {
                // Sposta e rinomina il file
                Files.move(files[0].toPath(),
                        Paths.get(destFilePath),
                        StandardCopyOption.REPLACE_EXISTING);

                System.out.println("File JSON creato con successo: " + destFilePath);
            }


        } catch (IOException e) {
            System.out.println("errore IO");
        } finally {
            // Elimina la directory temporanea
            try {
                FileUtils.deleteDirectory(new File(tempDir));
            } catch (Exception e) {
                System.err.println("Errore durante la pulizia della directory temporanea: " + e.getMessage());
            }
        }
    }

    public void totalRows(Dataset<Row> df){
        long nRows = df.count();
        Dataset<Row> dfResult = spark.createDataFrame(Arrays.asList(RowFactory.create(nRows)), new StructType().add("totale righe", DataTypes.LongType));
        saveDatasetJson(dfResult, "out", "totaleRighe.json");
    }

    /*
    Trova le righe su cui è possibile trovare i ritardi, ovvero le righe che hanno
    almeno fine_prev e almeno uno tra inizio_eff e fine_eff
     */

    public Dataset<Row> calcolaRitardabili(Dataset<Row> df, String fine_prev, String inizio_eff, String fine_eff) {
        // trova righe su cui si può calcolare il ritardo
        Dataset<Row> dfRitardabile = df.filter(
                col(fine_prev).isNotNull()
                        .and(col(fine_eff).isNotNull().or(col(inizio_eff).isNull())));
        return dfRitardabile;
    }

    public Dataset<Row> calcolaRitardi(Dataset<Row> df, String fine_prev, String inizio_eff, String fine_eff){
        Dataset<Row> dfResult = df.filter(
                    (
                            col(fine_prev).isNotNull()
                                    .and(col(fine_eff).isNotNull())
                                    .and(
                                            col(fine_prev).lt(col(fine_eff))
                                                    .or(col(fine_prev).lt(col(inizio_eff)))
                                    )
                    )
                            .or(
                                    col(fine_eff).isNull()
                                            .and(col(inizio_eff).isNotNull())
                                            .and(col(fine_prev).lt(current_date()))
                            )
                );
        return df;
    }

    private Dataset<Row> createRitardiDfFromMap(){
        return null;
    }

}