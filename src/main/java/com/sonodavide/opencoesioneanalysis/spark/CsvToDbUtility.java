package com.sonodavide.opencoesioneanalysis.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import java.util.Properties;

/**
 * Utility per importare un file CSV in un database utilizzando Spark.
 * Inferisce automaticamente lo schema dal CSV e crea una tabella nel database.
 */
public class CsvToDbUtility {

    /**
     * Importa un file CSV in un database creando una tabella con la struttura appropriata.
     *
     * @param csvPath Percorso del file CSV da importare
     * @param dbUrl URL di connessione JDBC al database (es. "jdbc:postgresql://localhost:5432/mydb")
     * @param dbTable Nome della tabella da creare/popolare
     * @param dbUser Username per il database
     * @param dbPassword Password per il database
     * @param delimiter Delimitatore usato nel CSV (default: ",")
     * @param header Se il CSV contiene una riga di intestazione (default: true)
     * @param inferSchema Se inferire lo schema automaticamente (default: true)
     * @param saveMode Modalità di salvataggio ("overwrite", "append", "ignore", "error")
     */
    public static void importCsvToDb(
            String csvPath,
            String dbUrl,
            String dbTable,
            String dbUser,
            String dbPassword,
            String delimiter,
            boolean header,
            boolean inferSchema,
            String saveMode) {

        // Crea la sessione Spark
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB Import")
                .master("local[*]")
                .getOrCreate();

        try {
            // Configura le opzioni per la lettura del CSV
            Dataset<Row> df = spark.read()
                    .option("delimiter", delimiter)
                    .option("header", header)
                    .option("inferSchema", inferSchema)
                    .csv(csvPath);

            // Stampa lo schema inferito
            System.out.println("Schema inferito dal CSV:");
            df.printSchema();

            // Crea le proprietà di connessione al database
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", dbUser);
            connectionProperties.put("password", dbPassword);

            // Importa i dati nel database
            df.write()
                    .mode(saveMode)
                    .jdbc(dbUrl, dbTable, connectionProperties);

            System.out.println("Importazione completata con successo nella tabella: " + dbTable);

        } catch (Exception e) {
            System.err.println("Errore durante l'importazione: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Chiudi la sessione Spark
            spark.stop();
        }
    }

    /**
     * Versione semplificata che utilizza valori predefiniti per alcuni parametri.
     */
    public static void importCsvToDb(
            String csvPath,
            String dbUrl,
            String dbTable,
            String dbUser,
            String dbPassword) {

        importCsvToDb(csvPath, dbUrl, dbTable, dbUser, dbPassword, ",", true, true, "overwrite");
    }

    /**
     * Metodo main per eseguire l'utility da riga di comando.
     */
    public static void main(String[] args) {


        String csvPath = "data/opencoesione/progetti_esteso_20250228.csv";
        String dbUrl = "jdbc:postgresql://localhost:5432/prova";
        String dbTable = "progetti_esteso";
        String dbUser = "postgres";
        String dbPassword = "admin";

        String delimiter = ";";
        boolean header =  true;
        boolean inferSchema = true;
        String saveMode = "overwrite";

        importCsvToDb(csvPath, dbUrl, dbTable, dbUser, dbPassword, delimiter, header, inferSchema, saveMode);
    }
}
