package com.sonodavide.opencoesioneanalysis.utils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
public class OpencoesioneUpdateChecker {
    private static String url = "https://opencoesione.gov.it/it/opendata/#!progetti_section";
    private static final String FILE_STATO = "stato_precedente.txt";
    private static final int TIMEOUT_CONNESSIONE = 30000;

    public static boolean verificaAggiornamento() throws IOException {
        System.out.println("ci sono aggiornamenti sul dataset!");
        Document doc = Jsoup.connect(url)
                .timeout(TIMEOUT_CONNESSIONE)
                .get();

        String testoAttuale = doc.select("p.notes").text();



        String statoPrecedente = leggiStatoPrecedente();

        if (statoPrecedente == null) {
            salvaStatoAttuale(testoAttuale);
            return true;
        }

        boolean cambiato = !testoAttuale.equals(statoPrecedente);

        if (cambiato) {
            salvaStatoAttuale(testoAttuale);

        }

        return cambiato;
    }


    private static String leggiStatoPrecedente() {
        try {
            Path path = Paths.get(FILE_STATO);
            if (Files.exists(path)) {
                return Files.readString(path);
            }
        } catch (IOException e) {
            System.err.println("Errore nella lettura dello stato precedente: " + e.getMessage());
        }
        return null;
    }


    private static void salvaStatoAttuale(String statoAttuale) throws IOException {
        Path path = Paths.get(FILE_STATO);
        Files.writeString(path, statoAttuale);
    }




}
