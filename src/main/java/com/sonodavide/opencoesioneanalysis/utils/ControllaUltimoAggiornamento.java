package com.sonodavide.opencoesioneanalysis.utils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;

public class ControllaUltimoAggiornamento {
    public static void main(String[] args) {
        // Inserisci qui il tuo link diretto
        String linkFile = "https://opencoesione.gov.it/it/opendata/progetti_esteso.zip";

        try {
            URL url = new URL(linkFile);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("HEAD");

            // Esegue la richiesta
            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                long lastModified = conn.getLastModified();
                if (lastModified == 0) {
                    System.out.println("Header 'Last-Modified' non disponibile.");
                } else {
                    System.out.println("Ultima modifica: " + new Date(lastModified));
                }
            } else {
                System.out.println("Errore HTTP: " + responseCode);
            }

            conn.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
