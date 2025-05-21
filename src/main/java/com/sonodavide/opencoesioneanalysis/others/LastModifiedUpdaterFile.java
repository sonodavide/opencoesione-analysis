package com.sonodavide.opencoesioneanalysis.others;

import com.sonodavide.opencoesioneanalysis.utils.UrlUtils;

import java.io.*;
import java.net.MalformedURLException;

public class LastModifiedUpdaterFile implements LastModifiedUpdater {
    @Override
    public long read(String url) {
        File fileStato = null;
        try {
            fileStato = new File(UrlUtils.getFilenameFromUrl(url)+".txt");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }


        if (!fileStato.exists()) {

            return Long.MIN_VALUE;
        }


        long dataSalvata;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileStato))) {
            dataSalvata = Long.parseLong(reader.readLine().trim());

            return dataSalvata;
        }  catch (IOException e) {

        }
        return Long.MIN_VALUE;
    }

    @Override
    public void update(String url, long lastModified) {
        File fileStato = null;
        try {
            fileStato = new File(UrlUtils.getFilenameFromUrl(url)+".txt");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileStato))) {
            writer.write(Long.toString(lastModified));
        }catch (IOException e) {}
    }
}
