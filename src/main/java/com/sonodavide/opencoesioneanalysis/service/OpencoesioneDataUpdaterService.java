package com.sonodavide.opencoesioneanalysis.service;

import com.sonodavide.opencoesioneanalysis.others.CheckUpdate;
import com.sonodavide.opencoesioneanalysis.others.LastModifiedUpdaterFile;
import com.sonodavide.opencoesioneanalysis.utils.FileUtils;
import com.sonodavide.opencoesioneanalysis.utils.UrlUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;


@Service
public class OpencoesioneDataUpdaterService {
    private final String SAVE_PATH="data/opencoesione/";
    private final String SAVE_ZIP_PATH="data/opencoesione/zip/";
    private final String DOWNLOAD_PATH="data/opencoesione/dwl/";
    private final String ALL_PROJECTS_URL="https://opencoesione.gov.it/it/opendata/progetti_esteso.zip";
    private final String CALABRIA_PROJECTS_URL="https://opencoesione.gov.it/it/opendata/regioni/progetti_esteso_CAL.zip";
    public void updateData(boolean overrideUpdate) throws IOException {
        createSaveDir();
        CheckUpdate checkUpdate = new CheckUpdate(new LastModifiedUpdaterFile());
        if(checkUpdate.checkUpdate(CALABRIA_PROJECTS_URL) | overrideUpdate|FileUtils.countFiles(SAVE_ZIP_PATH)==0){
            System.out.println("Downloading OpenCoesione data...");
            InputStream in = new URL(CALABRIA_PROJECTS_URL).openStream();
            String fileName = UrlUtils.getFilenameFromUrl(CALABRIA_PROJECTS_URL);
            Files.copy(in, Paths.get(DOWNLOAD_PATH + fileName), StandardCopyOption.REPLACE_EXISTING);
            File f = new File(DOWNLOAD_PATH + fileName);
            if (f.exists()) {
                Path targetPath = Paths.get(SAVE_ZIP_PATH + fileName);
                Files.move(f.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
            }
            in.close();
            FileUtils.unzip(SAVE_ZIP_PATH+fileName, SAVE_PATH);
        }else{
            return;
        }






    }



    private void createSaveDir() {
        File saveDir = new File(SAVE_PATH);
        if (!saveDir.exists()) {
            saveDir.mkdirs();
        }
        File zipDir = new File(SAVE_ZIP_PATH);
        if(!zipDir.exists()) {
            zipDir.mkdirs();
        }
        File dwlDir = new File(DOWNLOAD_PATH);
        if(!dwlDir.exists()) {
            dwlDir.mkdirs();
        }
    }


    public boolean isUpdated(String urlString) {
        try {

            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("HEAD");

            long lastModified = conn.getLastModified();
            conn.disconnect();

            if (lastModified == 0) {
                System.out.println("Il server non ha fornito un header 'Last-Modified'.");
                return false;
            }

            File fileStato = new File(SAVE_PATH+UrlUtils.getFilenameFromUrl(urlString)+".txt");


            if (!fileStato.exists()) {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileStato))) {
                    writer.write(Long.toString(lastModified));
                }
                return true;
            }


            long dataSalvata;
            try (BufferedReader reader = new BufferedReader(new FileReader(fileStato))) {
                dataSalvata = Long.parseLong(reader.readLine().trim());
            }


            if (dataSalvata != lastModified) {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileStato))) {
                    writer.write(Long.toString(lastModified));
                }
                return true;
            }


            return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
