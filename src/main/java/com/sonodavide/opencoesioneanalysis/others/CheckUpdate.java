package com.sonodavide.opencoesioneanalysis.others;

import com.sonodavide.opencoesioneanalysis.utils.UrlUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class CheckUpdate {
    LastModifiedUpdater lastModifiedUpdater;

    public CheckUpdate(LastModifiedUpdater lastModifiedUpdater) {
        this.lastModifiedUpdater = lastModifiedUpdater;
    }

    public boolean checkUpdate(String urlString) {
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

            long dataSalvata = lastModifiedUpdater.read(urlString);


            if (dataSalvata < lastModified) {
                lastModifiedUpdater.update(urlString, lastModified);
                return true;
            }


            return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
