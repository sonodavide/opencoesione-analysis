package com.sonodavide.opencoesioneanalysis.utils;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlUtils {
    public static String getFilenameFromUrl(String urlString) throws MalformedURLException {
        URL url = new URL(urlString);
        String path = url.getPath();
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        return fileName.split("\\?")[0];
    }
}
