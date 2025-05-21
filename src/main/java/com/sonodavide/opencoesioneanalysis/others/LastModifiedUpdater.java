package com.sonodavide.opencoesioneanalysis.others;

public interface LastModifiedUpdater {
    long read(String url);
    void update(String url, long lastModified);
}
