package com.sonodavide.opencoesioneanalysis.utils;

import lombok.AllArgsConstructor;

public class Pair<T> {
    public T x;
    public T y;

    public Pair(T x, T y) {
        this.x = x;
        this.y = y;
    }
}
