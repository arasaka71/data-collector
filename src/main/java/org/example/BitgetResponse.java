package org.example;

public record BitgetResponse<T> (
        String code,
        String msg,
        long requestTime,
        T data
){ }
