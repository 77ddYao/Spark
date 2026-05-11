package org.yhm.spark2.service;

public interface SparkService {
    Object queryData(String start, String end);
    Object closeCalls(String start, String end);
    Object speedAnomaly(String start, String end);
    Object heatmap(String start, String end);
    Object mmsiAnomaly(String start, String end);
}
