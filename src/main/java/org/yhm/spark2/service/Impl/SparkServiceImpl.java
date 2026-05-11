package org.yhm.spark2.service.Impl;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.stereotype.Service;
import org.yhm.spark2.service.SparkService;

import static org.apache.spark.sql.functions.*;

@Service
public class SparkServiceImpl implements SparkService {

    private static final Logger log = Logger.getLogger(SparkServiceImpl.class);
    private static final String HDFS_PATH = "hdfs://localhost:9000/data/ais/raw/";

    private SparkSession getSession() {
        return SparkSession.builder()
                .appName("AisAnalysis")
                .master("local[*]")
                .getOrCreate();
    }

    // ============ 原始查询 ============
    @Override
    public Object queryData(String start, String end) {
        SparkSession spark = getSession();
        Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true").csv(HDFS_PATH);
        Dataset<Row> filtered = df.filter(df.col("base_date_time").between(start, end));
        filtered.show();
        String[] json = filtered.toJSON().collectAsList().toArray(new String[0]);
        spark.close();
        return json;
    }

    // ============ 1. 瞬时近距离告警（同一时刻两船 < 500m）============
    @Override
    public Object closeCalls(String start, String end) {
        log.info("closeCalls: " + start + " ~ " + end);
        SparkSession spark = getSession();

        Dataset<Row> raw = spark.read().option("header", "true").option("inferSchema", "true").csv(HDFS_PATH);
        Dataset<Row> df = raw.filter(raw.col("base_date_time").between(start, end))
                .select(
                        col("mmsi").cast("int"),
                        col("base_date_time"),
                        col("longitude").cast("double").alias("lon"),
                        col("latitude").cast("double").alias("lat"),
                        col("sog").cast("double"),
                        col("vessel_name")
                )
                .filter(col("lon").isNotNull().and(col("lat").isNotNull()));

        // Self-join on same timestamp, different MMSI
        Dataset<Row> a = df.select(
                col("base_date_time").alias("t1"),
                col("mmsi").alias("mmsi1"), col("vessel_name").alias("name1"),
                col("lon").alias("lon1"), col("lat").alias("lat1"),
                col("sog").alias("sog1")
        );
        Dataset<Row> b = df.select(
                col("base_date_time").alias("t2"),
                col("mmsi").alias("mmsi2"), col("vessel_name").alias("name2"),
                col("lon").alias("lon2"), col("lat").alias("lat2"),
                col("sog").alias("sog2")
        );

        // Haversine distance in meters
        double R = 6371000.0;
        String haversine = String.format(
            "2 * %f * asin(sqrt(" +
            "pow(sin(radians(lat2 - lat1) / 2), 2) + " +
            "cos(radians(lat1)) * cos(radians(lat2)) * " +
            "pow(sin(radians(lon2 - lon1) / 2), 2)))", R);

        Dataset<Row> close = a.join(b, a.col("t1").equalTo(b.col("t2"))
                        .and(a.col("mmsi1").lt(b.col("mmsi2"))))
                .withColumn("distance_m", expr(haversine))
                .filter(col("distance_m").lt(500))
                .select(
                        a.col("t1").alias("time"),
                        col("mmsi1"), col("name1"), col("lon1"), col("lat1"), col("sog1"),
                        col("mmsi2"), col("name2"), col("lon2"), col("lat2"), col("sog2"),
                        round(col("distance_m"), 1).alias("distance_m")
                )
                .orderBy(col("distance_m"));

        log.info("Close calls found: " + close.count());
        String[] json = close.toJSON().collectAsList().toArray(new String[0]);
        spark.close();
        return json;
    }

    // ============ 2. 航速突变检测 ============
    @Override
    public Object speedAnomaly(String start, String end) {
        log.info("speedAnomaly: " + start + " ~ " + end);
        SparkSession spark = getSession();

        Dataset<Row> raw = spark.read().option("header", "true").option("inferSchema", "true").csv(HDFS_PATH);
        Dataset<Row> df = raw.filter(raw.col("base_date_time").between(start, end))
                .select(
                        col("mmsi").cast("int"),
                        to_timestamp(col("base_date_time"), "yyyy-MM-dd HH:mm:ss").alias("ts"),
                        col("longitude").cast("double").alias("lon"),
                        col("latitude").cast("double").alias("lat"),
                        col("sog").cast("double"),
                        col("vessel_name")
                )
                .filter(col("sog").isNotNull());

        WindowSpec win = Window.partitionBy("mmsi").orderBy("ts");

        Dataset<Row> withDiff = df
                .withColumn("prev_sog", lag("sog", 1).over(win))
                .withColumn("prev_ts", lag("ts", 1).over(win))
                .withColumn("delta_sog", abs(col("sog").minus(col("prev_sog"))))
                .withColumn("delta_seconds", col("ts").cast("long").minus(col("prev_ts").cast("long")))
                .filter(col("prev_sog").isNotNull())
                .filter(col("delta_seconds").gt(2))  // avoid same-second duplicates
                .withColumn("accel_kt_per_min", round(col("delta_sog").multiply(60).divide(col("delta_seconds")), 2));

        // Flag anomalies: SOG change > 5 knots in 1 minute (hard braking/acceleration)
        Dataset<Row> anomaly = withDiff
                .filter(col("accel_kt_per_min").gt(5))
                .select(
                        col("mmsi"), col("vessel_name"),
                        col("ts").alias("time"),
                        col("prev_ts").alias("prev_time"),
                        col("prev_sog"), col("sog").alias("current_sog"),
                        col("delta_sog").alias("sog_change_kt"),
                        col("delta_seconds").alias("interval_sec"),
                        col("accel_kt_per_min"),
                        col("lon"), col("lat")
                )
                .orderBy(col("accel_kt_per_min").desc());

        log.info("Speed anomalies: " + anomaly.count());
        String[] json = anomaly.toJSON().collectAsList().toArray(new String[0]);
        spark.close();
        return json;
    }

    // ============ 3. 区域热力图 ============
    @Override
    public Object heatmap(String start, String end) {
        log.info("heatmap: " + start + " ~ " + end);
        SparkSession spark = getSession();

        Dataset<Row> raw = spark.read().option("header", "true").option("inferSchema", "true").csv(HDFS_PATH);
        Dataset<Row> df = raw.filter(raw.col("base_date_time").between(start, end))
                .select(
                        col("longitude").cast("double").alias("lon"),
                        col("latitude").cast("double").alias("lat")
                )
                .filter(col("lon").isNotNull().and(col("lat").isNotNull()));

        // 0.1 degree grid (~11km at equator)
        Dataset<Row> heatmap = df
                .withColumn("grid_lon", round(col("lon"), 1))
                .withColumn("grid_lat", round(col("lat"), 1))
                .groupBy("grid_lon", "grid_lat")
                .agg(count("*").alias("count"))
                .orderBy(col("count").desc());

        // Add density category labels
        Dataset<Row> result = heatmap
                .withColumn("density",
                        when(col("count").gt(1000), "极高(>1000)")
                        .when(col("count").gt(500), "高(500-1000)")
                        .when(col("count").gt(100), "中(100-500)")
                        .when(col("count").gt(10), "低(10-100)")
                        .otherwise("稀疏(<10)"))
                .select(
                        col("grid_lon").alias("lon_center"),
                        col("grid_lat").alias("lat_center"),
                        col("count").alias("ship_count"),
                        col("density")
                );

        log.info("Heatmap cells: " + result.count());
        String[] json = result.toJSON().collectAsList().toArray(new String[0]);
        spark.close();
        return json;
    }

    // ============ 4. MMSI 时空异常检测 ============
    @Override
    public Object mmsiAnomaly(String start, String end) {
        log.info("mmsiAnomaly: " + start + " ~ " + end);
        SparkSession spark = getSession();

        Dataset<Row> raw = spark.read().option("header", "true").option("inferSchema", "true").csv(HDFS_PATH);
        Dataset<Row> df = raw.filter(raw.col("base_date_time").between(start, end))
                .select(
                        col("mmsi").cast("int"),
                        to_timestamp(col("base_date_time"), "yyyy-MM-dd HH:mm:ss").alias("ts"),
                        col("longitude").cast("double").alias("lon"),
                        col("latitude").cast("double").alias("lat"),
                        col("sog").cast("double"),
                        col("vessel_name")
                )
                .filter(col("lon").isNotNull().and(col("lat").isNotNull()));

        WindowSpec win = Window.partitionBy("mmsi").orderBy("ts");

        double R = 6371000.0;
        String haversineExpr = String.format(
            "2 * %f * asin(sqrt(" +
            "pow(sin(radians(lat - prev_lat) / 2), 2) + " +
            "cos(radians(prev_lat)) * cos(radians(lat)) * " +
            "pow(sin(radians(lon - prev_lon) / 2), 2)))", R);

        Dataset<Row> withAnomaly = df
                .withColumn("prev_ts", lag("ts", 1).over(win))
                .withColumn("prev_lon", lag("lon", 1).over(win))
                .withColumn("prev_lat", lag("lat", 1).over(win))
                .filter(col("prev_ts").isNotNull())
                .withColumn("time_diff_sec", col("ts").cast("long").minus(col("prev_ts").cast("long")))
                .filter(col("time_diff_sec").gt(0))
                .withColumn("distance_m", expr(haversineExpr))
                .withColumn("speed_kt", round(col("distance_m").multiply(1.94384).divide(col("time_diff_sec")), 1));

        // Detect: calculated speed between two points > 50 knots (physically impossible for most ships)
        Dataset<Row> anomaly = withAnomaly
                .filter(col("speed_kt").gt(50))
                .select(
                        col("mmsi"), col("vessel_name"),
                        col("prev_ts").alias("from_time"), col("ts").alias("to_time"),
                        col("prev_lon").alias("from_lon"), col("prev_lat").alias("from_lat"),
                        col("lon").alias("to_lon"), col("lat").alias("to_lat"),
                        col("time_diff_sec"), round(col("distance_m"), 0).alias("distance_m"),
                        col("speed_kt")
                )
                .orderBy(col("speed_kt").desc());

        log.info("MMSI anomalies: " + anomaly.count());
        String[] json = anomaly.toJSON().collectAsList().toArray(new String[0]);
        spark.close();
        return json;
    }
}
