package org.yhm.spark2.service.Impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.stereotype.Service;
import org.yhm.spark2.service.SparkService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.apache.spark.sql.functions.*;

@Service
public class SparkServiceImpl implements SparkService {

    private static final Logger log = Logger.getLogger(SparkServiceImpl.class);
    private static final String PARQUET_PATH = "hdfs://localhost:9000/data/ais/parquet/";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private SparkSession spark;

    @PostConstruct
    public void init() {
        spark = SparkSession.builder()
                .appName("AisAnalysis")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();
        log.info("SparkSession initialized. Parquet source: " + PARQUET_PATH);
    }

    @PreDestroy
    public void cleanup() {
        if (spark != null) {
            spark.close();
            log.info("SparkSession closed.");
        }
    }

    private Dataset<Row> loadParquet() {
        return spark.read().parquet(PARQUET_PATH);
    }

    // ============ 快照查询：查询某一时刻所有船舶数据 ============
    @Override
    public Object querySnapshot(String timestamp) {
        log.info("querySnapshot: " + timestamp);
        Dataset<Row> df = loadParquet();
        Dataset<Row> filtered = df.filter(df.col("base_date_time").equalTo(timestamp));
        log.info("Snapshot count: " + filtered.count());
        String[] json = filtered.toJSON().collectAsList().toArray(new String[0]);
        return json;
    }

    // ============ 原始查询 ============
    @Override
    public Object queryData(String start, String end) {
        log.info("queryData: " + start + " ~ " + end);
        Dataset<Row> df = loadParquet();
        Dataset<Row> filtered = df.filter(df.col("base_date_time").between(start, end));
        filtered.show();
        String[] json = filtered.toJSON().collectAsList().toArray(new String[0]);
        return json;
    }

    // ============ 1. 瞬时近距离告警（同一时刻两船 < 500m）============
    @Override
    public Object closeCalls(String start, String end) {
        log.info("closeCalls: " + start + " ~ " + end);

        Dataset<Row> df = loadParquet()
                .filter(col("base_date_time").between(start, end))
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
        return json;
    }

    // ============ 2. 航速突变检测 ============
    @Override
    public Object speedAnomaly(String start, String end) {
        log.info("speedAnomaly: " + start + " ~ " + end);

        Dataset<Row> df = loadParquet()
                .filter(col("base_date_time").between(start, end))
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
                .filter(col("delta_seconds").gt(2))
                .withColumn("accel_kt_per_min", round(col("delta_sog").multiply(60).divide(col("delta_seconds")), 2));

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
        return json;
    }

    // ============ 3. 区域热力图 ============
    @Override
    public Object heatmap(String start, String end) {
        log.info("heatmap: " + start + " ~ " + end);

        Dataset<Row> df = loadParquet()
                .filter(col("base_date_time").between(start, end))
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
        return json;
    }

    // ============ 4. 航迹生成：按时间序列返回指定船只轨迹（含卡尔曼平滑）============
    @Override
    public Object vesselTrack(String mmsi, String start, String end) {
        log.info("vesselTrack: MMSI=" + mmsi + " | " + start + " ~ " + end);

        Dataset<Row> df = loadParquet()
                .filter(col("base_date_time").between(start, end))
                .filter(col("mmsi").equalTo(mmsi))
                .select(
                        col("mmsi").cast("int"),
                        col("base_date_time").alias("time"),
                        col("longitude").cast("double").alias("lon"),
                        col("latitude").cast("double").alias("lat"),
                        col("sog").cast("double"),
                        col("cog").cast("double"),
                        col("vessel_name"),
                        col("imo"),
                        col("vessel_type"),
                        col("status").alias("nav_status"),
                        col("heading").cast("double")
                )
                .filter(col("lon").isNotNull().and(col("lat").isNotNull()))
                .orderBy("time");

        long count = df.count();
        log.info("Track points for MMSI " + mmsi + ": " + count);

        if (count == 0) {
            return "[]";
        }

        String[] rawJson = df.toJSON().collectAsList().toArray(new String[0]);
        return applyKalmanSmooth(rawJson, mmsi);
    }

    /**
     * 卡尔曼滤波平滑：对轨迹点的 lon/lat 做 2D 匀速模型平滑，
     * 同时在 JSON 中附加 smoothed_lon / smoothed_lat 字段，保留原始值。
     */
    private Object applyKalmanSmooth(String[] rawJson, String mmsi) {
        try {
            List<ObjectNode> nodes = new ArrayList<>();
            List<Double> lons = new ArrayList<>();
            List<Double> lats = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();

            for (String s : rawJson) {
                ObjectNode node = (ObjectNode) mapper.readTree(s);
                nodes.add(node);
                lons.add(node.get("lon").asDouble());
                lats.add(node.get("lat").asDouble());
                long ts = parseTimestamp(node.get("time").asText());
                timestamps.add(ts);
            }

            int n = nodes.size();
            if (n < 2) {
                // 单点无需平滑
                for (ObjectNode node : nodes) {
                    node.put("smoothed_lon", node.get("lon").asDouble());
                    node.put("smoothed_lat", node.get("lat").asDouble());
                }
                return nodes.stream().map(ObjectNode::toString).toArray(String[]::new);
            }

            // ---- 卡尔曼滤波 2D 匀速模型 ----
            // 状态 x = [lon, lat, v_lon, v_lat]
            double lon = lons.get(0), lat = lats.get(0);
            double vLon = 0, vLat = 0;

            // 初始估计 — 用前两个点差分
            double dt0 = (timestamps.get(1) - timestamps.get(0)) / 1000.0;
            if (dt0 <= 0) dt0 = 1;
            vLon = (lons.get(1) - lons.get(0)) / dt0;
            vLat = (lats.get(1) - lats.get(0)) / dt0;

            double[] x = {lon, lat, vLon, vLat};

            // 协方差矩阵 P (4x4)，初始不确定性
            double[][] P = {
                {1e-6, 0, 0, 0},
                {0, 1e-6, 0, 0},
                {0, 0, 1e-4, 0},
                {0, 0, 0, 1e-4}
            };

            // 过程噪声 Q (位置 ~1e-12 deg², 速度 ~1e-10)
            double[][] Q = {
                {1e-12, 0, 0, 0},
                {0, 1e-12, 0, 0},
                {0, 0, 1e-10, 0},
                {0, 0, 0, 1e-10}
            };

            // 测量噪声 R — GPS 精度约 3m ≈ 3e-5 deg
            double r = 9e-10; // (~3m)² in deg²
            double[][] R = {{r, 0}, {0, r}};

            // 测量矩阵 H
            double[][] H = {{1, 0, 0, 0}, {0, 1, 0, 0}};

            // 存平滑结果
            double[] smoothLon = new double[n];
            double[] smoothLat = new double[n];
            smoothLon[0] = lon;
            smoothLat[0] = lat;

            for (int i = 1; i < n; i++) {
                double dt = (timestamps.get(i) - timestamps.get(i - 1)) / 1000.0;
                if (dt <= 0) dt = 1;

                // ---- 预测 ----
                // F = [[1,0,dt,0],[0,1,0,dt],[0,0,1,0],[0,0,0,1]]
                double[][] F = {
                    {1, 0, dt, 0},
                    {0, 1, 0, dt},
                    {0, 0, 1, 0},
                    {0, 0, 0, 1}
                };

                double[] xPred = matVecMul4(F, x);
                double[][] PPred = matAdd4(matMul44(matMul44(F, P), transpose4(F)), Q);

                // ---- 更新 ----
                double[] z = {lons.get(i), lats.get(i)};
                double[] y = {z[0] - (H[0][0]*xPred[0] + H[0][1]*xPred[1] + H[0][2]*xPred[2] + H[0][3]*xPred[3]),
                              z[1] - (H[1][0]*xPred[0] + H[1][1]*xPred[1] + H[1][2]*xPred[2] + H[1][3]*xPred[3])};

                // S = H*P*H' + R  (2x2)
                double s00 = PPred[0][0] + R[0][0];
                double s01 = PPred[0][1] + R[0][1];
                double s10 = PPred[1][0] + R[1][0];
                double s11 = PPred[1][1] + R[1][1];
                double detS = s00 * s11 - s01 * s10;
                if (Math.abs(detS) < 1e-20) detS = 1e-20;

                // K = P*H' * inv(S)  (4x2)
                double k00 = (PPred[0][0] * s11 - PPred[0][1] * s10) / detS;
                double k01 = (PPred[0][0] * -s01 + PPred[0][1] * s00) / detS;
                double k10 = (PPred[1][0] * s11 - PPred[1][1] * s10) / detS;
                double k11 = (PPred[1][0] * -s01 + PPred[1][1] * s00) / detS;
                double k20 = (PPred[2][0] * s11 - PPred[2][1] * s10) / detS;
                double k21 = (PPred[2][0] * -s01 + PPred[2][1] * s00) / detS;
                double k30 = (PPred[3][0] * s11 - PPred[3][1] * s10) / detS;
                double k31 = (PPred[3][0] * -s01 + PPred[3][1] * s00) / detS;

                x[0] = xPred[0] + k00 * y[0] + k01 * y[1];
                x[1] = xPred[1] + k10 * y[0] + k11 * y[1];
                x[2] = xPred[2] + k20 * y[0] + k21 * y[1];
                x[3] = xPred[3] + k30 * y[0] + k31 * y[1];

                // P = (I - K*H) * P
                double[][] KH = {
                    {k00, k01*0, k00, k01*0},  // K*H simplified
                    {k10, 0, k10, 0},
                    {k20, 0, k20, 0},
                    {k30, 0, k30, 0}
                };
                // Full: KH = K * H
                double[][] KHfull = {
                    {k00 * H[0][0] + k01 * H[1][0], k00 * H[0][1] + k01 * H[1][1], k00 * H[0][2] + k01 * H[1][2], k00 * H[0][3] + k01 * H[1][3]},
                    {k10 * H[0][0] + k11 * H[1][0], k10 * H[0][1] + k11 * H[1][1], k10 * H[0][2] + k11 * H[1][2], k10 * H[0][3] + k11 * H[1][3]},
                    {k20 * H[0][0] + k21 * H[1][0], k20 * H[0][1] + k21 * H[1][1], k20 * H[0][2] + k21 * H[1][2], k20 * H[0][3] + k21 * H[1][3]},
                    {k30 * H[0][0] + k31 * H[1][0], k30 * H[0][1] + k31 * H[1][1], k30 * H[0][2] + k31 * H[1][2], k30 * H[0][3] + k31 * H[1][3]}
                };

                // Since H = [[1,0,0,0],[0,1,0,0]], KH = K*H simplifies to:
                // [[k00, k01, 0, 0],
                //  [k10, k11, 0, 0],
                //  [k20, k21, 0, 0],
                //  [k30, k31, 0, 0]]
                double[][] I4 = {{1,0,0,0},{0,1,0,0},{0,0,1,0},{0,0,0,1}};

                // I - KH
                double[][] I_KH = {
                    {1 - k00, -k01, 0, 0},
                    {-k10, 1 - k11, 0, 0},
                    {-k20, -k21, 1, 0},
                    {-k30, -k31, 0, 1}
                };

                P = matMul44(I_KH, PPred);

                smoothLon[i] = x[0];
                smoothLat[i] = x[1];
            }

            // 写回 JSON
            String[] result = new String[n];
            for (int i = 0; i < n; i++) {
                ObjectNode node = nodes.get(i);
                node.put("smoothed_lon", round6(smoothLon[i]));
                node.put("smoothed_lat", round6(smoothLat[i]));
                result[i] = node.toString();
            }

            log.info("Kalman smoothed " + n + " track points for MMSI " + mmsi);
            return result;

        } catch (Exception e) {
            log.error("Kalman filter failed, returning raw data: " + e.getMessage());
            return rawJson;
        }
    }

    // ---- 矩阵工具 (4x4 / 4x1) ----

    private static double[] matVecMul4(double[][] A, double[] v) {
        double[] r = new double[4];
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 4; j++)
                r[i] += A[i][j] * v[j];
        return r;
    }

    private static double[][] matMul44(double[][] A, double[][] B) {
        double[][] R = new double[4][4];
        for (int i = 0; i < 4; i++)
            for (int k = 0; k < 4; k++)
                if (A[i][k] != 0)
                    for (int j = 0; j < 4; j++)
                        R[i][j] += A[i][k] * B[k][j];
        return R;
    }

    private static double[][] transpose4(double[][] A) {
        double[][] R = new double[4][4];
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 4; j++)
                R[j][i] = A[i][j];
        return R;
    }

    private static double[][] matAdd4(double[][] A, double[][] B) {
        double[][] R = new double[4][4];
        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 4; j++)
                R[i][j] = A[i][j] + B[i][j];
        return R;
    }

    private static double round6(double v) {
        return Math.round(v * 1_000_000.0) / 1_000_000.0;
    }

    private static long parseTimestamp(String timeStr) {
        try {
            return sdf.parse(timeStr).getTime();
        } catch (Exception e) {
            return 0;
        }
    }

    // ============ 5. MMSI 时空异常检测 ============
    @Override
    public Object mmsiAnomaly(String start, String end) {
        log.info("mmsiAnomaly: " + start + " ~ " + end);

        Dataset<Row> df = loadParquet()
                .filter(col("base_date_time").between(start, end))
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
        return json;
    }
}
