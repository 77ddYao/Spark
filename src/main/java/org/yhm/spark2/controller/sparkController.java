package org.yhm.spark2.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.yhm.spark2.service.SparkService;

@RestController
@RequestMapping("/spark")
public class sparkController {

    @Autowired
    SparkService sparkService;

    // 原始查询：按时间窗口返回船舶位置
    @RequestMapping("/query/{start}/{end}")
    public Object queryData(@PathVariable("start") String start, @PathVariable("end") String end) {
        return sparkService.queryData(start, end);
    }

    // 快照查询：查询某一时刻所有船舶数据
    @RequestMapping("/snapshot/{timestamp}")
    public Object querySnapshot(@PathVariable("timestamp") String timestamp) {
        return sparkService.querySnapshot(timestamp);
    }

    // 1. 瞬时近距离告警：同一时刻两船距离 < 500m
    @RequestMapping("/close-calls/{start}/{end}")
    public Object closeCalls(@PathVariable("start") String start, @PathVariable("end") String end) {
        return sparkService.closeCalls(start, end);
    }

    // 2. 航速突变检测：SOG 短时剧烈变化
    @RequestMapping("/speed-anomaly/{start}/{end}")
    public Object speedAnomaly(@PathVariable("start") String start, @PathVariable("end") String end) {
        return sparkService.speedAnomaly(start, end);
    }

    // 3. 区域热力图：按经纬网格统计船舶密度
    @RequestMapping("/heatmap/{start}/{end}")
    public Object heatmap(@PathVariable("start") String start, @PathVariable("end") String end) {
        return sparkService.heatmap(start, end);
    }

    // 4. MMSI 时空异常：同一 MMSI 不可能的位置跳变
    @RequestMapping("/mmsi-anomaly/{start}/{end}")
    public Object mmsiAnomaly(@PathVariable("start") String start, @PathVariable("end") String end) {
        return sparkService.mmsiAnomaly(start, end);
    }

    // 5. 航迹生成：指定 MMSI 船只按时间戳的线性轨迹
    @RequestMapping("/track/{mmsi}/{start}/{end}")
    public Object vesselTrack(@PathVariable("mmsi") String mmsi,
                               @PathVariable("start") String start,
                               @PathVariable("end") String end) {
        return sparkService.vesselTrack(mmsi, start, end);
    }
}
