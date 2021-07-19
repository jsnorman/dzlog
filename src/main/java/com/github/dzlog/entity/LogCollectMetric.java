package com.github.dzlog.entity;

import lombok.Data;

/**
 * Created by admin on 2018/3/8.
 */
@Data
public class LogCollectMetric {

    private Long id;

    private String nodeIp;

    private String code;

    private String collectDate;

    private String hourPeriod;

    private String minutePeriod;

    private long minuteCount;

    private long minuteBytes;
}
