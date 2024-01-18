package pers.cc.util.auto_producer;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * @author Chen768959
 * @date 2024/1/15
 */
@Data
public class ArgsConfig {
    @JsonProperty("concurrent_num")
    private int concurrentNum;

    @JsonProperty("send_type")
    private String sendType;

    @JsonProperty("dataSource_config")
    private DataSourceConfig dataSourceConfig;

    @JsonProperty("produceData_config")
    private ProduceDataConfig produceDataConfig;
}
