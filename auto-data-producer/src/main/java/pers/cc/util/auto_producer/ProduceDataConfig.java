package pers.cc.util.auto_producer;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
@Data
public class ProduceDataConfig {
    // 数据库名称
    @JsonProperty("db_name")
    private String dbName;

    // 表名称
    @JsonProperty("table_name")
    private String tableName;

    // 最多产生的数据量
    @JsonProperty("produce_num")
    private int produceNum;

    // 批处理大小
    @JsonProperty("batch_size")
    private int batchSize;

    // 行信息列表
    @JsonProperty("col_rules")
    private List<ColRule> colRules;

    @Data
    public static class ColRule {
        // 列名称
        @JsonProperty("col_name")
        private String colName;

        // 数据类型（String, DateTime, Int, Float, Decimal）
        @JsonProperty("data_type")
        private String dataType;

        // 是否包含null值
        @JsonProperty("has_null")
        private boolean hasNull;

        // 随机值部分的近似长度大小，会尽量补全到该大小(DateTime、Decimal无法指定长度)
        @JsonProperty("near_len")
        private int nearLen;

        // 随机值范围(在多少范围内随机，如2，则最多生成2种随机值，自增时无法指定范围)
        @JsonProperty("random_range")
        private int randomRange;

        // 是否自增（DateTime无法自增）
        @JsonProperty("auto_inc")
        private boolean autoInc;

        // 字符串类型数据增加固定前缀字符串
        @JsonProperty("string_pre_regular")
        private String stringPreRegular;

        // DateTime类型数据是否使用最新时间
        @JsonProperty("data_time_latest")
        private boolean dataTimeLatest;
    }
}
