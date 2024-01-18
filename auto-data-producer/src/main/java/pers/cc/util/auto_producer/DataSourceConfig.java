package pers.cc.util.auto_producer;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
@Data
public class DataSourceConfig {

    @JsonProperty("engine_type")
    private String engineType;

    @JsonProperty("user_name")
    private String userName;

    @JsonProperty("password")
    private String password;

    @JsonProperty("host_list")
    private List<HostInfo> hostList;

    @Data
    public static class HostInfo {
        @JsonProperty("host")
        private String host;

        @JsonProperty("jdbc_port")
        private int jdbcPort;

        @JsonProperty("http_port")
        private int httpPort;
    }
}
