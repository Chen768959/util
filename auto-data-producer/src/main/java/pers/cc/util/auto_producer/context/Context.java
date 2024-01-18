package pers.cc.util.auto_producer.context;

import pers.cc.util.auto_producer.ArgsConfig;
import pers.cc.util.auto_producer.DataSourceConfig;
import pers.cc.util.auto_producer.ProduceDataConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Chen768959
 * @date 2024/1/15
 */
public class Context {
    private static Context instance = null;
    private static final ObjectMapper mapper = new ObjectMapper();
    private boolean init;
    private Timer timestampTimer;

    @Getter
    private List<Connection> connectionList = new ArrayList<>();
    @Getter
    private ProduceDataConfig produceDataRule;
    @Getter
    private ExecutorService insertThreadPool;
    @Getter
    private volatile Timestamp currentTimestamp;
    @Getter
    private int concurrentNum;
    @Getter
    private SendType sendType;
    @Getter
    private List<DataSourceConfig.HostInfo> hostInfoList;
    @Getter
    private DataSourceConfig dataSourceConfig;
    @Getter
    private EngineType engineType;

    private Context() {}

    public static Context getInstance() {
        if (instance == null) {
            instance = new Context();
        }
        return instance;
    }

    public void init(String[] args) throws JsonProcessingException, SQLException, ClassNotFoundException {
        if (args.length != 1){
            throw new IllegalArgumentException("args num error, need config json");
        }

        ArgsConfig argsConfig = mapper.readValue(args[0], ArgsConfig.class);

        // 并发数
        this.concurrentNum = argsConfig.getConcurrentNum();

        // 发送方式
        switch (argsConfig.getSendType()){
            case "jdbc":
                this.sendType = SendType.JDBC;
                break;
            case "http_stream":
                this.sendType = SendType.HTTP_STREAM;
                break;
            default:
                throw new IllegalArgumentException("arg SendType error, support jdbc,http_stream");
        }

        switch (argsConfig.getDataSourceConfig().getEngineType()){
            case "clickhouse":
                this.engineType = EngineType.CLICKHOUSE;
                break;
            case "doris":
                this.engineType = EngineType.DORIS;
                break;
            default:
                throw new IllegalArgumentException("arg EngineType error, support clickhouse,doris");
        }

        // ip信息
        this.hostInfoList = argsConfig.getDataSourceConfig().getHostList();

        // 获取连接信息
        if (sendType == SendType.JDBC){
            this.connectionList = createDataConnectList(this.engineType, this.concurrentNum, argsConfig.getDataSourceConfig());
        }

        // 获取生产数据的规则
        this.produceDataRule = argsConfig.getProduceDataConfig();

        this.dataSourceConfig = argsConfig.getDataSourceConfig();

        // 生成insert线程池
        this.insertThreadPool = Executors.newFixedThreadPool(concurrentNum);

        // 定时获取当前时间戳
        startCurrentTimestamp();

        init = true;
    }

    public void finish() throws SQLException {
        if (init){
            for (Connection connection : this.connectionList) {
                connection.close();
            }
            this.insertThreadPool.shutdownNow();
            this.timestampTimer.cancel();
        }
    }

    private void startCurrentTimestamp() {
        this.timestampTimer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                currentTimestamp = new Timestamp(System.currentTimeMillis());
            }
        };
        this.timestampTimer.scheduleAtFixedRate(task, 0, 1000);
    }

    private List<Connection> createDataConnectList(EngineType engineType, int concurrentNum, DataSourceConfig dataSourceJson) throws ClassNotFoundException, SQLException {
        List<String> jdbcUrlList;
        switch (engineType){
            case DORIS:
                jdbcUrlList = dataSourceJson.getHostList().stream()
                        .filter(hostInfo -> hostInfo.getJdbcPort()!=0)
                        .map(hostInfo->
                                "jdbc:mysql://"+hostInfo.getHost()+":"+hostInfo.getJdbcPort()+"?rewriteBatchedStatements=true&&socket_timeout=600000")
                        .collect(Collectors.toList());
                Class.forName("com.mysql.jdbc.Driver");
                break;
            case CLICKHOUSE:
                jdbcUrlList = dataSourceJson.getHostList().stream()
                        .filter(hostInfo -> hostInfo.getJdbcPort()!=0)
                        .map(hostInfo->
                                "jdbc:clickhouse://"+hostInfo.getHost()+":"+hostInfo.getJdbcPort()+"?socket_timeout=600000")
                        .collect(Collectors.toList());
                Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
                break;
            default:
                throw new IllegalArgumentException("dataSource engineType error, only support[doris,clickhouse]");
        }

        List<Connection> connectionList = new ArrayList<>();

        for (int i=0, j = 0; i < concurrentNum; i++,j++) {
            if (j == jdbcUrlList.size()){
                j=0;
            }
            String jdbcUrl = jdbcUrlList.get(j);
            connectionList.add(DriverManager.getConnection(jdbcUrl, dataSourceJson.getUserName(), dataSourceJson.getPassword()));
        }

        return connectionList;
    }
}
