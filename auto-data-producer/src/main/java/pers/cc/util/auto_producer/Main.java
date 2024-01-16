package pers.cc.util.auto_producer;

import pers.cc.util.auto_producer.context.Context;
import pers.cc.util.auto_producer.producer.DataProducer;
import pers.cc.util.auto_producer.producer.DataProducerImpl;
import pers.cc.util.auto_producer.writer.JdbcBatchRecordWriter;
import pers.cc.util.auto_producer.writer.RecordWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Chen768959
 * @date 2024/1/10
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static Context context = Context.getInstance();

    /**
     * args:
     * {
     *     "concurrent_num":"",     // 并发数
     *     "dataSource_config":{
     *         "engine_type":"",    // 字符串，"doris","clickhouse"
     *         "user_name":"",      // 字符串，用户名
     *         "password":"",       // 字符串，密码
     *         "jdbc_url_list":[    // 可配置多个并自动负载均衡发送
     *             ""               // 字符串，该数据源的jdbc_url字符串
     *         ]
     *     },
     *     "produceData_config":{
     *         "db_name":"",        // 字符串，写入库名
     *         "table_name":"",     // 字符串，写入表名
     *         "produce_num":"",    // 数值，最多产生多少数据
     *         "batch_size":"",     // 数值，batch_size
     *         "col_rules":[
     *             {
     *                 "col_name":"",            // 字符串，列名
     *                 "data_type":"",           // 字符串，String,DateTime,Int,Float,Decimal
     *                 "has_null":"",            // boolean，是否包含null值
     *                 "near_len":"",            // 数值，随机值部分的近似长度大小，会尽量补全到该大小(DateTime、Decimal无法指定长度)
     *                 "random_range":"",        // 数值，在多少范围内随机，如2，则最多生成2种随机值(自增条件下无法设置)
     *                 "auto_inc":"",            // boolean，是否自增（DateTime无法自增）
     *                 "string_pre_regular":"",  // 字符串，字符串类型数据增加固定前缀字符串
     *                 "data_time_latest":""     // boolean，DateTime类型数据是否使用最新时间
     *             }
     *         ]
     *     }
     * }
     */
    public static void main(String[] args) throws JsonProcessingException, SQLException, ClassNotFoundException, InterruptedException {
        context.init(args);

        // 当前写入总数
        AtomicInteger curInsertSum = new AtomicInteger(0);

        // 并发写入
        ExecRes[] execResArr = new ExecRes[context.getConcurrentNum()];
        CountDownLatch latch = new CountDownLatch(context.getConcurrentNum());
        executorExec(execResArr, curInsertSum, latch);

        // 等待写入结束，输出报告
        latch.await();
        for (ExecRes execRes : execResArr) {
            System.out.println(execRes.toReport());
        }
        System.out.println(getFinalRes(execResArr));

        context.finish();
    }

    private static String getFinalRes(ExecRes[] execResArr) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        long finalStartTime = execResArr[0].getStartTimestamp();
        long finalEndTime = 0;
        int finalInsertSum = 0;
        long finalFlushConsumeTime = 0;
        for (ExecRes execRes : execResArr) {
            finalInsertSum+=execRes.getInsertSum();
            finalFlushConsumeTime += execRes.getFlushConsumeTime();
            if (execRes.getStartTimestamp() < finalStartTime){
                finalStartTime = execRes.getStartTimestamp();
            }
            if (execRes.getEndTimestamp() > finalEndTime){
                finalEndTime = execRes.getEndTimestamp();
            }
        }
        long avgFlushConsumeTime = (finalFlushConsumeTime / 1000) / execResArr.length;
        long finalAvgSpeed = avgFlushConsumeTime == 0 ? finalInsertSum : finalInsertSum / avgFlushConsumeTime;

        return "Final: " +
                "成功写入条数("+finalInsertSum+")，" +
                "各线程平均耗时("+avgFlushConsumeTime+")秒，" +
                "平均写入速度：("+finalAvgSpeed+")行/秒，" +
                "启动时间("+sdf.format(finalStartTime)+")，" +
                "结束时间("+sdf.format(finalEndTime)+");";

    }

    private static void executorExec(ExecRes[] execResArr, AtomicInteger curInsertSum, CountDownLatch latch) {
        for (int i = 0; i < context.getConcurrentNum(); i++) {
            Connection connection = context.getConnectionList().get(i % context.getConnectionList().size());
            int finalI = i;
            context.getInsertThreadPool().submit(() -> {
                ExecRes execRes = new ExecRes();
                execResArr[finalI] = execRes;

                int insertSum = 0;
                int batches = 0;
                long startTime = System.currentTimeMillis();
                long consumeTime = 0;
                RecordWriter recordWriter = null;
                try {
                    int batchSize = context.getProduceDataRule().getBatchSize();
                    DataProducer dataProducer = new DataProducerImpl(context.getProduceDataRule());
                    recordWriter = new JdbcBatchRecordWriter(
                            context.getProduceDataRule().getDbName(),
                            context.getProduceDataRule().getTableName(),
                            context.getProduceDataRule().getColRules().stream().map(ProduceDataConfig.ColRule::getColName).collect(Collectors.toList()),
                            dataProducer.dataTypeList(),
                            connection);

                    boolean finish = false;
                    while (true){
                        try {
                            synchronized (curInsertSum){
                                int newInsertSum = curInsertSum.addAndGet(batchSize);
                                if (newInsertSum > context.getProduceDataRule().getProduceNum()){
                                    batchSize = Math.max(batchSize - (newInsertSum - context.getProduceDataRule().getProduceNum()), 0);
                                    finish = true;
                                }
                            }

                            for (int row = 0; row < batchSize; row++) {
                                List<Object> data = dataProducer.produceSingleData();
                                recordWriter.write(data);
                            }

                            long startFlush = System.currentTimeMillis();
                            recordWriter.flush();
                            long endFlush = System.currentTimeMillis();

                            insertSum += batchSize;

                            consumeTime += (endFlush - startFlush);
                            if (batchSize > 0){
                                logger.info("Thread({}), id:{}, 提交batchSize:{}, 耗时:{}s，当前已提交:{}",finalI, Thread.currentThread().getId(), batchSize, (endFlush-startFlush)/1000, insertSum);
                            }

                            if (finish){
                                break;
                            }

                            batches++;
                        } catch (SQLException e){
                            logger.error("线程("+finalI+") SQLException",e);
                        }
                    }
                } catch (Exception e){
                    logger.error("线程("+finalI+") error",e);
                    execRes.setSuccess(false);
                    execRes.setErrMsg(e.toString());
                }

                // 执行结果
                execRes.setThreadId(finalI);
                execRes.setInsertSum(insertSum);
                execRes.setInsertBatches(batches);
                execRes.setStartTimestamp(startTime);
                execRes.setEndTimestamp(System.currentTimeMillis());
                execRes.setFlushConsumeTime(consumeTime);

                if (recordWriter!=null){
                    try {
                        recordWriter.close();
                    } catch (SQLException e) {
                        logger.error("recordWriter close error",e);
                    }
                }
                latch.countDown();
            });
        }
    }

    @Data
    static class ExecRes{
        int threadId;

        // 总成功插入条数
        int insertSum;

        // 批次数
        int insertBatches;

        // 是否成功
        boolean success = true;

        // 报错信息
        String errMsg = "";

        // 启动时间
        long startTimestamp;

        // 结束时间
        long endTimestamp;

        // 写入总耗时
        long flushConsumeTime;

        public String toReport(){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            if (success){
                String startStr = sdf.format(startTimestamp);
                String endStr = sdf.format(endTimestamp);
                long totalTime = (endTimestamp - startTimestamp) / 1000;
                long flushConsumeTimeS = (flushConsumeTime / 1000);
                long avgSpeed = flushConsumeTimeS == 0 ? insertSum : insertSum/flushConsumeTimeS;

                return "threadId("+threadId+") 执行成功，" +
                        "成功写入条数("+insertSum+")，" +
                        "成功写入批次数("+insertBatches+")，" +
                        "启动时间("+startStr+")，" +
                        "结束时间("+endStr+")，" +
                        "写入总耗时("+flushConsumeTimeS+")秒，" +
                        "总耗时("+totalTime+")秒，" +
                        "平均写入速度：("+avgSpeed+")行/秒;";
            }else {
                return "threadId("+threadId+")执行失败，" +
                        "成功写入条数("+insertSum+")，" +
                        "成功写入批次数("+insertBatches+")，" +
                        "报错信息：" + errMsg;
            }
        }

        public long getAvgSpeed(){
            long flushConsumeTimeS = (flushConsumeTime / 1000);
            return flushConsumeTimeS == 0 ? insertSum : insertSum/flushConsumeTimeS;
        }
    }
}