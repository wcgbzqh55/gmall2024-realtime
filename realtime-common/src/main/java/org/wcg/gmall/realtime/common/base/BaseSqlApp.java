package org.wcg.gmall.realtime.common.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.wcg.gmall.realtime.common.constant.Constant;
import org.wcg.gmall.realtime.common.util.SqlUtil;

/**
 * @author wangchengguang
 * @date 2025/3/16 23:32:51
 * @description
 * FlinkSQL的基类
 */
public abstract class BaseSqlApp {
    public void start(int port, int parallelism, String ck) {
        // TODO 1.基本环境准备
        // TODO 2.检查点相关的设置
        // 1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 指定并行度
        env.setParallelism(parallelism);
        // 1.3 指定执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2.1 开启检查点 检查点屏障的对齐方式
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        // 2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(6000L);
//        // 2.3 设置状态取消后，检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 2.4 设置两个检查点之间最小的时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        // 2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
//        // 2.6 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop100:8020/ck" + ck);
//        // 2.7 指定操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME", "wcg");
        // TODO 3.业务处理逻辑
        handle(tableEnv);
    }

    /**
     * 实现业务逻辑
     * @param tableEnv
     */
    public abstract void handle(StreamTableEnvironment tableEnv);

    public void readOdsDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` MAP<STRING,STRING>,\n" +
                "  `old` MAP<STRING,STRING>,\n" +
                "  `proc_time` as proctime()\n" +
                ") " + SqlUtil.getKafkaDDL(Constant.TOPIC_DB, groupId));
    }

    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SqlUtil.getHBaseDDL("dim_base_dic"));
//        tableEnv.executeSql("select * from base_dic").print();
    }
}
