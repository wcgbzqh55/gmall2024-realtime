package org.wcg.gmall.reltime.dwd.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wangchengguang
 * @date 2025/3/16 13:55:59
 * @description
 * 通过当前Demo模拟评论事实表的写入过程
 */
public class Test03_Demo {
    public static void main(String[] args) {
        // TODO 1.基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 指定并行度
        env.setParallelism(1);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2.设置Checkpoint策略（略）
        // TODO 3.从kafka的first中读取员工表的数据
        tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  `empId` STRING,\n" +
                "  `empName` STRING,\n" +
                "  `deptId` STRING,\n" +
                "  `procTime` as PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop100:9092,hadoop101:9092,hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from emp").print();
        // TODO 4.从hbase中读取出部门维度信息表
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                " deptId string,\n" +
                " info ROW<deptName STRING>,\n" +
                " PRIMARY KEY (deptId) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 't_dept',\n" +
                " 'zookeeper.quorum' = 'hadoop100:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ");");
//        tableEnv.executeSql("select * from dept").print();
        // TODO 5.关联员工和部门
        // 如果使用lookupjoin，底层实现原理和普通的内外连接完全不同，没有为参与链接的两张表维护状态
        // 它是左表进行驱动的，每有一条左表数据到来，都会申请查询右表
        Table joinedTable = tableEnv.sqlQuery("SELECT e.empId, e.empName, d.deptId, d.deptName\n" +
                "FROM emp AS e\n" +
                "  JOIN dept FOR SYSTEM_TIME AS OF e.procTime AS d\n" +
                "    ON e.deptId = d.deptId;");
//        joinedTable.execute().print();
        // TODO 6.将关联的结果写入kafka
        // 6.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empId STRING,\n" +
                "  empName STRING,\n" +
                "  deptId STRING,\n" +
                "  deptName STRING,\n" +
                "  PRIMARY KEY (empId) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'test-sql-join',\n" +
                "  'properties.bootstrap.servers' = 'hadoop100:9092,hadoop101:9092,hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        // 6.2 写入
        joinedTable.executeInsert("emp_dept");
    }
}
