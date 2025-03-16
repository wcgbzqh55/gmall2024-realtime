package org.wcg.gmall.reltime.dwd.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author wangchengguang
 * @date 2025/3/16 10:49:49
 * @description
 * Flink SQL 双流Join
 *                     左表                  右表
 * 内连接        OnCreateAndWrite       OnCreateAndWrite
 * 左外连接      OnReadAndWrite         OnCreateAndWrite
 * 右外连接      OnCreateAndWrite       OnReadAndWrite
 * 全外连接      OnReadAndWrite         OnReadAndWrite
 */
public class Test02_SQL_JOIN {
    public static void main(String[] args) throws Exception {
        // TODO 1.基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 指定并行度
        env.setParallelism(1);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        // TODO 2.检查点相关的设置（略）
        // TODO 3.从指定的网络端口读取员工数据 并转换为动态表
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("localhost", 8888)
                .map(new MapFunction<String, Emp>() {
                    @Override
                    public Emp map(String lineStr) throws Exception {
                        String[] stringArr = lineStr.split(" ");
                        return new Emp(new Integer(stringArr[0]), new String(stringArr[1]), new Integer(stringArr[2]), new Long(stringArr[3]));
                    }
                });
        tableEnv.createTemporaryView("emp", empDS);
        // TODO 4.从指定的网络端口读取部门数据 并转换为动态表
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("localhost", 8889)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String lineStr) throws Exception {
                        String[] stringArr = lineStr.split(" ");
                        return new Dept(new Integer(stringArr[0]), new String(stringArr[1]), new Long(stringArr[2]));
                    }
                });
        tableEnv.createTemporaryView("dept", deptDS);
        // TODO 5.内连接
        // 如果使用普通的内外连接，底层会为参与链接的两张表各自维护一个状态，用与存放两张表的数据，默认情况下，状态永远不会失效
        // 生产环境需要设置状态保留时间
//        tableEnv.executeSql("select e.empId, e.empName, e.deptId, d.deptId, d.deptName from emp e join dept d on e.deptId = d.deptId").print();
//        tableEnv.sqlQuery("select e.empId, e.empName, e.deptId, d.deptId, d.deptName from emp e join dept d on e.deptId = d.deptId");
        // TODO 6.左外连接

        // 这样的动态表的转换，我们称之为回撤流
//        tableEnv.executeSql("select e.empId, e.empName, d.deptId, d.deptName from emp e left join dept d on e.deptId = d.deptId").print();
        // TODO 7.右外连接
//        tableEnv.executeSql("select e.empId, e.empName, d.deptId, d.deptName from emp e right join dept d on e.deptId = d.deptId").print();
        // TODO 8.全外连接
//        tableEnv.executeSql("select e.empId, e.empName, d.deptId, d.deptName from emp e full join dept d on e.deptId = d.deptId").print();


        // TODO 9.将连接的结果写到kafka主题
        // 注意：kafka连接器不支持写入的时候包含update或者delete操作，需要用upsert-kafka连接器
        // 9.1 创建一个动态表和要写入的kafka主题进行映射
        // 注意：如果左表数据先到，右表数据后到
        // 左表  null  标记为+I
        // 左表  null  标记为-D
        // 左表  右表   标记为+I
        // 当从kafka主题中读取数据的时候，存在空消息，如果使用的FlinkSQL的方式读取，会自动的将空消息过滤掉
        // 如果使用的是FlinkAPI的方式读取的话，默认的SimpleStringSchema是处理不了空消息的，需要自定义反序列化器
        // 除了空消息外吗，在DWS进行汇总操作的时候，还需要进行去重处理
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empId INTEGER,\n" +
                "  empName STRING,\n" +
                "  deptId INTEGER,\n" +
                "  deptName STRING,\n" +
                "  PRIMARY KEY (empId) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'test-sql-join',\n" +
                "  'properties.bootstrap.servers' = 'hadoop100:9092,hadoop101:9092,hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        tableEnv.executeSql("insert into emp_dept select e.empId, e.empName, d.deptId, d.deptName from emp e left join dept d on e.deptId = d.deptId");

    }
}
