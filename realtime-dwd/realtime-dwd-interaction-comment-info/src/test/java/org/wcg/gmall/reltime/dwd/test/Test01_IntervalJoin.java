package org.wcg.gmall.reltime.dwd.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * @author wangchengguang
 * @date 2025/3/15 20:19:55
 * @description
 */
public class Test01_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("localhost", 8888)
                .map(
                        new MapFunction<String, Emp>() {
                            @Override
                            public Emp map(String lineStr) throws Exception {
                                String[] stringArr = lineStr.split(" ");
                                return new Emp(new Integer(stringArr[0]), new String(stringArr[1]), new Integer(stringArr[2]), new Long(stringArr[3]));
                            }
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Emp>() {
                                    @Override
                                    public long extractTimestamp(Emp emp, long recordTimestamp) {
                                        return emp.getTs();
                                    }
                                })
                );
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("localhost", 8889)
                .map(
                        new MapFunction<String, Dept>() {
                            @Override
                            public Dept map(String lineStr) throws Exception {
                                String[] stringArr = lineStr.split(" ");
                                return new Dept(new Integer(stringArr[0]), new String(stringArr[1]), new Long(stringArr[2]));
                            }
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Dept>() {
                                    @Override
                                    public long extractTimestamp(Dept dept, long recordTimestamp) {
                                        return dept.getTs();
                                    }
                                })
                );
        empDS.print();
        deptDS.print();
        // 底层 connect + 状态
        // 判断是否迟到
        // 用当前元素添加到缓存中
        // 用当前元素和另外一条流中缓存数据做关联
        // 清缓存
        empDS
                .keyBy(Emp::getDeptId)
                .intervalJoin(deptDS.keyBy(Dept::getDeptId))
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                            @Override
                            public void processElement(Emp emp, Dept dept, ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context context, Collector<Tuple2<Emp, Dept>> collector) throws Exception {
                                collector.collect(Tuple2.of(emp, dept));
                            }
                        }
                ).print();
        env.execute();
    }
}
