package org.wcg.gmall.realtime.dwd.log.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.wcg.gmall.realtime.common.base.BaseApp;
import org.wcg.gmall.realtime.common.constant.Constant;
import org.wcg.gmall.realtime.common.util.DateFormatUtil;
import org.wcg.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangchengguang
 * @date 2025/3/14 17:49:54
 * @description 日志分流
 *              需要启动的进程：zk、kafka、flume
 * KafkaSource: 从kafka主题中读取数据
 *              通过手动维护偏移量，保证消费的精准一次
 *              消费端需要设置消费的隔离级别为读已提交，.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
 * KafkaSink: 向kafka主题中写入数据,也可以保证写入的精准一次，需要做如下操作：
 *            开启检查点
 *            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *            .setTransactionalIdPrefix("dwd_base_log_")
 *            .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
 *
 */
public class DwdBaseLog extends BaseApp {

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO  对流中数据类型进行转换，并做简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);
        // TODO 对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(jsonObjDS);
        // TODO 分流 错误日志--错误侧输出流 启动日志--启动侧输出流 曝光日志--曝光侧输出流 动作日志--动作侧输出流 页面日志--主流
        Map<String, DataStream<String>> streamMap = splitDataStream(fixedDS);
        // TODO 将不同的流中的数据写入不同的kafka主题中
        writeToKafka(streamMap);

    }

    private void writeToKafka(Map<String, DataStream<String>> streamMap) {
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream<String>> splitDataStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        // 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<>("errTag", TypeInformation.of(String.class));
        OutputTag<String> startTag = new OutputTag<>("startTag", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("displayTag", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<>("actionTag", TypeInformation.of(String.class));
        // 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> collector) throws Exception {
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            // 启动日志
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            // 页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            // 曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                // 遍历当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }
                            // 动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                // 遍历出每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            // 页面日志写入主流
                            collector.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面日志：");
        errDS.print("错误日志：");
        startDS.print("启动日志：");
        displayDS.print("曝光日志：");
        actionDS.print("动作日志：");
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(PAGE, pageDS);
        streamMap.put(ERR, errDS);
        streamMap.put(START, startDS);
        streamMap.put(DISPLAY, displayDS);
        streamMap.put(ACTION, actionDS);
        return streamMap;
    }

    private SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        // 按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 使用Flink的状态编程进行修改
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
//                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.fromDuration(Duration.ofSeconds(10)))
//                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                                .build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        // 从状态中获取首次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        // 获取当前访问日期
                        Long ts = value.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            // 如果 is_new 的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    value.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterday);
                            }
                        }
                        return value;
                    }
                }
        );
//        fixedDS.print();
        return fixedDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        // 定义侧输出流标签
        // ETL
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag", TypeInformation.of(String.class));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            // 如果转换过程中没有发生异常，说明是标准json，是正常数据，将数据传递到下游
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            // 如果转换过程中出现异常，说明是非标准json，脏数据，输出到侧输出流
                            context.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
//        jsonObjDS.print("标准的json：");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//        dirtyDS.print("脏数据：");
        // 将侧输出流中的脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }


}
