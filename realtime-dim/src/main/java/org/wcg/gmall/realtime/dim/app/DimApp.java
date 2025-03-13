package org.wcg.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.wcg.gmall.realtime.common.base.BaseApp;
import org.wcg.gmall.realtime.common.bean.TableProcessDim;
import org.wcg.gmall.realtime.common.constant.Constant;
import org.wcg.gmall.realtime.common.util.FlinkSourceUtil;
import org.wcg.gmall.realtime.common.util.HBaseUtil;
import org.wcg.gmall.realtime.dim.function.HBaseSinkFunction;
import org.wcg.gmall.realtime.dim.function.TableProcessFunction;

/**
 * @author wangchengguang
 * Dim层逻辑开发
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 对业务流中数据进行类型转换并进行简单ETL   jsonStr --> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);
        // TODO 使用FlinkCDC读取配置表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        // TODO 根据配置流中的配置信息到HBase中进行建表或者删表操作
        tpDS = manageHBaseTable(tpDS);

        // TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpDS);
        // TODO 将维度数据同步到HBase表中
        dimDS.print();
        dimDS.addSink(new HBaseSinkFunction());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        // 将配置流中的配置信息进行广播 -- Broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // 将主流业务数据和广播配置信息进行关联 -- connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // 处理关联之后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(new TableProcessFunction(mapStateDescriptor));
        return dimDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> manageHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {
                // 获取对配置表操作的类型
                String op = tp.getOp();
                String sinkTable = tp.getSinkTable();
                String[] families = tp.getSinkFamily().split(",");
                if ("d".equals(op)) {
                    // 从配置表中删除了一条数据，将hbase中对应的表删除
                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                } else if ("r".equals(op) || "c".equals(op)) {
                    // 从配置表中读取了一条数据或者向配置表中添加了一条数据，在hbase中建表
                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, families);
                } else {
                    // 对配置表中的配置信息进行了修改，先从hbase中将对应的表删除掉，再重新创建
                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, families);
                }
                return tp;
            }
        });
        tpDS.print();
        return tpDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // 5.1 创建MysqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "table_process_dim");
        // 5.2 读取数据，封装为流
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        // TODO 6.对配置流中的数据类型进行转换   jsonStr --> 实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                // 为了处理方便，先将jsonStr转化成json对象
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    // 对配置表进行了一次删除操作，从before属性中获取删除前的配置信息
                    tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                } else {
                    // 对配置表进行了读取、添加、修改操作，从after属性中获取最新的配置数据
                    tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);
        // tpDS.print();
        return tpDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String db = jsonObj.getString("database");
                String type = jsonObj.getString("type");
                String data = jsonObj.getString("data");

                if (Constant.MYSQL_DATABASE.equals(db)
                        && ("insert".equals(type)
                        || "update".equals(type)
                        || "delete".equals(type)
                        || "bootstrap-insert".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
                    collector.collect(jsonObj);
                }
            }
        });
        // jsonObjDS.print();
        return jsonObjDS;
    }
}
