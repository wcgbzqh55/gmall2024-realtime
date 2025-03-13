package org.wcg.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.wcg.gmall.realtime.common.bean.TableProcessDim;
import org.wcg.gmall.realtime.common.constant.Constant;
import org.wcg.gmall.realtime.common.util.JdbcUtil;

import java.sql.*;
import java.util.*;

/**
 * @author wangchengguang
 * @date 2025/1/9 15:03:51
 * @description
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 将配置表的配置信息预加载到程序configMap中
        String sql = "select * from gmall2024_config.table_process_dim";
        Connection conn = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(conn, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeMysqlConnection(conn);
    }

    // processElement:处理主流业务数据              根据维度表名到广播状态中读取配置信息，判断是否为维度
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 获取处理的数据的表名
        String table = jsonObject.getString("table");
        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        TableProcessDim tableProcessDim = null;
        if ((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim = configMap.get(table)) != null) {
            // 如过非空，是维度数据
            // 将维度数据继续向下游传递 只需要传递data属性即可
            JSONObject dataObj = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcessDim.getSinkColumns();
            // 在想下游传递data前，将维度表操作类型添加到data中
            String type = jsonObject.getString("type");
            deleteNotNeedColumns(dataObj, sinkColumns);
            dataObj.put("op_type", type);
            collector.collect(Tuple2.of(dataObj, tableProcessDim));
        }
    }

    // processBroadcastElement:处理广播流配置信息   将配置数据放到广播状态中,或者删除对应的配置  k：维度表名  v：一个配置对象
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String op = tp.getOp();
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String sourceTable = tp.getSourceTable();
        if ("d".equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable, tp);
        }
    }

    public static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
//        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
//        for (; it.hasNext();) {
//            Map.Entry<String, Object> entry = it.next();
//            if (!columnList.contains(entry.getKey())) {
//                it.remove();
//            }
//        }
    }
}

