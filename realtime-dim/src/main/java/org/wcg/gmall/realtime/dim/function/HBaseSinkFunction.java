package org.wcg.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.wcg.gmall.realtime.common.bean.TableProcessDim;
import org.wcg.gmall.realtime.common.constant.Constant;
import org.wcg.gmall.realtime.common.util.HBaseUtil;

/**
 * @author wangchengguang
 * @date 2025/1/9 15:25:52
 * @description
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
        JSONObject jsonObj = tup.f0;
        TableProcessDim tableProcessDim = tup.f1;
        String opType = jsonObj.getString("op_type");
        jsonObj.remove("op_type");
        String sinkTable = tableProcessDim.getSinkTable();
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
        if ("delete".equals(opType)) {
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
        } else {
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
        }
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
    }
}
