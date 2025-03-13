package org.wcg.gmall.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        // TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 2.设置并行度
        env.setParallelism(1);

        // enable checkpoint
        env.enableCheckpointing(3000);
        // TODO 3.读取mysql binlog 数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .databaseList("gmall2024_config") // set captured database
                .tableList("gmall2024_config.t_user") // set captured table
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial() )
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        //{"before":null,"after":
        // {"id":3,"user_name":"gejiahui","age":28},
        // "source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source",
        //           "ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user",
        //           "server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
        //           "op":"r","ts_ms":1734233553476,"transaction":null}

        //{"before":null,"after":
        // {"id":4,"user_name":"jiangmin","age":62},
        // source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1734233668000,
        //          "snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,
        //          "gtid":null,"file":"mysql-bin.000003","pos":1353,"row":0,"thread":13,"query":null},
        //          "op":"c","ts_ms":1734233668570,"transaction":null}

        //{"before":{"id":4,"user_name":"jiangmin","age":62},
        // "after":{"id":4,"user_name":"jiangmin","age":32},
        // "source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1734233752000,
        //           "snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,
        //           "gtid":null,"file":"mysql-bin.000003","pos":1683,"row":0,"thread":13,"query":null},
        //           "op":"u","ts_ms":1734233752588,"transaction":null}

        //{"before":{"id":4,"user_name":"jiangmin","age":32},
        // "after":null,
        // "source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1734233859000,
        //           "snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,
        //           "file":"mysql-bin.000003","pos":2023,"row":0,"thread":13,"query":null},
        //           "op":"d","ts_ms":1734233859487,"transaction":null}
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print(); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}