package org.wcg.gmall.realtime.common.util;

import org.wcg.gmall.realtime.common.constant.Constant;

/**
 * @author wangchengguang
 * @date 2025/3/16 23:16:20
 * @description
 * FlinkSQL操作的工具类
 */
public class SqlUtil {
    public static String getKafkaDDL(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                        "  'topic' = '" + topic + "',\n" +
                        "  'properties.bootstrap.servers' = 'hadoop100:9092,hadoop101:9092,hadoop102:9092',\n" +
                        "  'properties.group.id' = '" + groupId + "',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")\n";
    }

    public static String getHBaseDDL(String tableName) {
        return "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + Constant.HBASE_NAMESPACE + ":" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'hadoop100,hadoop101,hadoop102:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ");";
    }

    public static String getUpsertKafkaDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");";
    }
}
