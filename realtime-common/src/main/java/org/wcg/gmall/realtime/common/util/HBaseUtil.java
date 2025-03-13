package org.wcg.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;

/**
 * @author wangchengguang
 */
@Slf4j
public class HBaseUtil {
    // 获取HBase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop100,hadoop101,hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(conf);
    }

    // 关闭HBase链接
    public static void closeHBaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    // 创建表
    public static void createHBaseTable(Connection hbaseConn, String namespace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("至少指定一个列簇");
            return;
        }
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println("表空间["+ namespace + "]下的表[" + tableName + "]已存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family :
                    families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表空间["+ namespace + "]下的表[" + tableName + "]创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 删表
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("表空间["+ namespace + "]下的表[" + tableName + "]不存在");
                return;
            }
            admin.deleteTable(tableNameObj);
            System.out.println("表空间["+ namespace + "]下的表[" + tableName + "]删除成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param hbaseConn 连接对象
     * @param namespace  表空间
     * @param tableName  表名
     * @param rowKey  行键
     * @param family  列簇
     * @param jsonObj  要pu的数据
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间["+ namespace + "]下的表[" + tableName + "]中put数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("向表空间["+ namespace + "]下的表[" + tableName + "]中delete数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
