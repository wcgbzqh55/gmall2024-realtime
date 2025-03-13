package org.wcg.gmall.realtime.common.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.wcg.gmall.realtime.common.constant.Constant;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangchengguang
 * @date 2025/1/9 20:54:58
 * @description
 */
public class JdbcUtil {
    // 获取Mysql连接
    public static Connection getMysqlConnection() throws Exception{
        // 注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        // 建立连接
        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);

        return conn;
    }
    // 关闭Mysql连接
    public static void closeMysqlConnection(Connection conn) throws Exception{
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
    // 从数据库表中查询数据
    /**
     *
     * @param conn
     * @param sql
     * @param clz
     * @param isUnderlineToCamel
     * @return
     * @param <T>
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception{
        List<T> list = new ArrayList<>();
        // 默认不执行下划线转驼峰
        boolean defaultIsUToC = false;

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            // 通过反射创建一个对象，用于接收查询结果
            T obj = clz.newInstance();
            for (int i = 1; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                // 给对象的属性赋值
                if (defaultIsUToC) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(obj, columnName, columnValue);
            }
            list.add(obj);
        }
        return list;
    }
}
