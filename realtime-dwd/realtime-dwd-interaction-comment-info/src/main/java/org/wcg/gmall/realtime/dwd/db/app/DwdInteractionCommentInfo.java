package org.wcg.gmall.realtime.dwd.db.app;

import com.sun.org.apache.bcel.internal.Const;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.wcg.gmall.realtime.common.base.BaseSqlApp;
import org.wcg.gmall.realtime.common.constant.Constant;
import org.wcg.gmall.realtime.common.util.SqlUtil;

/**
 * @author wangchengguang
 * @date 2025/3/16 15:14:32
 * @description
 * 评论事实表
 */
public class DwdInteractionCommentInfo extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // TODO 3.从kafka的topic_db主题中读取数据 创建动态表      ---kafka连接器
        readOdsDb(tableEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        // TODO 4.过滤出评论数据                                ---where条件过滤 table='comment_info' type='insert'
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "    `ts`,\n" +
                "    `proc_time`\n" +
                "from topic_db\n" +
                "where `table` = 'comment_info'\n" +
                "and `type` = 'insert'");
//        commentInfo.execute().print();
        // 将表对象注册到表执行环境
        tableEnv.createTemporaryView("comment_info", commentInfo);
        // TODO 5.从HBase中读取字典数据 创建动态表                ---HBase连接器
        readBaseDic(tableEnv);
        // TODO 6.将评论表和字典表进行关联                       ---lookup join
        Table joinedTable = tableEnv.sqlQuery("SELECT \n" +
                "  c.`id`,\n" +
                "  c.`user_id`,\n" +
                "  c.`sku_id`,\n" +
                "  c.`appraise`,\n" +
                "  b.dic_name as appraise_name,\n" +
                "  c.comment_txt,\n" +
                "  c.`ts`\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS b\n" +
                "    ON c.appraise = b.dic_code;");
//        joinedTable.execute().print();
        // TODO 7.将关联的结果写到kafka主题中                    ---upsert kafka连接器
        // 7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "  `id` string,\n" +
                "  `user_id` string,\n" +
                "  `sku_id` string,\n" +
                "  `appraise` string,\n" +
                "  `appraise_name` string,\n" +
                "  `comment_txt` string,\n" +
                "  `ts` bigint,\n" +
                "  \n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SqlUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 7.2 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }


}
