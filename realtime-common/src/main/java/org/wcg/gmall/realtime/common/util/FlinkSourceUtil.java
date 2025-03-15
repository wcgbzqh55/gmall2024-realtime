package org.wcg.gmall.realtime.common.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.wcg.gmall.realtime.common.constant.Constant;

import java.io.IOException;
import java.util.Properties;

/**
 * @author wangchengguang
 */
public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
//                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                // 生产环境中一般为了保证消费的精准一致性，需要手动维护偏移量，KafkaSource --> KafkaSourceReader --> 存储偏移量
                // .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // ack=1 幂等性 事务
                // 从最末尾点开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                // 使用SimpleStringSchema对String类型的消息进行反序列化，如果消息为空，会报错
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return kafkaSource;
    }

    public static MySqlSource<String> getMySqlSource(String database, String tableName) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database)
                .tableList(database + "." + tableName)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
