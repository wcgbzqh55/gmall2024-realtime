package org.wcg.gmall.realtime.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.wcg.gmall.realtime.common.constant.Constant;

/**
 * @author wangchengguang
 * @date 2025/3/14 20:03:09
 * @description
 *
 * 获取相关Sink工具类
 */
public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
//                // 当前配置决定是否开启事务，保证写到kafka数据的精准一次性
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                // 指定事务ID的前缀，系统自动生成的可能会重复
//                .setTransactionalIdPrefix("dwd_base_log_")
//                // 设置事务超时时间，大于Checkpoint超时时间，小于等于事务最大的超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }
}
