package com.harris.kafka.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * spring.kafka.consumer.enable-auto-commit : false
 * spring.kafka.listener.type : batch
 * spring.kafka.listener.ack-mode : manual
 *
 * @author kittlen
 * @version 1.0
 * @date 2022/07/04 0010
 */
@Component("myBaseErrorHandler")
@Slf4j
public class MyBaseErrorHandler implements KafkaListenerErrorHandler {

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        return null;
    }

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        Throwable cause = exception.getCause();
        if (cause instanceof ProcessingKafkaSpecialException) {
            ProcessingKafkaSpecialException pse = (ProcessingKafkaSpecialException) cause;
            ConsumerRecord<?, ?> errorRecord = pse.getErrorRecord();
            //如果有传入errorRecord,则代表消息消费到该record,设修改偏移量
            //没有传入则偏移量不变,下次消费时还是时重新消费该消息列表
            if (errorRecord != null) {
                log.error("topic为: {} 消费数据:{} 时出现特殊异常,停止监听该topic", errorRecord.topic(), errorRecord.value());
                consumer.commitSync(Collections.singletonMap(new TopicPartition(errorRecord.topic(), errorRecord.partition()), new OffsetAndMetadata(errorRecord.offset())));
                List<TopicPartition> collect = Stream.of(new TopicPartition(errorRecord.topic(), errorRecord.partition())).collect(Collectors.toList());
                consumer.pause(collect);
                throw exception;
            } else {
                Record record = createRecord(message);
                log.error("topic为: {} 消费数据:{}时出现特殊异常,停止监听该topic", record.getTopic(), record.getValue());
                List<TopicPartition> collect = Stream.of(new TopicPartition(record.getTopic(), record.getPartition())).collect(Collectors.toList());
                consumer.pause(collect);
                throw exception;
            }
        }
        return null;
    }

    public Record createRecord(Message<?> message) {
        String topic;
        int partition;
        Object value;
        if (message.getPayload() instanceof List) {
            List<ConsumerRecord> list = (List<ConsumerRecord>) message.getPayload();
            if (list == null || list.isEmpty()) {
                return null;
            }
            ConsumerRecord consumerRecord = list.get(0);
            topic = consumerRecord.topic();
            partition = consumerRecord.partition();
            value = consumerRecord.value();
        } else {
            topic = (String) message.getHeaders().get("kafka_receivedTopic");
            Object partitionId = message.getHeaders().get("kafka_receivedPartitionId");
            if (partitionId instanceof Integer) {
                partition = (int) partitionId;
            } else {
                partition = partitionId == null ? 0 : Integer.parseInt(String.valueOf(partitionId));
            }
            value = message.getPayload();
        }
        return new Record(topic, partition, value);
    }

    class Record {
        private String topic;
        private int partition;
        private Object value;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public Record(String topic, int partition, Object value) {
            this.topic = topic;
            this.partition = partition;
            this.value = value;
        }
    }
}
