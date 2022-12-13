package com.harris.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author admin
 */
public class ProcessingKafkaSpecialException extends RuntimeException {
    /**
     * 已经消费到哪一个record,修改偏移量时使用
     */
    private ConsumerRecord<?, ?> errorRecord;

    public ProcessingKafkaSpecialException() {
    }

    public ProcessingKafkaSpecialException(Throwable cause) {
        super(cause);
    }

    public ProcessingKafkaSpecialException(String message) {
        super(message);
    }

    public ProcessingKafkaSpecialException(ConsumerRecord<?, ?> errorRecord) {
        this.errorRecord = errorRecord;
    }

    public ProcessingKafkaSpecialException(Throwable cause, ConsumerRecord<?, ?> errorRecord) {
        super(cause);
        this.errorRecord = errorRecord;
    }

    public ProcessingKafkaSpecialException(String message, ConsumerRecord<?, ?> errorRecord) {
        super(message);
        this.errorRecord = errorRecord;
    }

    public ConsumerRecord<?, ?> getErrorRecord() {
        return errorRecord;
    }

    public void setErrorRecord(ConsumerRecord<?, ?> errorRecord) {
        this.errorRecord = errorRecord;
    }
}
