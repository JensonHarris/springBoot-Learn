package com.harris.kafka.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author admin
 */
@RestController
public class KafkaProducerController {


    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/kafka/normal/{message}")
    public void sendMessage(@PathVariable("message") String normalMessage) {
        kafkaTemplate.send("topic1", normalMessage);
    }

    /**
     * 带回调的
     * @param callbackMessage 消息
     */
    @GetMapping("/kafka/callbackOne/{message}")
    public void sendMessage1(@PathVariable("message") String callbackMessage) {
        kafkaTemplate.send("topic2", callbackMessage).addCallback(success -> {
            // 消息发送到的topic
            assert success != null;
            String topic = success.getRecordMetadata().topic();
            // 消息发送到的分区
            int partition = success.getRecordMetadata().partition();
            // 消息在分区内的offset
            long offset = success.getRecordMetadata().offset();
            System.out.println("发送消息成功: topic-" + topic + "-partition" + partition + "-offset" + offset);
        }, failure -> {
            System.out.println("发送消息失败:" + failure.getMessage());
        });
    }

    @GetMapping("/kafka/callbackTwo/{message}")
    public void sendMessage2(@PathVariable("message") String callbackMessage) {
        kafkaTemplate.send("topic3", callbackMessage).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable e) {
                System.out.println("发送消息失败："+e.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("发送消息成功：topic " + result.getRecordMetadata().topic() + "- partition"
                        + result.getRecordMetadata().partition() + "- offset" + result.getRecordMetadata().offset());
            }
        });
    }

    @GetMapping("/kafka1/transaction")
    public void sendMessage3(){
        // 声明事务：后面报错消息不会发出去
//        kafkaTemplate.executeInTransaction(operations -> {
//            operations.send("topic1","test executeInTransaction");
//            throw new RuntimeException("fail");
//        });
        // 不声明事务：后面报错但前面消息已经发送成功了
        kafkaTemplate.send("topic1","test executeInTransaction");
        throw new RuntimeException("fail");
    }

}
