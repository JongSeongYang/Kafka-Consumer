package com.jongseong.kafkaConsumer.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

@Component
@Slf4j
public class ConsumerController {
    private final Logger logger = LoggerFactory.getLogger(ConsumerController.class);

    @KafkaListener(topics = "${spring.kafka.topic.test}", groupId = "test-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord consumerRecord) throws Exception {
        try {
            // 파일이 아닌 HDFS(Hadoop Distributed File System), ElasticSearch, Redis 등 다양한 스토리지에 적재 가능
            File file = new File("C:\\kafka\\test.txt");
            FileWriter writer = null;

            try {
                // 기존 파일의 내용에 이어서 쓰려면 true, 기존 내용을 없애고 새로 쓰려면 false
                writer = new FileWriter(file, true);
                writer.write(consumerRecord.value().toString());
                writer.write("\n");
                writer.flush();

                System.out.println("DONE");
            } catch(IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if(writer != null) writer.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            logger.warn("exception:{},{}", e.getMessage(), e.toString());
        }
    }
}
