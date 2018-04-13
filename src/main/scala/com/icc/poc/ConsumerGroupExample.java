package com.icc.poc;
//package org.com.icc.datalake.io;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//
//import kafka.consumer.Consumer;
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
// 
//public class ConsumerGroupExample {
//    private final ConsumerConnector consumerConnector;
//    private static String kafkaTopic;
//    private final ConsumerIterator<?, ?> consumerIterator;
//    private static String groupId;
// 
//    public ConsumerGroupExample(String zookeeperUrl, String topic, String groupId, String kafkaTopic) {
//        ConsumerGroupExample.kafkaTopic = kafkaTopic;
//            ConsumerGroupExample.groupId = groupId;
//            this.consumerConnector = kafkaConnector(zookeeperUrl);
//            this.consumerIterator = connectToKafka(consumerConnector);
//    }
// 
// 
//    private static ConsumerIterator<byte[], byte[]> connectToKafka(ConsumerConnector consumer) {
//        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        final int CONSUMER_THREAD_COUNT = 1;
//        topicCountMap.put(kafkaTopic, CONSUMER_THREAD_COUNT);
//        KafkaStream<byte[], byte[]> stream = consumer.createMessageStreams(topicCountMap).get(kafkaTopic).get(0);
//        return stream.iterator();
//    }
//    private static ConsumerConnector kafkaConnector (String zooKeeperUrl) {
//        return Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaProperties(zooKeeperUrl)));
//    }
//    private static Properties kafkaProperties (String zooKeeperUrl) {
//        Properties props = new Properties();
//        props.put("zookeeper.connect", zooKeeperUrl);
//        props.put("group.id", groupId);
////        props.put("fetch.message.max.bytes", FETCH_MESSAGE_MAX_BYTES);
//        props.put("zookeeper.session.timeout.ms", "400");
//        props.put("zookeeper.sync.time.ms", "200");
//        return props;
//    }
// 
//    public static void main(String[] args) {
//        String zooKeeper = "node1:2181/kafka";
//        String groupId = "test";
//        String kafkaTopic = "edhTopic";
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException ie) {
// 
//        }
//    }
//}