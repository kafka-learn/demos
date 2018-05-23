package com.mb;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @Author mubi
 * @Date 2018/5/16 下午2:10
 */
public class Main {

    /**
     * list all topics
     */
    public static void list_topic(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id","test");
//        props.put("auto.offset.reset","earliest");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        AdminClient adminClient = AdminClient.create(props);

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<java.util.Map<java.lang.String,TopicListing>> kafkaFuture
                = listTopicsResult.namesToListings();
        try {
            Map<String,TopicListing> mp = kafkaFuture.get();

            mp.forEach((String topic_name, TopicListing topicListing) -> {
                System.out.println(topicListing.toString());

            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            adminClient.close();
        }

    }

    /**
     * create topic
     */
    public static void create_topic(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id","test");
//        props.put("auto.offset.reset","earliest");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        AdminClient adminClient = AdminClient.create(props);

        String topicName = "topic-test1";
        Integer numPartions = 3;
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(topicName, numPartions, replicationFactor);

        Collection<NewTopic> newTopicCollection = new ArrayList<>();
        newTopicCollection.add(newTopic);

        CreateTopicsResult result = adminClient.createTopics(newTopicCollection);
        Map<String, KafkaFuture<Void>> mp = result.values();
        mp.forEach((String topic_name, KafkaFuture kafkaFuture) -> {
            System.out.println(topic_name);
        });
        adminClient.close();
    }

    /**
     * describe topic
     */
    public static void describe_topic(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id","test");
//        props.put("auto.offset.reset","earliest");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        AdminClient adminClient = AdminClient.create(props);

        String topicName = "topic-test1";

        Collection<String> topicNameCollection = new ArrayList<>();
        topicNameCollection.add(topicName);

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNameCollection);
        Map<String, KafkaFuture<TopicDescription>> mp = describeTopicsResult.values();
        mp.forEach((String name, KafkaFuture<TopicDescription> kafkaFuture)->{
            System.out.println("==="+ name +"====");

            try {
                TopicDescription description = kafkaFuture.get();
                System.out.println("===partion info====");
                List<TopicPartitionInfo> partitionInfoList = description.partitions();
                partitionInfoList.forEach( partitionInfo ->{
                    System.out.println(partitionInfo.toString());
                });

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        });

        adminClient.close();
    }

}
