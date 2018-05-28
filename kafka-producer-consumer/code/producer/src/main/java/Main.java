
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * @Author mubi
 * @Date 2018/5/28 上午8:27
 */
public class Main {
    public static void main(String args[]){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //生产者发送消息
        String topic = "topic-test1";
        Producer<String, String> procuder = new KafkaProducer<String, String>(props);
        for (int i = 1; i <= 10; i++) {
            //String value = "value_" + i;
            Scanner scanner = new Scanner(System.in);
            String value = scanner.next();
            //System.out.println(value);

            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, value);
            procuder.send(msg);
        }
        procuder.flush();

    }
}
