package spike.kafka.streams.KStreamSpike.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerApp {

    public static void main(String args[]) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","test-consumer-group");

        //props.put("config.setting","value");

        KafkaConsumer myConsumer = new KafkaConsumer(props);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("my-replicated-topic");
        //topics.add("my-topic");
        topics.add("test");

        myConsumer.subscribe(topics);

        try {
            System.out.println("============BEGIN POLLING KAFKA============");
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                    System.out.println(record.value());
                }

            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
            System.out.println("============END============");
        }

    }

}
