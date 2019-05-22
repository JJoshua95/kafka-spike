package spike.kafka.streams.KStreamSpike.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerAppForStreams2 {

    public static void main(String args[]) {

        // Create a properties dictionary for the required / optional Producer config settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // --> props.put("config.settings", "value");
        // :: http://kafka.apache.org/documentation.html#producerconfigs

        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);

        try {
            System.out.println("here");
            for (int i = 0; i < 10; i++) {
                if (i % 2 != 0) {
                    // send messages with odd keys to first streams-input topic
                    myProducer.send(new ProducerRecord<String, String>("streams-input",
                            Integer.toString(i), "MyMessage: " + Integer.toString(i)));
                } else {
                    // send messages with even keys to second streams-input topic
                    myProducer.send(new ProducerRecord<String, String>("streams-input-2",
                            Integer.toString(i), "MyMessage: " + Integer.toString(i)));
                }
            }
            System.out.println("there");
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("done");
            myProducer.close();
        }

    }


}
