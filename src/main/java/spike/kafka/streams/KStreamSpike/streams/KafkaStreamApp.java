package spike.kafka.streams.KStreamSpike.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaStreamApp {

    public static void main (String args[]) {
        // input topic streams-input , streams-input-2
        // output topic streams-output

        System.out.println("App Started.");

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStream-spike-app");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "KStream-spike-app-client");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

//        final KStream<String, String> inputStream1 = builder.stream(
//                "streams-input",
//                Consumed.with(Serdes.String(),
//                        Serdes.String()
//                )
//        );
//
//        final KStream<String, String> inputStream2 = builder.stream(
//                "streams-input-2",
//                Consumed.with(Serdes.String(),
//                        Serdes.String()
//                )
//        );

        KTable<String, String> inputStream1 = builder.table(
                "streams-input",
                Consumed.with(Serdes.String(),
                        Serdes.String()
                )
        );

        KTable<String, String> inputStream2 = builder.table(
                "streams-input-2",
                Consumed.with(Serdes.String(),
                        Serdes.String())
        );

//        KStream<String, String> outputStream = inputStream1.leftJoin(inputStream2,
//                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
//                Joined.with(
//                        Serdes.String(), /* key */
//                        Serdes.String(),   /* left value */
//                        Serdes.String())  /* right value */
//        );

        KTable<String, String> outputStream = inputStream1.leftJoin(inputStream2,
                (left, right) -> "left:" + left + ", right:" + right
        );

        // write the enriched order to the enriched-order topic
        outputStream.toStream().to("streams-output", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
    }

}
