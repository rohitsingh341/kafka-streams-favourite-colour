package com.rohit.kafkastreamsfavouritecolour;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
@Slf4j
public class KafkaStreamsFavouriteColourApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-group-java");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> textLines = streamsBuilder.stream("favourite-colour-input");

        KStream<String, String> userAndColours = textLines.filter((key, value) -> value.contains(","))
//                .filter((s, s2) -> !s2.split(",")[0].isEmpty())
                .selectKey((s, s2) -> s2.split(",")[0].toLowerCase())
                .mapValues((s, s2) -> s2.split(",")[1].toLowerCase())
                .filter((s, s2) -> Arrays.asList("green", "blue", "red").contains(s2));

        userAndColours.to("user-keys-and-colours");

        KTable<String, String> userColourTable = streamsBuilder.table("user-keys-and-colours");

        KTable<String, Long> favouriteColurTable = userColourTable.groupBy((s, s2) -> new KeyValue<>(s2, s2))
                .count(Named.as("CountsByColour"));

        favouriteColurTable.toStream().to("favourite-colour-output");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        log.info(kafkaStreams.toString());
    }

}
