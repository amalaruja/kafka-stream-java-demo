package amalaruja.kafkastreamsdemo;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Stream {

    private KafkaStreams streams;

    public Stream(String sourceTopic, String sinkTopic, Properties streamsConfig) {
        final Topology topology = createCaseConversionTopology(sourceTopic, sinkTopic);
        this.streams = new KafkaStreams(topology, streamsConfig);
    }

    public void start() {
        streams.start();
    }

    public void stop() {
        streams.close();
    }

    public static Topology createCaseConversionTopology(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(inputTopic);

        source.mapValues(str -> str.toUpperCase())
                .to(outputTopic);

        return builder.build();
    }
}