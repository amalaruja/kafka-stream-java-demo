package amalaruja.kafkastreamsdemo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DemoStreamsApplicationTest {
    @Test
    public void testCaseConversionTopology() {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-application");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final String inputTopicName = "lowercase", outputTopicName = "uppercase";
        final Topology topology = Stream.createCaseConversionTopology(inputTopicName, outputTopicName);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamsConfig);

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(inputTopicName, Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(outputTopicName, Serdes.String().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("key", "hello");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("key", "HELLO")));

        testDriver.close();
    }
}