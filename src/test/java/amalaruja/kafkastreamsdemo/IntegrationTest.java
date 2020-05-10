package amalaruja.kafkastreamsdemo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.*;
import org.junit.Test;

public class IntegrationTest {

    final String sourceTopic = "lowercase";
    final String sinkTopic = "uppercase";
    String bootstrapServers;

    @Test
    public void testEndToEndFlow() throws IOException, InterruptedException {
        final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        cluster.createTopics(sourceTopic, sinkTopic);
        bootstrapServers = cluster.bootstrapServers();

        System.out.printf("Embedded Cluster Bootstrap servers : %s\n", cluster.bootstrapServers());

        final Producer<String, String> producer = testProducer();
        producer.send(new ProducerRecord<String, String>(sourceTopic, "key", "hello"));
        System.out.println("Message sent successfully");

        Stream stream = testStream();
        stream.start();

        final Consumer<String, String> consumer = testConsumer();
        consumer.subscribe(Arrays.asList(sinkTopic));

        System.out.println("Subscribed to topic " + sinkTopic);
        boolean received = false;
        String receivedMessage = "";

        while (true && !received) {
            final ConsumerRecords<String, String> records = consumer.poll(100);
            for (final ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                receivedMessage = record.value();
                received = true;
            }
        }
        
        stream.stop();
        consumer.close();
        cluster.deleteAllTopicsAndWait(100);

        assertEquals("HELLO", receivedMessage);
    }

    private Stream testStream() {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-application");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return new Stream(sourceTopic, sinkTopic, streamsConfig);
    }

    private Producer<String, String> testProducer() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }

    private Consumer<String, String> testConsumer() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }
}