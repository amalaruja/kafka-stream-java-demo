# Kafka Streams Demo Application

This is sample kafka streams application that coverts lowercase words to uppercase

# Running the demo application

The application can be tested by creating lower cased messages through a command line producer and then verifying the results using a command line consumer.
After starting up zookeper and kafka on your machine, use the following commands to test the application.

### 1. Create input & ouptput topics

`./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic lowercase`<br/>
`./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic uppercase`

### 2. Start a command line producer

`./kafka-console-producer.sh --broker-list localhost:9092 --topic uppercase`

### 3. Start a command line consumer

`./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic uppercase --group topic_group`

### 4. Run the demo application

`./gradlew run`
